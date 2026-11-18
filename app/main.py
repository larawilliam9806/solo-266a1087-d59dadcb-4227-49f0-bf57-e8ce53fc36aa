from __future__ import annotations

import csv
import io
import logging
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response

from .config import Config
from .core import ErasureService, NoDataError, RestrictedError
from .schemas import ErasureRequest
from .storage import SCHEMA

log = logging.getLogger("erasure.api")

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def parse_timestamp(raw: str) -> datetime:
    """Parse an ISO-8601 timestamp (or epoch seconds) and normalize to UTC."""
    s = raw.strip()
    if not s:
        raise ValueError("empty timestamp")
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.fromtimestamp(float(s), tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_csv(raw: bytes) -> tuple[pa.Table, int, int]:
    """Parse an uploaded CSV into the canonical schema.

    Returns ``(table, received, invalid)`` where ``received`` counts all data
    rows and ``invalid`` the rows that had to be skipped.
    """
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise HTTPException(400, f"CSV must be UTF-8 encoded: {exc}")
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any(cell.strip() for cell in r)]
    if not rows:
        raise HTTPException(400, "empty CSV")
    header = [h.strip().lower() for h in rows[0]]
    try:
        iu = header.index("user_id")
        ip = header.index("page")
        it = header.index("timestamp")
    except ValueError:
        raise HTTPException(
            400, "CSV header must contain columns: user_id, page, timestamp"
        )
    users, pages, stamps = [], [], []
    invalid = 0
    for r in rows[1:]:
        try:
            user = r[iu].strip()
            if not user:
                raise ValueError("empty user_id")
            page = r[ip]
            ts = parse_timestamp(r[it])
        except (ValueError, IndexError):
            invalid += 1
            continue
        users.append(user)
        pages.append(page)
        stamps.append(ts)
    table = pa.table(
        {"user_id": users, "page": pages, "ts": stamps},
        schema=SCHEMA,
    )
    return table, len(users) + invalid, invalid


def validate_date(date: str) -> str:
    if not DATE_RE.match(date):
        raise HTTPException(422, f"invalid date {date!r}; expected YYYY-MM-DD")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(422, f"invalid date {date!r}")
    return date


def table_to_csv_bytes(table: pa.Table) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["user_id", "page", "timestamp"])
    for row in table.to_pylist():
        writer.writerow(
            [row["user_id"], row["page"], row["ts"].astimezone(timezone.utc).isoformat()]
        )
    return buf.getvalue().encode("utf-8")


def create_app(config: Config | None = None) -> FastAPI:
    config = config or Config.from_env()
    service = ErasureService(config)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        service.start()
        yield
        service.shutdown()

    app = FastAPI(title="Click Lake Erasure API", version="1.0.0", lifespan=lifespan)
    app.state.service = service

    # ------------------------------------------------------------- ingest

    @app.post("/upload/{date}", summary="Append a CSV of clicks for one day")
    def upload(date: str, file: UploadFile = File(...)):
        date = validate_date(date)
        table, received, invalid = parse_csv(file.file.read())
        result = service.upload(date, table)
        return {"date": date, "received": received, "invalid": invalid, **result}

    # ------------------------------------------------------------ download

    @app.get("/dates", summary="List all dates present in the lake")
    def list_dates():
        rows = service.store.query(
            "SELECT date, COUNT(*) AS files, SUM(rows) AS rows FROM files"
            " WHERE status='active' GROUP BY date ORDER BY date"
        )
        restricted = service.store.restricted_dates()
        return [
            {
                "date": r["date"],
                "files": r["files"],
                "rows": r["rows"],
                "restricted": r["date"] in restricted,
            }
            for r in rows
        ]

    @app.get("/dates/{date}/download", summary="Download one day as CSV or Parquet")
    def download(date: str, format: str = Query("csv", pattern="^(csv|parquet)$")):
        date = validate_date(date)
        try:
            table = service.download(date)
        except RestrictedError as exc:
            raise HTTPException(409, str(exc))
        except NoDataError as exc:
            raise HTTPException(404, str(exc))
        if format == "parquet":
            buf = io.BytesIO()
            pq.write_table(table, buf)
            return Response(
                buf.getvalue(),
                media_type="application/octet-stream",
                headers={
                    "Content-Disposition": f'attachment; filename="clicks-{date}.parquet"'
                },
            )
        return Response(
            table_to_csv_bytes(table),
            media_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="clicks-{date}.csv"'},
        )

    # ------------------------------------------------------------- erasure

    @app.post("/erasure", summary="Request erasure of a user (idempotent)")
    def submit_erasure(req: ErasureRequest):
        status, created = service.submit_erasure(req.user_id)
        return JSONResponse(status, status_code=201 if created else 200)

    @app.get("/erasure", summary="List erasure tasks")
    def list_erasures(user_id: str | None = None):
        out = []
        for task in service.store.all_tasks():
            if user_id is not None and task["user_id"] != user_id:
                continue
            out.append(service.task_status(task["task_id"]))
        return out

    @app.get("/erasure/{task_id}", summary="Erasure task progress")
    def erasure_status(task_id: str):
        status = service.task_status(task_id)
        if status is None:
            raise HTTPException(404, f"unknown task {task_id}")
        return status

    # --------------------------------------------------------------- misc

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app
