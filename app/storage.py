from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

# Canonical record schema for every part file in the lake.
SCHEMA = pa.schema(
    [
        pa.field("user_id", pa.string()),
        pa.field("page", pa.string()),
        pa.field("ts", pa.timestamp("us", tz="UTC")),
    ]
)


def new_file_id() -> str:
    return uuid.uuid4().hex


def day_dir(data_dir: Path, date: str) -> Path:
    return Path(data_dir) / f"date={date}"


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    except (OSError, AttributeError):  # pragma: no cover - platform dependent
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_table_atomic(
    data_dir: Path, date: str, table: pa.Table, file_id: Optional[str] = None
) -> tuple[str, str]:
    """Write a part file durably: tmp file + fsync + atomic rename + dir fsync.

    Returns ``(file_id, relative_path)``.
    """
    file_id = file_id or new_file_id()
    directory = day_dir(data_dir, date)
    directory.mkdir(parents=True, exist_ok=True)
    tmp_path = directory / f".part-{file_id}.parquet.tmp"
    final_path = directory / f"part-{file_id}.parquet"
    with open(tmp_path, "wb") as fh:
        pq.write_table(table, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp_path, final_path)
    _fsync_dir(directory)
    return file_id, f"date={date}/part-{file_id}.parquet"


def read_table(data_dir: Path, rel_path: str, columns: Optional[list[str]] = None) -> pa.Table:
    return pq.read_table(Path(data_dir) / rel_path, columns=columns)


def delete_file(data_dir: Path, rel_path: str) -> None:
    try:
        (Path(data_dir) / rel_path).unlink()
    except FileNotFoundError:
        pass


def cleanup_tmp_files(data_dir: Path) -> list[Path]:
    removed = []
    root = Path(data_dir)
    if not root.exists():
        return removed
    for path in root.rglob("*.tmp"):
        try:
            path.unlink()
            removed.append(path)
        except OSError:  # pragma: no cover - defensive
            pass
    return removed


def list_part_files(data_dir: Path) -> list[str]:
    root = Path(data_dir)
    if not root.exists():
        return []
    return [str(p.relative_to(root)) for p in sorted(root.rglob("part-*.parquet"))]
