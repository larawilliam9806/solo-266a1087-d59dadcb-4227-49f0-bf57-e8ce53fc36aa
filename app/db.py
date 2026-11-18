from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    file_id    TEXT PRIMARY KEY,
    date       TEXT NOT NULL,
    path       TEXT NOT NULL,
    rows       INTEGER NOT NULL,
    status     TEXT NOT NULL CHECK (status IN ('active', 'obsolete', 'deleted')),
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_files_date ON files(date, status);

-- every (user, date) pair ever ingested; used to know which dates a user touches
CREATE TABLE IF NOT EXISTS user_dates (
    user_id TEXT NOT NULL,
    date    TEXT NOT NULL,
    PRIMARY KEY (user_id, date)
);

CREATE TABLE IF NOT EXISTS erasure_tasks (
    task_id    TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL UNIQUE,          -- one task per user, forever
    status     TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'failed', 'completed')),
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- files a task still has to inspect/rewrite
CREATE TABLE IF NOT EXISTS erasure_files (
    task_id TEXT NOT NULL,
    file_id TEXT NOT NULL,
    status  TEXT NOT NULL CHECK (status IN ('pending', 'done')),
    PRIMARY KEY (task_id, file_id)
);

-- a date is restricted as long as at least one row references it
CREATE TABLE IF NOT EXISTS restricted_dates (
    date    TEXT NOT NULL,
    task_id TEXT NOT NULL,
    PRIMARY KEY (date, task_id)
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class StateStore:
    """SQLite-backed state.

    A single connection guarded by an RLock; every multi-step write happens
    inside ``transaction()`` (BEGIN IMMEDIATE) so concurrent API requests and
    the background worker never observe a half-applied change.
    """

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=15000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()) -> Optional[sqlite3.Row]:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    # ---------------------------------------------------------------- files

    def record_upload(self, file_id: str, date: str, path: str, rows: int,
                      user_ids: Iterable[str]) -> None:
        now = utcnow()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO files(file_id, date, path, rows, status, created_at)"
                " VALUES (?,?,?,?,'active',?)",
                (file_id, date, path, rows, now),
            )
            conn.executemany(
                "INSERT OR IGNORE INTO user_dates(user_id, date) VALUES (?,?)",
                [(u, date) for u in user_ids],
            )

    def get_file(self, file_id: str) -> Optional[sqlite3.Row]:
        return self.query_one("SELECT * FROM files WHERE file_id=?", (file_id,))

    def active_files_for_date(self, date: str) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM files WHERE date=? AND status='active' ORDER BY created_at",
            (date,),
        )

    def active_files_in_dates(self, dates: list[str]) -> list[sqlite3.Row]:
        if not dates:
            return []
        marks = ",".join("?" for _ in dates)
        return self.query(
            f"SELECT * FROM files WHERE status='active' AND date IN ({marks})",
            tuple(dates),
        )

    def files_with_status(self, *statuses: str) -> list[sqlite3.Row]:
        marks = ",".join("?" for _ in statuses)
        return self.query(f"SELECT * FROM files WHERE status IN ({marks})", statuses)

    def all_known_paths(self) -> set[str]:
        return {r["path"] for r in self.query("SELECT path FROM files")}

    def mark_file_deleted(self, file_id: str) -> None:
        with self.transaction() as conn:
            conn.execute("UPDATE files SET status='deleted' WHERE file_id=?", (file_id,))

    # -------------------------------------------------------- erasure tasks

    def accept_task(self, task_id: str, user_id: str) -> None:
        """Create the task, snapshot every active file that may hold the user's
        data and restrict the affected dates — atomically. Runs under the
        service commit lock, so no upload can slip in between."""
        now = utcnow()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO erasure_tasks(task_id, user_id, status, attempts, created_at, updated_at)"
                " VALUES (?,?,'pending',0,?,?)",
                (task_id, user_id, now, now),
            )
            conn.execute(
                "INSERT OR IGNORE INTO erasure_files(task_id, file_id, status)"
                " SELECT ?, file_id, 'pending' FROM files"
                " WHERE status='active' AND date IN"
                "       (SELECT date FROM user_dates WHERE user_id=?)",
                (task_id, user_id),
            )
            conn.execute(
                "INSERT OR IGNORE INTO restricted_dates(date, task_id)"
                " SELECT date, ? FROM user_dates WHERE user_id=?",
                (task_id, user_id),
            )

    def get_task(self, task_id: str) -> Optional[sqlite3.Row]:
        return self.query_one("SELECT * FROM erasure_tasks WHERE task_id=?", (task_id,))

    def get_task_by_user(self, user_id: str) -> Optional[sqlite3.Row]:
        return self.query_one("SELECT * FROM erasure_tasks WHERE user_id=?", (user_id,))

    def all_tasks(self) -> list[sqlite3.Row]:
        return self.query("SELECT * FROM erasure_tasks ORDER BY created_at")

    def incomplete_tasks(self) -> list[sqlite3.Row]:
        return self.query(
            "SELECT * FROM erasure_tasks WHERE status != 'completed' ORDER BY created_at"
        )

    def mark_processing(self, task_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE erasure_tasks SET status='processing', attempts=attempts+1,"
                " updated_at=? WHERE task_id=?",
                (utcnow(), task_id),
            )

    def mark_failed(self, task_id: str, error: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE erasure_tasks SET status='failed', last_error=?, updated_at=?"
                " WHERE task_id=?",
                (error, utcnow(), task_id),
            )

    # -------------------------------------------------------- erasure files

    def next_pending_file(self, task_id: str) -> Optional[sqlite3.Row]:
        return self.query_one(
            "SELECT file_id FROM erasure_files WHERE task_id=? AND status='pending'"
            " ORDER BY rowid LIMIT 1",
            (task_id,),
        )

    def mark_file_done(self, task_id: str, file_id: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "UPDATE erasure_files SET status='done' WHERE task_id=? AND file_id=?",
                (task_id, file_id),
            )

    def add_pending_files(self, task_id: str, file_ids: Iterable[str]) -> None:
        with self.transaction() as conn:
            conn.executemany(
                "INSERT OR IGNORE INTO erasure_files(task_id, file_id, status)"
                " VALUES (?,?,'pending')",
                [(task_id, f) for f in file_ids],
            )

    def file_counts(self, task_id: str) -> tuple[int, int]:
        row = self.query_one(
            "SELECT COUNT(*) AS total, COALESCE(SUM(status='done'),0) AS done"
            " FROM erasure_files WHERE task_id=?",
            (task_id,),
        )
        return row["total"], row["done"]

    def record_rewrite(self, task_id: str, old_file_id: str, new_file_id: str,
                       date: str, path: str, rows: int) -> None:
        """Atomically register the replacement file, retire the old one and
        re-point every *other* task's pending rows at the new file, so
        overlapping erasures never lose track of data they still must remove."""
        now = utcnow()
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO files(file_id, date, path, rows, status, created_at)"
                " VALUES (?,?,?,?,'active',?)",
                (new_file_id, date, path, rows, now),
            )
            conn.execute("UPDATE files SET status='obsolete' WHERE file_id=?", (old_file_id,))
            conn.execute(
                "UPDATE erasure_files SET status='done' WHERE task_id=? AND file_id=?",
                (task_id, old_file_id),
            )
            conn.execute(
                "UPDATE OR IGNORE erasure_files SET file_id=? WHERE file_id=? AND status='pending'",
                (new_file_id, old_file_id),
            )
            # rows that could not be re-pointed because the task already tracks the new file
            conn.execute(
                "DELETE FROM erasure_files WHERE file_id=? AND status='pending'",
                (old_file_id,),
            )

    def record_delete_only(self, task_id: str, old_file_id: str) -> None:
        """The filtered file would be empty: retire it and close every pending
        row pointing at it (its only remaining rows belonged to erased users)."""
        with self.transaction() as conn:
            conn.execute("UPDATE files SET status='obsolete' WHERE file_id=?", (old_file_id,))
            conn.execute(
                "UPDATE erasure_files SET status='done' WHERE file_id=?", (old_file_id,)
            )

    # ----------------------------------------------- user dates / restrictions

    def dates_for_user(self, user_id: str) -> list[str]:
        return [
            r["date"]
            for r in self.query("SELECT date FROM user_dates WHERE user_id=?", (user_id,))
        ]

    def is_restricted(self, date: str) -> bool:
        return (
            self.query_one(
                "SELECT 1 AS x FROM restricted_dates WHERE date=? LIMIT 1", (date,)
            )
            is not None
        )

    def restricted_dates(self) -> set[str]:
        return {r["date"] for r in self.query("SELECT DISTINCT date FROM restricted_dates")}

    def restricted_dates_for_task(self, task_id: str) -> list[str]:
        return [
            r["date"]
            for r in self.query(
                "SELECT date FROM restricted_dates WHERE task_id=? ORDER BY date", (task_id,)
            )
        ]

    def finalize_task(self, task_id: str, user_id: str) -> None:
        """Mark the task completed and lift this task's date restrictions.
        The user stays blocked forever because the task row itself remains."""
        now = utcnow()
        with self.transaction() as conn:
            conn.execute("DELETE FROM user_dates WHERE user_id=?", (user_id,))
            conn.execute("DELETE FROM restricted_dates WHERE task_id=?", (task_id,))
            conn.execute(
                "UPDATE erasure_tasks SET status='completed', last_error=NULL, updated_at=?"
                " WHERE task_id=?",
                (now, task_id),
            )
