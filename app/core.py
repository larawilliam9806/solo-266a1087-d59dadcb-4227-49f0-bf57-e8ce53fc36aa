from __future__ import annotations

import logging
import queue
import threading
from types import SimpleNamespace
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc

from . import storage
from .config import Config
from .db import StateStore
from .storage import new_file_id

log = logging.getLogger("erasure")


class RestrictedError(Exception):
    """The requested date is restricted while an erasure covering it runs."""

    def __init__(self, date: str):
        self.date = date
        super().__init__(
            f"date {date} is temporarily restricted while an erasure is in progress"
        )


class NoDataError(Exception):
    def __init__(self, date: str):
        self.date = date
        super().__init__(f"no data for date {date}")


class ErasureService:
    """Core of the click lake: ingest, download and GDPR-style erasure.

    Concurrency model
    -----------------
    * ``commit_lock`` serializes upload commits against erasure acceptance.
      An upload filters blocked users, writes its part file and commits the
      DB record while holding the lock; accepting an erasure takes the same
      lock. Therefore a file that contains an erased user can never be
      committed after the erasure was accepted, and every file committed
      before is part of the erasure snapshot.
    * A single background worker rewrites files sequentially, which makes
      overlapping erasures of the same files safe.
    * Per-date locks let an in-progress download finish with a consistent
      view while a rewrite of that date is waiting.
    """

    def __init__(self, config: Config):
        self.config = config
        self.store = StateStore(config.db_path)
        self.commit_lock = threading.RLock()
        self.blocked_users: set[str] = set()
        self._date_locks: dict[str, threading.RLock] = {}
        self._date_locks_guard = threading.Lock()
        self.worker = ErasureWorker(self)
        # test hook invoked inside the commit lock right before an upload is filtered
        self.hooks = SimpleNamespace(before_write=None)

    # ------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.config.state_dir.mkdir(parents=True, exist_ok=True)
        self._reconcile_storage()
        # every task ever created blocks its user, completed ones forever
        self.blocked_users = {row["user_id"] for row in self.store.all_tasks()}
        # resume unfinished erasures (pending / processing / failed)
        for task in self.store.incomplete_tasks():
            log.info("resuming erasure task %s (%s)", task["task_id"], task["status"])
            self.worker.submit(task["task_id"])
        self.worker.start()

    def shutdown(self) -> None:
        self.worker.stop()
        self.store.close()

    def _reconcile_storage(self) -> None:
        """Crash recovery: drop tmp files, finish pending deletions and remove
        orphaned part files that were never committed to the state DB."""
        storage.cleanup_tmp_files(self.config.data_dir)
        for row in self.store.files_with_status("obsolete", "deleted"):
            storage.delete_file(self.config.data_dir, row["path"])
            self.store.mark_file_deleted(row["file_id"])
        known = self.store.all_known_paths()
        for rel in storage.list_part_files(self.config.data_dir):
            if rel not in known:
                log.warning("removing orphaned parquet file %s", rel)
                storage.delete_file(self.config.data_dir, rel)

    def date_lock(self, date: str) -> threading.RLock:
        with self._date_locks_guard:
            return self._date_locks.setdefault(date, threading.RLock())

    # -------------------------------------------------------------- uploads

    def upload(self, date: str, table: pa.Table) -> dict:
        """Append the rows of one CSV as a new part file for ``date``.

        Rows belonging to blocked users are dropped at commit time, so even an
        upload that started before an erasure request was accepted can never
        write that user's data back.
        """
        with self.commit_lock:
            if self.hooks.before_write is not None:
                self.hooks.before_write()
            received = table.num_rows
            blocked_set = self.blocked_users
            if blocked_set and received:
                mask = pc.invert(
                    pc.is_in(
                        table.column("user_id"),
                        value_set=pa.array(sorted(blocked_set)),
                    )
                )
                filtered = table.filter(mask)
            else:
                filtered = table
            blocked = received - filtered.num_rows
            file_id = None
            if filtered.num_rows:
                file_id, rel_path = storage.write_table_atomic(
                    self.config.data_dir, date, filtered
                )
                users = set(filtered.column("user_id").to_pylist())
                self.store.record_upload(file_id, date, rel_path, filtered.num_rows, users)
            return {
                "blocked": blocked,
                "stored": filtered.num_rows,
                "file_id": file_id,
            }

    # ------------------------------------------------------------ downloads

    def download(self, date: str) -> pa.Table:
        """Return the day's full contents.

        Restriction is checked at request time, so previously shared links are
        blocked as well. The data is read under the date lock before the
        response starts streaming, which lets an already-started download
        finish even if a rewrite begins right after.
        """
        if self.store.is_restricted(date):
            raise RestrictedError(date)
        lock = self.date_lock(date)
        with lock:
            files = self.store.active_files_for_date(date)
            if not files:
                raise NoDataError(date)
            tables = [storage.read_table(self.config.data_dir, f["path"]) for f in files]
        return pa.concat_tables(tables) if len(tables) > 1 else tables[0]

    # -------------------------------------------------------------- erasure

    def submit_erasure(self, user_id: str) -> tuple[dict, bool]:
        """Accept an erasure request. Idempotent: a repeated submission for the
        same user returns the very same task."""
        with self.commit_lock:
            existing = self.store.get_task_by_user(user_id)
            if existing is not None:
                return self.task_status(existing["task_id"]), False
            task_id = new_file_id()
            self.store.accept_task(task_id, user_id)
            # blocking takes effect immediately, before any background work
            self.blocked_users.add(user_id)
        self.worker.submit(task_id)
        return self.task_status(task_id), True

    def task_status(self, task_id: str) -> Optional[dict]:
        task = self.store.get_task(task_id)
        if task is None:
            return None
        total, done = self.store.file_counts(task_id)
        return {
            "task_id": task["task_id"],
            "user_id": task["user_id"],
            "status": task["status"],
            "files_total": total,
            "files_done": done,
            "restricted_dates": self.store.restricted_dates_for_task(task_id),
            "attempts": task["attempts"],
            "last_error": task["last_error"],
            "created_at": task["created_at"],
            "updated_at": task["updated_at"],
        }

    # ---------------------------------------------------- worker operations

    def erase_file(self, task, file_id: str) -> None:
        """Rewrite one file without the target user's rows and delete the old
        file. Other users' records are preserved."""
        user_id = task["user_id"]
        info = self.store.get_file(file_id)
        if info is None or info["status"] != "active":
            self.store.mark_file_done(task["task_id"], file_id)
            return
        date = info["date"]
        with self.date_lock(date):
            info = self.store.get_file(file_id)
            if info is None or info["status"] != "active":
                self.store.mark_file_done(task["task_id"], file_id)
                return
            ids = storage.read_table(
                self.config.data_dir, info["path"], columns=["user_id"]
            ).column("user_id")
            if user_id not in set(ids.to_pylist()):
                self.store.mark_file_done(task["task_id"], file_id)
                return
            table = storage.read_table(self.config.data_dir, info["path"])
            kept = table.filter(pc.not_equal(table.column("user_id"), user_id))
            if kept.num_rows == 0:
                # the file held only erased rows: drop it entirely
                self.store.record_delete_only(task["task_id"], file_id)
            else:
                new_id, rel_path = storage.write_table_atomic(
                    self.config.data_dir, date, kept
                )
                self.store.record_rewrite(
                    task["task_id"], file_id, new_id, date, rel_path, kept.num_rows
                )
            storage.delete_file(self.config.data_dir, info["path"])
            self.store.mark_file_deleted(file_id)

    def verify_clean(self, task) -> bool:
        """Final sweep: prove that no active file in the user's dates still
        contains the user. Re-queues anything found and returns False."""
        user_id = task["user_id"]
        dates = self.store.dates_for_user(user_id)
        suspect = []
        for info in self.store.active_files_in_dates(dates):
            ids = storage.read_table(
                self.config.data_dir, info["path"], columns=["user_id"]
            ).column("user_id")
            if user_id in set(ids.to_pylist()):
                suspect.append(info["file_id"])
        if suspect:
            log.warning(
                "task %s: verification found %d suspect file(s)",
                task["task_id"],
                len(suspect),
            )
            self.store.add_pending_files(task["task_id"], suspect)
            return False
        return True

    def finalize(self, task) -> None:
        self.store.finalize_task(task["task_id"], task["user_id"])
        log.info(
            "erasure task %s for user %r completed", task["task_id"], task["user_id"]
        )


class ErasureWorker(threading.Thread):
    """Single background worker. Sequential processing is deliberate: it is
    what makes overlapping erasures of the same files safe."""

    def __init__(self, service: ErasureService):
        super().__init__(name="erasure-worker", daemon=True)
        self.service = service
        self._queue: queue.Queue[str] = queue.Queue()
        self._stop_event = threading.Event()
        self._timers: list[threading.Timer] = []

    def submit(self, task_id: str) -> None:
        self._queue.put(task_id)

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        self._queue.put("")
        self.join(timeout=timeout)

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                task_id = self._queue.get(timeout=self.service.config.worker_poll)
            except queue.Empty:
                continue
            if not task_id:
                continue
            try:
                self.process(task_id)
            except Exception:  # pragma: no cover - defensive
                log.exception("unexpected worker error for task %s", task_id)

    def process(self, task_id: str) -> None:
        store = self.service.store
        task = store.get_task(task_id)
        if task is None or task["status"] == "completed":
            return
        store.mark_processing(task_id)
        task = store.get_task(task_id)
        try:
            while True:
                while True:
                    row = store.next_pending_file(task_id)
                    if row is None:
                        break
                    self.service.erase_file(task, row["file_id"])
                if self.service.verify_clean(task):
                    break
            self.service.finalize(task)
        except Exception as exc:
            # stay restricted: the task is retried, and a restart resumes it
            log.error("erasure task %s failed: %s", task_id, exc)
            store.mark_failed(task_id, str(exc))
            self._schedule_retry(task_id)

    def _schedule_retry(self, task_id: str) -> None:
        timer = threading.Timer(
            self.service.config.retry_delay, self.submit, args=(task_id,)
        )
        timer.daemon = True
        timer.start()
        self._timers.append(timer)
