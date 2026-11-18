from __future__ import annotations

import csv
import io
import threading

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from app import storage
from app.main import create_app

from tests.test_api import (  # reuse the existing helpers
    csv_bytes,
    make_client,
    make_config,
    upload,
    wait_task,
)


# ------------------------------------------------------------------ helpers


def page_counts_from_csv(text: str) -> dict:
    counts: dict[str, int] = {}
    for row in list(csv.reader(io.StringIO(text)))[1:]:
        page = row[1]
        counts[page] = counts.get(page, 0) + 1
    return counts


def reconcile(client: TestClient, date: str, pages: dict, expected_code=(201,)):
    return client.post("/reconcile", json={"date": date, "pages": pages})


def voucher_download(client: TestClient, voucher_id: str, **params):
    return client.get(
        "/report/download", params={"voucher_id": voucher_id, **params}
    )


# ------------------------------------------------------------------- tests


def test_reconcile_success_issues_voucher_and_downloads_raw_clicks(tmp_path):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/home", "2026-11-01T10:00:00Z"),
                ("bob", "/home", "2026-11-01T10:05:00Z"),
                ("carol", "/pricing", "2026-11-01T10:06:00Z"),
            ],
        )
        upload(
            client,
            "2026-11-01",
            [("dave", "/home", "2026-11-01T11:00:00Z")],
        )

        r = reconcile(client, "2026-11-01", {"/home": 3, "/pricing": 1})
        assert r.status_code == 201, r.text
        body = r.json()
        voucher_id = body["voucher_id"]
        assert body["files"] == 2
        assert len(body["file_ids"]) == 2
        assert body["actual_counts"] == {"/home": 3, "/pricing": 1}

        r = voucher_download(client, voucher_id)
        assert r.status_code == 200
        assert page_counts_from_csv(r.text) == {"/home": 3, "/pricing": 1}
        assert r.headers["content-type"].startswith("text/csv")

        # parquet format works too and carries the canonical schema
        r = voucher_download(client, voucher_id, format="parquet")
        assert r.status_code == 200
        table = pq.read_table(io.BytesIO(r.content))
        assert table.num_rows == 4
        assert str(table.schema.field("ts").type) == "timestamp[us, tz=UTC]"

        # the voucher inspection endpoint reflects the issued credential
        status = client.get(f"/vouchers/{voucher_id}").json()
        assert status["status"] == "valid"
        assert status["date"] == "2026-11-01"
        assert status["expected_counts"] == {"/home": 3, "/pricing": 1}
        assert len(status["file_ids"]) == 2


def test_reconcile_mismatch_issues_no_voucher_and_reports_actual(tmp_path):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/home", "2026-11-01T10:00:00Z"),
                ("bob", "/pricing", "2026-11-01T10:05:00Z"),
            ],
        )
        r = reconcile(client, "2026-11-01", {"/home": 5, "/pricing": 1})
        assert r.status_code == 409
        detail = r.json()["detail"]
        assert detail["error"] == "count_mismatch"
        assert detail["actual"] == {"/home": 1, "/pricing": 1}
        assert detail["expected"] == {"/home": 5, "/pricing": 1}

        # an extra page in the checklist is also a mismatch
        r = reconcile(client, "2026-11-01", {"/home": 1, "/pricing": 1, "/x": 0})
        assert r.status_code == 409

        # a missing page in the checklist is a mismatch
        r = reconcile(client, "2026-11-01", {"/home": 1})
        assert r.status_code == 409


def test_resubmission_invalidates_old_voucher_even_when_new_one_mismatches(tmp_path):
    with make_client(tmp_path) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        r1 = reconcile(client, "2026-11-01", {"/a": 1})
        assert r1.status_code == 201
        old_voucher = r1.json()["voucher_id"]
        assert voucher_download(client, old_voucher).status_code == 200

        # resubmit a checklist that does NOT match: old voucher must still die
        r2 = reconcile(client, "2026-11-01", {"/a": 99})
        assert r2.status_code == 409
        r = voucher_download(client, old_voucher)
        assert r.status_code == 409
        assert r.json()["detail"]["error"] == "voucher_invalid"

        status = client.get(f"/vouchers/{old_voucher}").json()
        assert status["status"] == "invalidated"
        assert status["invalidated_reason"] == "reconciled_again"

        # a later correct submission issues a fresh voucher, the old one stays dead
        r3 = reconcile(client, "2026-11-01", {"/a": 1})
        assert r3.status_code == 201
        new_voucher = r3.json()["voucher_id"]
        assert new_voucher != old_voucher
        assert voucher_download(client, new_voucher).status_code == 200
        assert voucher_download(client, old_voucher).status_code == 409


def test_new_upload_invalidates_outstanding_voucher(tmp_path):
    with make_client(tmp_path) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        voucher = reconcile(client, "2026-11-01", {"/a": 1}).json()["voucher_id"]
        assert voucher_download(client, voucher).status_code == 200

        # new clicks land in the lake for the same day
        upload(client, "2026-11-01", [("bob", "/b", "2026-11-01T01:00:00Z")])

        r = voucher_download(client, voucher)
        assert r.status_code == 409
        status = client.get(f"/vouchers/{voucher}").json()
        assert status["status"] == "invalidated"
        assert status["invalidated_reason"] == "new_upload"

        # after re-checking with the new counts a new voucher works
        voucher2 = reconcile(
            client, "2026-11-01", {"/a": 1, "/b": 1}
        ).json()["voucher_id"]
        assert voucher_download(client, voucher2).status_code == 200


def test_erasure_acceptance_invalidates_voucher_and_blocks_reconciliation(tmp_path):
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/a", "2026-11-01T00:00:00Z"),
                ("bob", "/b", "2026-11-01T00:01:00Z"),
            ],
        )
        voucher = reconcile(client, "2026-11-01", {"/a": 1, "/b": 1}).json()["voucher_id"]

        service = client.app.state.service
        gate = threading.Event()
        original_erase = service.erase_file

        def slow_erase(task, file_id):
            gate.wait(10)
            return original_erase(task, file_id)

        service.erase_file = slow_erase
        try:
            r = client.post("/erasure", json={"user_id": "alice"})
            task_id = r.json()["task_id"]

            # the day is now restricted: the old voucher cannot download
            r = voucher_download(client, voucher)
            assert r.status_code == 409
            status = client.get(f"/vouchers/{voucher}").json()
            assert status["invalidated_reason"] == "erasure_accepted"

            # and a fresh reconciliation is refused while the erasure runs
            r = reconcile(client, "2026-11-01", {"/a": 1, "/b": 1})
            assert r.status_code == 409

            # other days keep serving normally
            upload(client, "2026-11-02", [("carol", "/c", "2026-11-02T00:00:00Z")])
            v2 = reconcile(client, "2026-11-02", {"/c": 1}).json()["voucher_id"]
            assert voucher_download(client, v2).status_code == 200
        finally:
            gate.set()
            service.erase_file = original_erase

        wait_task(client, task_id)
        # after completion the day must be re-checked before a new download
        r = voucher_download(client, voucher)
        assert r.status_code == 409
        # alice is gone, so the checklist must be re-derived
        r = reconcile(client, "2026-11-01", {"/b": 1})
        assert r.status_code == 201
        assert voucher_download(client, r.json()["voucher_id"]).status_code == 200


def test_voucher_pins_its_file_set_and_serves_only_those_files(tmp_path):
    """A voucher corresponds to one complete set of active files; an erasure
    rewrite cannot make it serve different or partial data."""
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/a", "2026-11-01T00:00:00Z"),
                ("bob", "/b", "2026-11-01T00:01:00Z"),
            ],
        )
        voucher = reconcile(client, "2026-11-01", {"/a": 1, "/b": 1}).json()["voucher_id"]

        task_id = client.post("/erasure", json={"user_id": "alice"}).json()["task_id"]
        wait_task(client, task_id)

        # the voucher was invalidated at acceptance; it never comes back to life
        r = voucher_download(client, voucher)
        assert r.status_code == 409
        status = client.get(f"/vouchers/{voucher}").json()
        assert status["status"] == "invalidated"


def test_in_progress_voucher_download_finishes_against_its_file_set(tmp_path):
    """Once a voucher download has started, an erasure rewrite of the same day
    waits on the date lock until the pinned file set has been fully read."""
    with make_client(tmp_path) as client:
        upload(
            client,
            "2026-11-01",
            [
                ("alice", "/a", "2026-11-01T00:00:00Z"),
                ("bob", "/b", "2026-11-01T00:01:00Z"),
            ],
        )
        voucher = reconcile(client, "2026-11-01", {"/a": 1, "/b": 1}).json()["voucher_id"]
        service = client.app.state.service

        started = threading.Event()
        release = threading.Event()
        real_read = storage.read_table

        def slow_read(data_dir, rel_path, columns=None):
            if columns is None:  # only slow the full download reads
                started.set()
                release.wait(10)
            return real_read(data_dir, rel_path, columns=columns)

        storage.read_table = slow_read
        result: dict = {}

        def do_download():
            result["resp"] = voucher_download(client, voucher)

        t = threading.Thread(target=do_download)
        t.start()
        assert started.wait(5)
        try:
            # erasure acceptance happens while the download read is in flight;
            # the voucher is invalidated but the running download keeps its
            # pinned files and completes
            task_id = client.post(
                "/erasure", json={"user_id": "alice"}
            ).json()["task_id"]
            # give the worker a moment to show it cannot rip files away
            assert t.is_alive()
        finally:
            storage.read_table = real_read
            release.set()
        t.join(timeout=5)

        resp = result["resp"]
        assert resp.status_code == 200
        assert page_counts_from_csv(resp.text) == {"/a": 1, "/b": 1}

        wait_task(client, task_id)
        # subsequent use of the same voucher is refused
        assert voucher_download(client, voucher).status_code == 409


def test_disk_read_failure_during_reconcile_creates_no_voucher(tmp_path, monkeypatch):
    with TestClient(create_app(make_config(tmp_path)), raise_server_exceptions=False) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])

        def boom(*args, **kwargs):
            raise OSError("unreadable parquet")

        monkeypatch.setattr(storage, "read_table", boom)
        r = reconcile(client, "2026-11-01", {"/a": 1})
        assert r.status_code == 500

        service = client.app.state.service
        rows = service.store.query("SELECT * FROM reconciliation_vouchers")
        assert rows == []
        monkeypatch.undo()

        # nothing was issued, so there is no credential to download with
        r = client.get("/report/download", params={"voucher_id": "anything"})
        assert r.status_code == 409


def test_registration_failure_during_reconcile_creates_no_usable_voucher(tmp_path, monkeypatch):
    with TestClient(create_app(make_config(tmp_path)), raise_server_exceptions=False) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        service = client.app.state.service

        def boom(*args, **kwargs):
            raise OSError("state db write failed")

        monkeypatch.setattr(service.store, "issue_voucher", boom)
        r = reconcile(client, "2026-11-01", {"/a": 1})
        assert r.status_code == 500
        assert service.store.query("SELECT * FROM reconciliation_vouchers") == []
        monkeypatch.undo()

        # and the earlier (pre-failure) invalidation still holds: no stray
        # valid vouchers exist for the day
        r = client.get(
            "/report/download", params={"voucher_id": "nope"}
        )
        assert r.status_code == 409


def test_reconcile_validation_and_edge_cases(tmp_path):
    with make_client(tmp_path) as client:
        # bad date shape
        r = client.post("/reconcile", json={"date": "2026-13-40", "pages": {"/a": 1}})
        assert r.status_code == 422

        # negative counts rejected
        r = client.post(
            "/reconcile", json={"date": "2026-11-01", "pages": {"/a": -1}}
        )
        assert r.status_code == 422

        # empty checklist rejected
        r = client.post("/reconcile", json={"date": "2026-11-01", "pages": {}})
        assert r.status_code == 422

        # no data for the day
        r = reconcile(client, "2026-11-01", {"/a": 1})
        assert r.status_code == 404

        # unknown voucher
        assert client.get("/vouchers/ghost").status_code == 404
        r = voucher_download(client, "ghost")
        assert r.status_code == 409

        # missing voucher_id parameter
        assert client.get("/report/download").status_code == 422
        # bad format
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        voucher = reconcile(client, "2026-11-01", {"/a": 1}).json()["voucher_id"]
        r = client.get(
            "/report/download", params={"voucher_id": voucher, "format": "xml"}
        )
        assert r.status_code == 422


def test_blocked_only_upload_does_not_destroy_voucher(tmp_path):
    """An upload whose rows are all blocked by an erasure writes no file, so the
    day's active file set is unchanged and an outstanding voucher survives."""
    with make_client(tmp_path) as client:
        upload(client, "2026-11-01", [("bob", "/b", "2026-11-01T00:00:00Z")])
        voucher = reconcile(client, "2026-11-01", {"/b": 1}).json()["voucher_id"]

        client.post("/erasure", json={"user_id": "alice"})  # alice has no data
        r = upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T02:00:00Z")])
        assert r.json()["stored"] == 0
        assert r.json()["blocked"] == 1

        # nothing entered the lake for the day: the voucher still works
        assert voucher_download(client, voucher).status_code == 200


def test_voucher_survives_restart(tmp_path):
    config = make_config(tmp_path)
    with TestClient(create_app(config)) as client:
        upload(client, "2026-11-01", [("alice", "/a", "2026-11-01T00:00:00Z")])
        voucher = reconcile(client, "2026-11-01", {"/a": 1}).json()["voucher_id"]

    # restart the whole service against the same data/state directories
    with TestClient(create_app(config)) as client2:
        assert voucher_download(client2, voucher).status_code == 200
        status = client2.get(f"/vouchers/{voucher}").json()
        assert status["status"] == "valid"
        assert len(status["file_ids"]) == 1
