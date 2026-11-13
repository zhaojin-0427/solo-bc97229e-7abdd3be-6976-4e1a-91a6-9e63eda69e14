"""Withdrawal (撤档) workflow tests.

Covers the full lifecycle: idempotent receipts, disappearance from the
catalog, 404 on new searches, completion of searches that already selected a
version, withdrawal while an upload is in flight, same-id re-archival protected
by the epoch guard, in-flight read deferral, durable reclaim scope with
restart resumption, and isolation from other documents.
"""
from __future__ import annotations

import json
import threading
import time
import unittest

from app import config, db, ingest, reclaim, search
from app.db import get_conn

from tests.helpers import (
    block,
    json_body,
    make_app,
    ndjson,
    reset_db,
    wsgi_request,
)


def table_names() -> set[str]:
    conn = get_conn()
    try:
        return {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    finally:
        conn.close()


def wait_until(predicate, timeout: float = 10.0, interval: float = 0.02):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


class WithdrawalTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # Stop any worker left by a previous test *before* resetting the db,
        # so it cannot reclaim rows out from under this test.
        reclaim.worker.stop()
        reset_db()
        self.app = make_app()
        reclaim.worker.stop()  # deterministic: drive reclaim explicitly below

    def tearDown(self) -> None:
        reclaim.worker.stop()

    def _upload(self, doc_id: str, text: str = "撤档前的档案内容 alpha",
                bid: str = "b1") -> int:
        status, body = wsgi_request(
            self.app, "PUT", f"/documents/{doc_id}",
            ndjson([block(bid, 1, text)]),
        )
        self.assertEqual(status, 202, body)
        return body["version_id"]

    def _withdraw(self, doc_id: str, wid: str = "W-1"):
        return wsgi_request(
            self.app,
            "POST",
            f"/documents/{doc_id}/withdrawals",
            json_body({"withdrawal_id": wid}),
            content_type="application/json",
        )

    # ---- basic withdrawal semantics --------------------------------------

    def test_withdraw_hides_document_and_search_404_and_drops_tables(self):
        vid = self._upload("doc-w")
        self.assertIn(f"blocks_{vid}", table_names())

        status, receipt = self._withdraw("doc-w", "WD-001")
        self.assertEqual(status, 202, receipt)
        self.assertEqual(receipt["withdrawal_id"], "WD-001")
        self.assertFalse(receipt["replayed"])
        self.assertEqual(receipt["state"], db.WITHDRAWAL_ACCEPTED)
        self.assertEqual([i["version_id"] for i in receipt["items"]], [vid])
        self.assertTrue(all(i["state"] == db.ITEM_PENDING for i in receipt["items"]))

        # Immediately invisible in the catalog and 404 for fresh searches.
        status, docs = wsgi_request(self.app, "GET", "/documents")
        self.assertEqual(status, 200)
        self.assertNotIn("doc-w", {d["doc_id"] for d in docs})
        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-w/search", query={"q": "alpha"}
        )
        self.assertEqual(status, 404)

        # Reclaim completes; only then do the physical tables disappear.
        reclaim.process_pending()
        status, receipt = wsgi_request(
            self.app, "GET", "/withdrawals/WD-001"
        )
        self.assertEqual(status, 200)
        self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)
        self.assertIsNotNone(receipt["completed_at"])
        self.assertTrue(all(i["state"] == db.ITEM_DONE for i in receipt["items"]))
        for suffix in ("blocks", "unigrams", "bigrams"):
            self.assertNotIn(f"{suffix}_{vid}", table_names())

    def test_duplicate_withdrawal_id_returns_first_receipt(self):
        vid = self._upload("doc-idem")
        status, first = self._withdraw("doc-idem", "DUP-9")
        self.assertEqual(status, 202)
        reclaim.process_pending()

        # Same number again -> 200 with the exact original receipt, no new row.
        status, second = self._withdraw("doc-idem", "DUP-9")
        self.assertEqual(status, 200, second)
        self.assertTrue(second["replayed"])
        self.assertEqual(second["created_at"], first["created_at"])
        self.assertEqual(second["state"], db.WITHDRAWAL_COMPLETED)

        conn = get_conn()
        try:
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM withdrawals WHERE withdrawal_id = 'DUP-9'"
            ).fetchone()["c"]
        finally:
            conn.close()
        self.assertEqual(count, 1)

    def test_withdrawal_id_collision_across_documents_is_409(self):
        self._upload("doc-a")
        self._upload("doc-b")
        status, _ = self._withdraw("doc-a", "SHARED")
        self.assertEqual(status, 202)
        status, body = self._withdraw("doc-b", "SHARED")
        self.assertEqual(status, 409)
        self.assertIn("doc-a", body["detail"])
        # doc-b must be untouched and still searchable.
        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-b/search", query={"q": "alpha"}
        )
        self.assertEqual(status, 200)

    def test_withdraw_unknown_document_404(self):
        status, body = self._withdraw("ghost", "WX")
        self.assertEqual(status, 404)
        status, _ = wsgi_request(self.app, "GET", "/withdrawals/WX")
        self.assertEqual(status, 404)

    def test_withdraw_payload_validation(self):
        self._upload("doc-val")
        for payload in (b"", b"{not json", b"[]", json.dumps({}).encode(),
                        json.dumps({"withdrawal_id": ""}).encode(),
                        json.dumps({"withdrawal_id": 7}).encode()):
            status, body = wsgi_request(
                self.app, "POST", "/documents/doc-val/withdrawals",
                payload, content_type="application/json",
            )
            self.assertEqual(status, 400, payload)

    # ---- same-id re-archival ----------------------------------------------

    def test_reupload_after_withdrawal_is_new_epoch_and_searchable(self):
        old_vid = self._upload("doc-re", "旧档案内容 old text")
        self._withdraw("doc-re", "WR-1")
        reclaim.process_pending()

        # Same doc id, corrected content -> a brand-new generation.
        new_vid = self._upload("doc-re", "正确的新档案内容 corrected text", bid="c1")
        self.assertNotEqual(old_vid, new_vid)
        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-re/search", query={"q": "corrected"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["version_id"], new_vid)
        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-re/search", query={"q": "old text"}
        )
        self.assertEqual(body["result_count"], 0)

        # The document is back in the catalog and old tables stay gone.
        status, docs = wsgi_request(self.app, "GET", "/documents")
        self.assertIn("doc-re", {d["doc_id"] for d in docs})
        self.assertNotIn(f"blocks_{old_vid}", table_names())
        self.assertIn(f"blocks_{new_vid}", table_names())

        # Versions endpoint preserves the audit trail of the withdrawn epoch.
        status, versions = wsgi_request(
            self.app, "GET", "/documents/doc-re/versions"
        )
        self.assertEqual(status, 200)
        states = {v["version_id"]: v["state"] for v in versions}
        self.assertEqual(states[old_vid], db.STATE_WITHDRAWN)
        self.assertEqual(states[new_vid], db.STATE_READY)

    def test_withdraw_during_inflight_upload_old_job_cannot_revive_archive(self):
        # Start a version directly (ingesting), exactly like a request sitting
        # in _stream_ingest, then withdraw while the upload is still open.
        # The app was created in setUp, so no startup recovery runs in between.
        conn = get_conn()
        try:
            db.init_schema(conn)
            inflight_vid = db.create_version(conn, "doc-inflight")
        finally:
            conn.close()

        # Withdraw through the DB layer so no background reclaim task is
        # scheduled: the old upload must still find its staging tables alive.
        conn = get_conn()
        try:
            db.withdraw_document(conn, "doc-inflight", "WF-1")
        finally:
            conn.close()
        conn = get_conn()
        try:
            receipt = db.get_withdrawal(conn, "WF-1")
            self.assertEqual(
                [int(i["version_id"]) for i in db.list_withdrawal_items(conn, "WF-1")],
                [inflight_vid],
            )
        finally:
            conn.close()
        self.assertEqual(receipt["state"], db.WITHDRAWAL_ACCEPTED)

        # The old upload finishes streaming *after* the withdrawal; its tables
        # were preserved for the in-flight request and ingestion can tail out.
        conn = get_conn()
        try:
            count = ingest.ingest_version(
                conn,
                inflight_vid,
                iter([ndjson([block("late", 1, "迟到的旧上传数据")])]),
            )
        finally:
            conn.close()
        # Its background promotion must refuse to resurrect the archive.
        with self.assertRaises(db.StaleVersionError):
            c2 = get_conn()
            try:
                db.activate_version(c2, "doc-inflight", inflight_vid, count)
            finally:
                c2.close()

        # Same id re-upload works at once (old version is terminal).
        new_vid = self._upload("doc-inflight", "重传后的新数据 fresh", bid="n1")
        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-inflight/search",
            query={"q": "fresh"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["version_id"], new_vid)

        # Reclaim removes only the old generation's tables, never the new ones.
        reclaim.process_pending()
        names = table_names()
        self.assertNotIn(f"blocks_{inflight_vid}", names)
        self.assertIn(f"blocks_{new_vid}", names)
        status, receipt = wsgi_request(self.app, "GET", "/withdrawals/WF-1")
        self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)

    def test_background_indexer_silently_skips_stale_version(self):
        from app import main as main_mod

        # Old upload ingests fully; withdraw before its background activation.
        old_vid = self._upload("doc-stale", "旧版 stale text")
        # Manually withdraw, then run the background indexer as the request
        # layer would after the upload response.
        conn = get_conn()
        try:
            db.withdraw_document(conn, "doc-stale", "WS-1")
        finally:
            conn.close()
        main_mod._index_in_background("doc-stale", old_vid, 1)

        conn = get_conn()
        try:
            row = db.get_version(conn, old_vid)
            doc = db.get_document(conn, "doc-stale")
        finally:
            conn.close()
        self.assertEqual(row["state"], db.STATE_WITHDRAWN)  # not 'ready'
        self.assertIsNone(doc["active_version_id"])

    def test_old_receipt_replay_does_not_touch_rearchived_data(self):
        old_vid = self._upload("doc-rr", "first generation")
        self._withdraw("doc-rr", "RR-1")
        reclaim.process_pending()
        new_vid = self._upload("doc-rr", "second generation", bid="g2")

        # Replaying the old receipt (e.g. a client retrying) must stay scoped
        # to the old generation and never reclaim new tables.
        status, receipt = self._withdraw("doc-rr", "RR-1")
        self.assertEqual(status, 200)
        reclaim.process_pending()
        self.assertEqual(
            [i["version_id"] for i in receipt["items"]], [old_vid]
        )
        names = table_names()
        self.assertIn(f"blocks_{new_vid}", names)
        self.assertNotIn(f"blocks_{old_vid}", names)

    # ---- in-flight read protection ----------------------------------------

    def test_inflight_search_completes_and_drop_is_deferred(self):
        vid = self._upload("doc-read", "需要保留读取的内容 marker")
        entered = threading.Event()
        release = threading.Event()
        old_timeout = config.RECLAIM_READ_WAIT_SECONDS
        config.RECLAIM_READ_WAIT_SECONDS = 0.2
        try:
            def slow_search():
                conn = get_conn()
                try:
                    with reclaim.read_guard(vid):
                        entered.set()
                        release.wait(timeout=10)
                        return search.search(conn, vid, "marker")
                finally:
                    conn.close()

            thread = threading.Thread(target=slow_search)
            thread.start()
            self.assertTrue(entered.wait(timeout=5))
            self.assertIn(vid, reclaim.reads.active_versions())

            # Withdraw while the search holds its read guard.
            status, _ = self._withdraw("doc-read", "WRD-1")
            self.assertEqual(status, 202)

            # Reclaim passes must leave the item pending and the tables intact
            # while the reader is registered.
            reclaim.process_pending()
            reclaim.process_pending()
            self.assertIn(f"blocks_{vid}", table_names())
            conn = get_conn()
            try:
                item = db.get_pending_item(conn, "WRD-1")
            finally:
                conn.close()
            self.assertIsNotNone(item)
            self.assertEqual(int(item["version_id"]), vid)

            # Once the read finishes, the next pass drops the tables.
            release.set()
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            reclaim.process_pending()
            self.assertNotIn(f"blocks_{vid}", table_names())
            status, receipt = wsgi_request(
                self.app, "GET", "/withdrawals/WRD-1"
            )
            self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)
        finally:
            config.RECLAIM_READ_WAIT_SECONDS = old_timeout

    # ---- durability / restart ---------------------------------------------

    def test_reclaim_resumes_after_restart_and_records_errors(self):
        v1 = self._upload("doc-restart", "gen one", bid="a")
        # A second replacement gives the withdrawal two scoped versions.
        v2 = self._upload("doc-restart", "gen one revised", bid="b")
        self.assertNotEqual(v1, v2)

        # Withdraw through the DB layer so no endpoint background task runs;
        # this leaves the reclaim scope pending for a controlled failure.
        conn = get_conn()
        try:
            db.withdraw_document(conn, "doc-restart", "WRS-1")
            db.set_withdrawal_state(
                conn, "WRS-1", db.WITHDRAWAL_FAILED,
                error="reclaim failed: simulated disk error",
            )
        finally:
            conn.close()
        status, receipt = wsgi_request(self.app, "GET", "/withdrawals/WRS-1")
        self.assertEqual(receipt["state"], db.WITHDRAWAL_FAILED)
        self.assertIn("simulated disk error", receipt["error"])
        self.assertTrue(all(i["state"] == db.ITEM_PENDING for i in receipt["items"]))

        # The next reclaim pass (same mechanism as a restart resume) retries,
        # clears the error and finishes the durable scope.
        reclaim.process_pending()
        status, receipt = wsgi_request(self.app, "GET", "/withdrawals/WRS-1")
        self.assertEqual(status, 200)
        self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)
        self.assertIsNone(receipt["error"])
        names = table_names()
        for vid in (v1, v2):
            self.assertNotIn(f"blocks_{vid}", names)

        # Restart with an already-complete receipt: nothing is re-done and the
        # startup recovery stays idempotent.
        app2 = make_app()
        reclaim.worker.stop()
        conn = get_conn()
        try:
            messages = db.startup_recovery(conn)
        finally:
            conn.close()
        self.assertFalse(any("WRS-1" in m for m in messages))
        status, receipt = wsgi_request(app2, "GET", "/withdrawals/WRS-1")
        self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)

    def test_startup_recovery_folds_interrupted_upload_into_withdrawal(self):
        # Version stuck INDEXING at process death, then a surviving node
        # records the withdrawal before the process comes back up.
        conn = get_conn()
        try:
            db.init_schema(conn)
            vid = db.create_version(conn, "doc-crash-w")
            ingest.ingest_version(
                conn, vid, iter([ndjson([block("b1", 1, "half done")])])
            )
            db.set_state(conn, vid, db.STATE_INDEXING)
            db.withdraw_document(conn, "doc-crash-w", "WC-1")
        finally:
            conn.close()

        # Fresh app instance performs startup recovery.
        app2 = make_app()
        reclaim.worker.stop()
        conn = get_conn()
        try:
            row = db.get_version(conn, vid)
            items = db.list_withdrawal_items(conn, "WC-1")
            receipt = db.get_withdrawal(conn, "WC-1")
        finally:
            conn.close()
        # The interrupted upload belongs to the withdrawn generation and must
        # never be resurrected; its scope stays durable for the reclaimer.
        self.assertEqual(row["state"], db.STATE_WITHDRAWN)
        self.assertIn(vid, [int(i["version_id"]) for i in items])
        self.assertIsNotNone(receipt)

        reclaim.process_pending()
        self.assertNotIn(f"blocks_{vid}", table_names())
        status, receipt = wsgi_request(app2, "GET", "/withdrawals/WC-1")
        self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)

    def test_startup_backfills_reclaim_scope_lost_before_restart(self):
        # Withdrawal recorded, but its scope rows vanished (interrupted write):
        # restart backfills them and cleanup still finishes.
        conn = get_conn()
        try:
            db.init_schema(conn)
            vid = db.create_version(conn, "doc-backfill")
            ingest.ingest_version(
                conn, vid, iter([ndjson([block("b1", 1, "content")])])
            )
            db.activate_version(conn, "doc-backfill", vid, 1)
            db.withdraw_document(conn, "doc-backfill", "WB-1")
            conn.execute(
                "DELETE FROM withdrawal_items WHERE withdrawal_id = 'WB-1'"
            )
            conn.commit()
        finally:
            conn.close()

        app2 = make_app()
        reclaim.worker.stop()
        conn = get_conn()
        try:
            messages = db.startup_recovery(conn)
            items = db.list_withdrawal_items(conn, "WB-1")
        finally:
            conn.close()
        self.assertIn(vid, [int(i["version_id"]) for i in items])
        self.assertTrue(any("WB-1" in m for m in messages))
        reclaim.process_pending()
        self.assertNotIn(f"blocks_{vid}", table_names())

    # ---- isolation ---------------------------------------------------------

    def test_reclaim_waits_for_old_upload_job_to_exit(self):
        vid = self._upload("doc-job", "job guard content")
        # Simulate an old execution still finishing after the withdrawal
        # (exactly what happens when withdraw races an in-flight upload).
        reclaim.jobs.register(vid)
        try:
            conn = get_conn()
            try:
                db.withdraw_document(conn, "doc-job", "WJ-1")
            finally:
                conn.close()

            old_timeout = config.RECLAIM_READ_WAIT_SECONDS
            config.RECLAIM_READ_WAIT_SECONDS = 0.1
            try:
                reclaim.process_pending()
            finally:
                config.RECLAIM_READ_WAIT_SECONDS = old_timeout

            # Job still active: item pending, tables intact, receipt not done.
            self.assertIn(f"blocks_{vid}", table_names())
            conn = get_conn()
            try:
                self.assertIsNotNone(db.get_pending_item(conn, "WJ-1"))
                state = db.get_withdrawal(conn, "WJ-1")["state"]
            finally:
                conn.close()
            self.assertEqual(state, db.WITHDRAWAL_RECLAIMING)

            # Once the stale job exits, cleanup finishes immediately.
            reclaim.jobs.release(vid)
            reclaim.process_pending()
            self.assertNotIn(f"blocks_{vid}", table_names())
            status, receipt = wsgi_request(self.app, "GET", "/withdrawals/WJ-1")
            self.assertEqual(receipt["state"], db.WITHDRAWAL_COMPLETED)
        finally:
            # Defensive cleanup in case an assertion failed mid-test.
            reclaim.jobs.release(vid)

    def test_other_documents_remain_available_throughout(self):
        keep_vid = self._upload("doc-keep", "不受影响的邻居文档")
        gone_vid = self._upload("doc-gone", "即将撤下的文档")
        self._withdraw("doc-gone", "WK-1")
        reclaim.process_pending()

        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-keep/search",
            query={"q": "邻居"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["version_id"], keep_vid)
        # New uploads for other documents keep working.
        new_vid = self._upload("doc-keep-2", "另一份正常档案", bid="k2")
        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-keep-2/search",
            query={"q": "正常"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["version_id"], new_vid)
        self.assertIn(f"blocks_{keep_vid}", table_names())
        self.assertNotIn(f"blocks_{gone_vid}", table_names())

    def test_second_withdrawal_after_reupload_uses_new_epoch_safely(self):
        v1 = self._upload("doc-cycle", "generation one text")
        self._withdraw("doc-cycle", "C-1")
        reclaim.process_pending()
        v2 = self._upload("doc-cycle", "generation two text", bid="g2")
        self._withdraw("doc-cycle", "C-2")
        reclaim.process_pending()
        v3 = self._upload("doc-cycle", "generation three text", bid="g3")

        status, body = wsgi_request(
            self.app, "GET", "/documents/doc-cycle/search", query={"q": "three"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["version_id"], v3)
        names = table_names()
        for old in (v1, v2):
            self.assertNotIn(f"blocks_{old}", names)
        self.assertIn(f"blocks_{v3}", names)

        conn = get_conn()
        try:
            epochs = {
                r["withdrawal_id"]: int(r["epoch"])
                for r in conn.execute(
                    "SELECT withdrawal_id, epoch FROM withdrawals ORDER BY created_at"
                )
            }
        finally:
            conn.close()
        self.assertEqual(epochs, {"C-1": 0, "C-2": 1})


if __name__ == "__main__":
    unittest.main()
