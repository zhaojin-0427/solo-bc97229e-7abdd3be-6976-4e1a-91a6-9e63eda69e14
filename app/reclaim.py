"""Background reclamation of withdrawn versions.

A withdrawal makes a document disappear immediately (the catalog switch is
synchronous, see :func:`app.db.withdraw_document`). Reclaiming the physical
storage -- the per-version ``blocks_<vid>`` / ``unigrams_<vid>`` /
``bigrams_<vid>`` tables -- is deliberately asynchronous and fully durable:

* every version to remove is persisted as a ``withdrawal_items`` row, so the
  reclaim scope survives failures and process restarts;
* a version whose tables are currently being read by an in-flight search is
  only dropped once that read finishes (readers registered through
  :class:`ReadGuard`); the item stays ``pending`` and the worker retries;
* each successful drop is committed individually, and the receipt only reaches
  ``completed`` when the old upload jobs have exited *and* every scoped table is
  gone; a failure flips the receipt to ``failed`` with the error text and is
  retried automatically on the next worker tick / process restart;
* reclamation is keyed by explicit version ids, never by ``doc_id``, so a fresh
  archive reusing the same document id can never be mistaken for old data.

The engine runs both as a small background worker thread (bounded tick
interval) and as a one-shot task kicked off by the withdrawal endpoint; all
entry points execute the same idempotent :func:`process_pending`.
"""
from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from . import config, db

logger = logging.getLogger("archive.reclaim")


class _ReadRegistry:
    """In-process registry of versions with an in-flight read.

    Single uvicorn worker means every search shares this registry; a condition
    variable lets the reclaimer sleep exactly until a reader finishes instead
    of polling the database.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._readers: dict[int, int] = {}

    def register(self, vid: int) -> None:
        with self._cond:
            self._readers[vid] = self._readers.get(vid, 0) + 1

    def release(self, vid: int) -> None:
        with self._cond:
            count = self._readers.get(vid, 0) - 1
            if count <= 0:
                self._readers.pop(vid, None)
            else:
                self._readers[vid] = count
            self._cond.notify_all()

    def wait_until_idle(self, vid: int, timeout: float) -> bool:
        """Block while readers of ``vid`` exist. Return True when idle."""
        with self._cond:
            self._cond.wait_for(lambda: vid not in self._readers, timeout)
            return vid not in self._readers

    def active_versions(self) -> tuple[int, ...]:
        with self._cond:
            return tuple(self._readers)


# Process-wide registry; imported and used directly by the request layer.
reads = _ReadRegistry()


class _JobRegistry:
    """Versions whose upload/ingestion job is still running.

    Reclamation of a withdrawn version waits until the old job exits, so the
    receipt only reports ``completed`` once both conditions hold: the stale
    execution has finished *and* its tables have been dropped.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition()
        self._jobs: set[int] = set()

    def register(self, vid: int) -> None:
        with self._cond:
            self._jobs.add(vid)

    def release(self, vid: int) -> None:
        with self._cond:
            self._jobs.discard(vid)
            self._cond.notify_all()

    def wait_until_done(self, vid: int, timeout: float) -> bool:
        with self._cond:
            self._cond.wait_for(lambda: vid not in self._jobs, timeout)
            return vid not in self._jobs


jobs = _JobRegistry()


@contextmanager
def job_guard(vid: int) -> Iterator[None]:
    """Track an in-flight ingestion/promotion job for version ``vid``."""
    jobs.register(vid)
    try:
        yield
    finally:
        jobs.release(vid)


@contextmanager
def read_guard(vid: int) -> Iterator[None]:
    """Keep version ``vid``'s tables alive for the duration of a search.

    The reclaimer sees the registration before a withdrawal transaction could
    hide the version and will defer the drop until ``release`` runs, so every
    search against a previously selected version completes against intact
    tables.
    """
    reads.register(vid)
    try:
        yield
    finally:
        reads.release(vid)


def _drop_when_idle(conn, withdrawal_id: str, vid: int) -> bool:
    """Drop one scoped version's tables.

    Returns True when the item is finished (dropped now, or the tables were
    already gone). Returns False while the old upload job is still running or a
    reader is still using the tables; the caller leaves the item pending and
    retries later.
    """
    if not jobs.wait_until_done(vid, config.RECLAIM_READ_WAIT_SECONDS):
        logger.info(
            "withdrawal %s: upload job for version %s still running; "
            "deferring table drop",
            withdrawal_id,
            vid,
        )
        return False
    if not reads.wait_until_idle(vid, config.RECLAIM_READ_WAIT_SECONDS):
        logger.info(
            "withdrawal %s: version %s still being read; deferring table drop",
            withdrawal_id,
            vid,
        )
        return False
    existing = {
        name
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    blocks, unigrams, bigrams = (
        f"blocks_{vid}",
        f"unigrams_{vid}",
        f"bigrams_{vid}",
    )
    with db.transaction(conn):
        for name in (bigrams, unigrams, blocks):
            if name in existing:
                conn.execute(f"DROP TABLE {name}")
    db.mark_item_done(conn, withdrawal_id, vid)
    logger.info("withdrawal %s: reclaimed tables of version %s", withdrawal_id, vid)
    return True


def process_pending() -> int:
    """Run one idempotent reclaim pass over all open withdrawals.

    Returns the number of receipts that reached a terminal state during this
    pass. Any exception is recorded on the affected receipt (state ``failed``)
    and retried on the next pass; the exception never escapes, since this runs
    in background threads.
    """
    conn = db.get_conn()
    completed_receipts = 0
    try:
        try:
            due = db.list_due_withdrawals(conn)
        except Exception as exc:  # schema not ready / transient db problem
            logger.warning("reclaim pass skipped: %s", exc)
            return 0
        for receipt in due:
            wid = receipt["withdrawal_id"]
            try:
                if receipt["state"] == db.WITHDRAWAL_FAILED:
                    # Retry: drop the stale error text and re-enter the
                    # reclaiming state in the same durable step.
                    db.set_withdrawal_state(
                        conn,
                        wid,
                        db.WITHDRAWAL_RECLAIMING,
                        clear_error=True,
                    )
                elif receipt["state"] != db.WITHDRAWAL_RECLAIMING:
                    db.set_withdrawal_state(conn, wid, db.WITHDRAWAL_RECLAIMING)
                while True:
                    item = db.get_pending_item(conn, wid)
                    if item is None:
                        break
                    vid = int(item["version_id"])
                    if not _drop_when_idle(conn, wid, vid):
                        # Reader still active; hand the remaining work to the
                        # next tick (or the wake-up triggered when it finishes).
                        return completed_receipts
                if db.remaining_item_count(conn, wid) == 0:
                    db.set_withdrawal_state(
                        conn,
                        wid,
                        db.WITHDRAWAL_COMPLETED,
                        completed=True,
                        clear_error=True,
                    )
                    completed_receipts += 1
                    logger.info("withdrawal %s completed", wid)
            except Exception as exc:
                logger.exception("reclaim failed for withdrawal %s", wid)
                with contextlib.suppress(Exception):
                    db.set_withdrawal_state(
                        conn, wid, db.WITHDRAWAL_FAILED, error=f"reclaim failed: {exc}"
                    )
    finally:
        conn.close()
    return completed_receipts


class ReclaimWorker:
    """Daemon worker that keeps reclaiming until every receipt is durable.

    A single worker per process is sufficient (SQLite serialises writers
    anyway); :meth:`wake` makes cleanup prompt while the timed loop guarantees
    progress even if a wake-up was missed or a pass previously failed.
    """

    def __init__(self, poll_interval: float | None = None) -> None:
        self.interval = (
            poll_interval
            if poll_interval is not None
            else config.RECLAIM_POLL_INTERVAL
        )
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        # Reset in case this worker was stopped and restarted (tests create
        # many app instances against the same process-wide singleton).
        self._stop.clear()
        self._wake.set()
        self._thread = threading.Thread(
            target=self._run, name="reclaim-worker", daemon=True
        )
        self._thread.start()

    def wake(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        # Leave the events in a clean state so start() works again afterwards.
        self._stop.clear()
        self._wake.clear()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                process_pending()
            except Exception:  # absolute backstop: the worker must never die
                logger.exception("reclaim worker pass crashed")
            self._wake.wait(self.interval)
            self._wake.clear()


# Process-wide worker, started from the FastAPI lifespan.
worker = ReclaimWorker()
