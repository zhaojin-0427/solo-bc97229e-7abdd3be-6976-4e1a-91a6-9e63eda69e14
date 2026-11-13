"""FastAPI application: streaming upload, versioned search, replacement workflow.

Concurrency model (single uvicorn worker)
-----------------------------------------
* An in-process lock per document enforces "at most one replacement at a time";
  the catalog table enforces the same rule inside SQLite as a backstop.
* Ingestion runs in a worker thread, fed through a bounded queue, so an
  arbitrarily large upload never resides in memory and never blocks the loop.
* Indexing happens after the upload response (202) is returned; the previous
  active version stays fully searchable until the atomic catalog switch.
* Search opens a short-lived read-only snapshot of the active version.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import queue
from collections.abc import Iterator
from contextlib import asynccontextmanager

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from starlette.concurrency import run_in_threadpool
from starlette.responses import JSONResponse

from . import config, db, ingest, reclaim, search
from .schemas import (
    DocumentInfo,
    Health,
    RecoveryReport,
    ReplacementAccepted,
    SearchResponse,
    VersionInfo,
    WithdrawalReceipt,
)

logger = logging.getLogger("archive")

DOC_ID_MAX_LEN = 200
# Bounded in-memory backlog of raw chunks for the ingestion thread.
CHUNK_QUEUE_MAX = 32


def _validate_doc_id(doc_id: str) -> str:
    if not doc_id or len(doc_id) > DOC_ID_MAX_LEN:
        raise HTTPException(status_code=400, detail="invalid doc_id")
    return doc_id


async def _stream_ingest(
    conn, vid: int, chunk_iter: Iterator[bytes]
) -> int:
    """Bridge an async byte stream to the synchronous ingestion routine."""
    q: queue.Queue = queue.Queue(maxsize=CHUNK_QUEUE_MAX)
    sentinel = object()
    producer_error: list[BaseException] = []
    loop = asyncio.get_running_loop()

    async def produce() -> None:
        try:
            async for chunk in chunk_iter:
                if chunk:
                    await loop.run_in_executor(None, q.put, chunk)
        except BaseException as exc:  # surfaced through the consumer
            producer_error.append(exc)
        finally:
            # Always unblock the consumer, even on client disconnect.
            q.put(sentinel)

    producer = asyncio.create_task(produce())

    def consume() -> int:
        def gen() -> Iterator[bytes]:
            while True:
                item = q.get()
                if item is sentinel:
                    return
                yield item

        return ingest.ingest_version(conn, vid, gen())

    try:
        result = await run_in_threadpool(consume)
    finally:
        if not producer.done():
            producer.cancel()
            with contextlib.suppress(Exception):
                await producer
    if producer_error and not isinstance(
        producer_error[0], asyncio.CancelledError
    ):
        raise producer_error[0]
    return result


def _index_in_background(doc_id: str, vid: int, block_count: int) -> None:
    """Promote a fully ingested version; any failure keeps the old version."""
    # Reclamation considers the whole stale execution (ingest + promotion);
    # the guard is released when this function returns for any reason.
    with reclaim.job_guard(vid):
        conn = db.get_conn()
        try:
            # Epoch check first: a withdrawal may have happened while the upload
            # was streaming, in which case the version is already `withdrawn` and
            # we must neither change its state nor touch the catalog.
            try:
                doc = db.get_document(conn, doc_id)
                ver = db.get_version(conn, vid)
                if (
                    doc is None
                    or ver is None
                    or ver["state"] == db.STATE_WITHDRAWN
                    or int(ver["epoch"]) != int(doc["epoch"])
                ):
                    logger.info(
                        "version %s of %r belongs to a withdrawn epoch; "
                        "skipping activation",
                        vid,
                        doc_id,
                    )
                    return
            except db.StaleVersionError:  # pragma: no cover - defensive
                return

            db.set_state(conn, vid, db.STATE_INDEXING)
            # Indexes were built incrementally during ingestion; the tables are
            # already durable. This step is the single atomic visibility switch.
            try:
                db.activate_version(conn, doc_id, vid, block_count)
            except db.StaleVersionError:
                # Withdrawal raced with this promotion; the reclaimer owns the
                # tables from here on.
                logger.info(
                    "version %s of %r withdrawn during promotion; skipping activation",
                    vid,
                    doc_id,
                )
                return
            logger.info("document %r switched to version %s", doc_id, vid)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("background indexing failed for version %s", vid)
            with contextlib.suppress(Exception):
                db.fail_version(conn, vid, f"indexing failed: {exc}")
        finally:
            conn.close()


def create_app() -> FastAPI:
    config.ensure_dirs()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        conn = db.get_conn()
        try:
            db.init_schema(conn)
            recovery_messages.extend(db.startup_recovery(conn))
            for msg in recovery_messages:
                logger.warning("recovery: %s", msg)
        finally:
            conn.close()
        # Resume any withdrawal whose reclamation was interrupted by a restart.
        reclaim.worker.start()
        reclaim.worker.wake()
        yield
        reclaim.worker.stop()

    app = FastAPI(
        title="Archive OCR cross-page search API",
        version="1.0.0",
        lifespan=lifespan,
        description=(
            "Upload OCR block streams (UTF-8 NDJSON) per document and search "
            "the whitespace-insensitive text, including matches that cross "
            "blocks and scan pages. Re-uploading a document builds a new "
            "version that replaces the old one atomically."
        ),
    )

    locks: dict[str, asyncio.Lock] = {}
    recovery_messages: list[str] = []

    def lock_for(doc_id: str) -> asyncio.Lock:
        lock = locks.get(doc_id)
        if lock is None:
            lock = asyncio.Lock()
            locks[doc_id] = lock
        return lock

    @app.get("/health", response_model=Health, tags=["meta"])
    def health() -> Health:
        return Health(status="ok", db_path=str(config.DB_PATH))

    @app.get("/recovery", response_model=RecoveryReport, tags=["meta"])
    def recovery() -> RecoveryReport:
        return RecoveryReport(recovered=list(recovery_messages))

    @app.get("/documents", response_model=list[DocumentInfo], tags=["documents"])
    def list_documents() -> list[DocumentInfo]:
        conn = db.get_conn()
        try:
            return [DocumentInfo(**dict(row)) for row in db.list_documents(conn)]
        finally:
            conn.close()

    @app.get(
        "/documents/{doc_id}/versions",
        response_model=list[VersionInfo],
        tags=["documents"],
    )
    def list_versions(doc_id: str) -> list[VersionInfo]:
        _validate_doc_id(doc_id)
        conn = db.get_conn()
        try:
            rows = conn.execute(
                "SELECT version_id, doc_id, state, created_at, finalized_at, "
                "block_count, error FROM versions WHERE doc_id = ? "
                "ORDER BY version_id",
                (doc_id,),
            ).fetchall()
            return [VersionInfo(**dict(row)) for row in rows]
        finally:
            conn.close()

    @app.put(
        "/documents/{doc_id}",
        response_model=ReplacementAccepted,
        status_code=202,
        tags=["documents"],
        responses={
            409: {"description": "a replacement for this document is already running"},
            400: {"description": "malformed NDJSON upload"},
            507: {"description": "storage write failure"},
        },
    )
    async def replace_document(
        doc_id: str, request: Request, background_tasks: BackgroundTasks
    ) -> ReplacementAccepted:
        """Replace (or create) a document with an NDJSON block stream.

        Body: one JSON object per line, each with unique ``block_id``,
        ``page``, ``text`` and ``rect`` (``x0/y0/x1/y1``). Line order is the
        reading order. The old version stays searchable while the new one is
        being prepared.
        """
        _validate_doc_id(doc_id)
        lock = lock_for(doc_id)
        if lock.locked():
            raise HTTPException(
                status_code=409,
                detail="a replacement for this document is already in progress",
            )

        async with lock:
            conn = db.get_conn()
            try:
                try:
                    vid = db.create_version(conn, doc_id)
                except RuntimeError as exc:
                    raise HTTPException(status_code=409, detail=str(exc)) from exc

                # The reclaimer waits on this guard if the document is
                # withdrawn mid-upload: the old job's tables are only dropped
                # once ingestion has tailed out.
                with reclaim.job_guard(vid):
                    try:
                        block_count = await _stream_ingest(
                            conn, vid, request.stream()
                        )
                    except ingest.IngestError as exc:
                        db.fail_version(conn, vid, str(exc))
                        raise HTTPException(
                            status_code=400, detail=str(exc)
                        ) from exc
                    except OSError as exc:
                        db.fail_version(conn, vid, str(exc))
                        raise HTTPException(
                            status_code=507, detail=str(exc)
                        ) from exc
            finally:
                conn.close()

        background_tasks.add_task(_index_in_background, doc_id, vid, block_count)
        return ReplacementAccepted(
            doc_id=doc_id,
            version_id=vid,
            state=db.STATE_INDEXING,
            detail="upload accepted; new version is being prepared",
        )

    @app.get(
        "/documents/{doc_id}/search",
        response_model=SearchResponse,
        tags=["search"],
        responses={404: {"description": "document not found or no active version"}},
    )
    def search_document(doc_id: str, q: str) -> SearchResponse:
        """Search one document.

        Whitespace in ``q`` and in the stored text is ignored. Matches may span
        multiple blocks and pages; overlapping matches are returned separately.
        At most the earliest 100 matches are returned.
        """
        _validate_doc_id(doc_id)
        compact_q = "".join(q.split())
        if not (config.QUERY_MIN_CHARS <= len(compact_q) <= config.QUERY_MAX_CHARS):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"query must contain {config.QUERY_MIN_CHARS}-"
                    f"{config.QUERY_MAX_CHARS} non-whitespace characters"
                ),
            )
        conn = db.get_conn()
        try:
            vid = db.get_active_version(conn, doc_id)
            if vid is None:
                raise HTTPException(
                    status_code=404,
                    detail="document not found or no searchable version yet",
                )
            # Hold the read for the whole query: a concurrent withdrawal may
            # hide the document, but it cannot drop these tables until we finish.
            with reclaim.read_guard(vid):
                try:
                    result = search.search(conn, vid, q)
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            return SearchResponse(**result)
        finally:
            conn.close()

    def _build_receipt(conn, withdrawal_id: str, replayed: bool) -> WithdrawalReceipt:
        row = db.get_withdrawal(conn, withdrawal_id)
        if row is None:
            raise HTTPException(
                status_code=404, detail=f"unknown withdrawal_id {withdrawal_id!r}"
            )
        items = [
            {
                "version_id": int(item["version_id"]),
                "state": item["state"],
                "updated_at": item["updated_at"],
            }
            for item in db.list_withdrawal_items(conn, withdrawal_id)
        ]
        return WithdrawalReceipt(
            withdrawal_id=row["withdrawal_id"],
            doc_id=row["doc_id"],
            epoch=int(row["epoch"]),
            state=row["state"],
            replayed=replayed,
            detail=row["detail"],
            error=row["error"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            completed_at=row["completed_at"],
            items=items,
        )

    @app.post(
        "/documents/{doc_id}/withdrawals",
        response_model=WithdrawalReceipt,
        status_code=202,
        tags=["withdrawals"],
        responses={
            404: {"description": "document not found"},
            409: {"description": "withdrawal number already used by another document"},
            400: {"description": "invalid withdrawal payload"},
        },
    )
    async def withdraw_document(
        doc_id: str, request: Request, background_tasks: BackgroundTasks
    ):
        """Withdraw (撤档) a document: take its OCR content offline.

        Body: ``{"withdrawal_id": "<client-chosen unique 撤档号>"}``.

        The document immediately disappears from ``GET /documents`` and fresh
        searches return 404, while searches already running against the
        previously selected version finish untouched. Re-submitting the same
        withdrawal number returns the *first* receipt (idempotent). The same
        ``doc_id`` can afterwards be re-uploaded; old upload jobs and old
        receipts can never affect the new archive.
        """
        _validate_doc_id(doc_id)
        try:
            payload = await request.json()
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HTTPException(
                status_code=400, detail=f"invalid JSON body: {exc}"
            ) from exc
        if not isinstance(payload, dict) or not isinstance(
            payload.get("withdrawal_id"), str
        ):
            raise HTTPException(
                status_code=400,
                detail="body must be a JSON object with a string 'withdrawal_id'",
            )
        withdrawal_id = payload["withdrawal_id"]
        if not withdrawal_id or len(withdrawal_id) > config.WITHDRAWAL_ID_MAX_LEN:
            raise HTTPException(
                status_code=400, detail="invalid withdrawal_id"
            )

        conn = db.get_conn()
        try:
            existing = db.get_withdrawal(conn, withdrawal_id)
            if existing is not None:
                # Idempotency: the same withdrawal number always yields the
                # first receipt, no matter how often it is retried.
                if existing["doc_id"] != doc_id:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            f"withdrawal_id {withdrawal_id!r} already belongs to "
                            f"document {existing['doc_id']!r}"
                        ),
                    )
                receipt = _build_receipt(conn, withdrawal_id, replayed=True)
                # A replay also nudges the worker in case cleanup had stalled.
                background_tasks.add_task(reclaim.worker.wake)
                return JSONResponse(
                    receipt.dict(), status_code=200,
                    headers={"content-type": "application/json; charset=utf-8"},
                )

            doc = db.get_document(conn, doc_id)
            if doc is None:
                raise HTTPException(
                    status_code=404, detail=f"document {doc_id!r} not found"
                )
            if int(doc["is_withdrawn"]):
                raise HTTPException(
                    status_code=404,
                    detail=(
                        f"document {doc_id!r} is already withdrawn; re-upload it "
                        "before withdrawing again"
                    ),
                )
            try:
                db.withdraw_document(conn, doc_id, withdrawal_id)
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            receipt = _build_receipt(conn, withdrawal_id, replayed=False)
        finally:
            conn.close()

        background_tasks.add_task(reclaim.process_pending)
        background_tasks.add_task(reclaim.worker.wake)
        return receipt

    @app.get(
        "/withdrawals/{withdrawal_id}",
        response_model=WithdrawalReceipt,
        tags=["withdrawals"],
        responses={404: {"description": "unknown withdrawal number"}},
    )
    def get_withdrawal(withdrawal_id: str) -> WithdrawalReceipt:
        """Look up a withdrawal receipt: reclaim state, scope items and errors."""
        if not withdrawal_id or len(withdrawal_id) > config.WITHDRAWAL_ID_MAX_LEN:
            raise HTTPException(status_code=400, detail="invalid withdrawal_id")
        conn = db.get_conn()
        try:
            return _build_receipt(conn, withdrawal_id, replayed=False)
        except HTTPException:
            raise
        finally:
            conn.close()

    return app


app = create_app()
