"""SQLite persistence, version state machine and crash recovery.

Layout
------
Two catalog tables hold document/version metadata. Every version of a document
gets its own set of tables (``blocks_<vid>``, ``unigrams_<vid>``,
``bigrams_<vid>``) so that a replacement can be streamed, indexed and only
switched in once complete, and dropping an obsolete version never produces a
giant WAL inside a shared table.

A version moves through the states

    INGESTING -> INDEXING -> READY
                   \\------> FAILED

Only ``READY`` versions are searchable. Crash recovery on startup turns every
non-terminal version into ``FAILED`` and removes its staging tables, so an
interrupted replacement never masquerades as a usable index.
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from typing import Iterator, Optional

from . import config

STATE_INGESTING = "ingesting"
STATE_INDEXING = "indexing"
STATE_READY = "ready"
STATE_FAILED = "failed"
# A version whose document was withdrawn. Terminal: it must never activate and
# its tables are reclaimed by the withdrawal workflow (see app/reclaim.py).
STATE_WITHDRAWN = "withdrawn"
TERMINAL_STATES = (STATE_READY, STATE_FAILED, STATE_WITHDRAWN)

# Withdrawal receipt lifecycle.
WITHDRAWAL_ACCEPTED = "accepted"
WITHDRAWAL_RECLAIMING = "reclaiming"
WITHDRAWAL_COMPLETED = "completed"
WITHDRAWAL_FAILED = "failed"
WITHDRAWAL_TERMINAL = (WITHDRAWAL_COMPLETED, WITHDRAWAL_FAILED)

# Reclaim item lifecycle (one row per version whose tables must be dropped).
ITEM_PENDING = "pending"
ITEM_DONE = "done"


class StaleVersionError(RuntimeError):
    """Raised when a background job touches a version superseded by a newer epoch.

    Typical case: an upload was in flight when its document was withdrawn and
    re-archived under the same id; the old upload's tail must not resurrect
    anything belonging to the new archive.
    """


def get_conn(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Open a connection with sane durability/concurrency settings."""
    path = db_path or str(config.DB_PATH)
    # Connections are request-private but may be driven from a worker thread
    # (streaming ingestion runs in a threadpool), so disable the thread check.
    conn = sqlite3.connect(path, timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            doc_id            TEXT PRIMARY KEY,
            active_version_id INTEGER,
            created_at        REAL NOT NULL,
            updated_at        REAL NOT NULL
        );
        CREATE TABLE IF NOT EXISTS versions (
            version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id       TEXT NOT NULL REFERENCES documents(doc_id),
            state        TEXT NOT NULL,
            created_at   REAL NOT NULL,
            finalized_at REAL,
            error        TEXT,
            block_count  INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_versions_doc ON versions(doc_id, state);

        /* Withdrawal receipts: the durable unit of the withdraw workflow.
           `epoch` captures the document generation that was withdrawn, so a
           later re-archive with the same doc_id can never be confused with it. */
        CREATE TABLE IF NOT EXISTS withdrawals (
            withdrawal_id TEXT PRIMARY KEY,
            doc_id        TEXT NOT NULL,
            epoch         INTEGER NOT NULL,
            state         TEXT NOT NULL,
            replayed      INTEGER NOT NULL DEFAULT 0,
            detail        TEXT,
            error         TEXT,
            created_at    REAL NOT NULL,
            updated_at    REAL NOT NULL,
            completed_at  REAL
        );
        /* Reclaim scope: one row per pre-withdrawal version. A row only becomes
           `done` once that version's tables have actually been dropped, which
           makes cleanup resumable after failures and process restarts. */
        CREATE TABLE IF NOT EXISTS withdrawal_items (
            withdrawal_id TEXT NOT NULL REFERENCES withdrawals(withdrawal_id),
            version_id    INTEGER NOT NULL,
            state         TEXT NOT NULL,
            updated_at    REAL NOT NULL,
            PRIMARY KEY (withdrawal_id, version_id)
        );
        CREATE INDEX IF NOT EXISTS idx_witems_state
            ON withdrawal_items(state);
        CREATE INDEX IF NOT EXISTS idx_withdrawals_state
            ON withdrawals(state);
        """
    )
    # Lightweight migrations for catalogs created before withdrawals existed.
    doc_cols = _column_names(conn, "documents")
    if "is_withdrawn" not in doc_cols:
        conn.execute(
            "ALTER TABLE documents ADD COLUMN is_withdrawn INTEGER NOT NULL DEFAULT 0"
        )
    if "epoch" not in doc_cols:
        conn.execute("ALTER TABLE documents ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0")
    ver_cols = _column_names(conn, "versions")
    if "epoch" not in ver_cols:
        conn.execute("ALTER TABLE versions ADD COLUMN epoch INTEGER NOT NULL DEFAULT 0")
    conn.commit()


def _version_tables(vid: int) -> tuple[str, str, str]:
    return f"blocks_{vid}", f"unigrams_{vid}", f"bigrams_{vid}"


def create_version_tables(conn: sqlite3.Connection, vid: int) -> None:
    blocks, unigrams, bigrams = _version_tables(vid)
    conn.executescript(
        f"""
        CREATE TABLE {blocks} (
            block_order INTEGER PRIMARY KEY,
            block_id    TEXT NOT NULL UNIQUE,
            page        INTEGER NOT NULL,
            text        TEXT NOT NULL,
            x0          REAL,
            y0          REAL,
            x1          REAL,
            y1          REAL,
            start_pos   INTEGER NOT NULL,
            end_pos     INTEGER NOT NULL,
            nws_len     INTEGER NOT NULL
        );
        CREATE INDEX idx_{blocks}_start ON {blocks}(start_pos);
        CREATE TABLE {unigrams} (
            ch  TEXT NOT NULL,
            pos INTEGER NOT NULL,
            PRIMARY KEY (ch, pos)
        ) WITHOUT ROWID;
        CREATE TABLE {bigrams} (
            bg  TEXT NOT NULL,
            pos INTEGER NOT NULL,
            PRIMARY KEY (bg, pos)
        ) WITHOUT ROWID;
        """
    )


def drop_version_tables(conn: sqlite3.Connection, vid: int) -> None:
    blocks, unigrams, bigrams = _version_tables(vid)
    conn.execute(f"DROP TABLE IF EXISTS {bigrams}")
    conn.execute(f"DROP TABLE IF EXISTS {unigrams}")
    conn.execute(f"DROP TABLE IF EXISTS {blocks}")


def has_pending_version(conn: sqlite3.Connection, doc_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM versions WHERE doc_id = ? AND state NOT IN (?, ?, ?) LIMIT 1",
        (doc_id, STATE_READY, STATE_FAILED, STATE_WITHDRAWN),
    ).fetchone()
    return row is not None


def create_version(conn: sqlite3.Connection, doc_id: str) -> int:
    """Register a new replacement version and its staging tables.

    Raises ``RuntimeError`` when another replacement for the same document is
    already in progress (enforced in the database, not only in process locks).
    The version inherits the document's current epoch; an epoch mismatch later
    proves the version belongs to a superseded (withdrawn) generation.
    """
    now = time.time()
    if has_pending_version(conn, doc_id):
        raise RuntimeError(f"document {doc_id!r} already has a replacement in progress")
    # A re-upload after withdrawal re-archives the id: the document becomes
    # visible again while keeping the bumped epoch (which protects the new
    # generation from the old upload's stale activation).
    conn.execute(
        "INSERT INTO documents(doc_id, created_at, updated_at, is_withdrawn, "
        "active_version_id) VALUES(?, ?, ?, 0, NULL) "
        "ON CONFLICT(doc_id) DO UPDATE SET updated_at = excluded.updated_at, "
        "is_withdrawn = 0",
        (doc_id, now, now),
    )
    epoch = int(
        conn.execute(
            "SELECT epoch FROM documents WHERE doc_id = ?", (doc_id,)
        ).fetchone()["epoch"]
    )
    cur = conn.execute(
        "INSERT INTO versions(doc_id, state, created_at, epoch) "
        "VALUES(?, ?, ?, ?)",
        (doc_id, STATE_INGESTING, now, epoch),
    )
    vid = int(cur.lastrowid)
    create_version_tables(conn, vid)
    conn.commit()
    return vid


def set_state(
    conn: sqlite3.Connection,
    vid: int,
    state: str,
    error: Optional[str] = None,
    block_count: Optional[int] = None,
) -> None:
    if block_count is None:
        conn.execute(
            "UPDATE versions SET state = ?, error = COALESCE(?, error) WHERE version_id = ?",
            (state, error, vid),
        )
    else:
        conn.execute(
            "UPDATE versions SET state = ?, error = COALESCE(?, error), "
            "block_count = ?, finalized_at = ? WHERE version_id = ?",
            (state, error, block_count, time.time(), vid),
        )
    conn.commit()


def fail_version(conn: sqlite3.Connection, vid: int, error: str) -> None:
    """Mark a version failed and delete its staging data in one transaction."""
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE versions SET state = ?, error = ?, finalized_at = ? WHERE version_id = ?",
            (STATE_FAILED, error[:2000], time.time(), vid),
        )
        drop_version_tables(conn, vid)
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def activate_version(
    conn: sqlite3.Connection, doc_id: str, vid: int, block_count: int
) -> None:
    """Atomically point the document at the newly built version.

    The catalog flip happens in one transaction; the old version's tables are
    dropped afterwards in a second transaction so readers always see either the
    complete old version or the complete new one.

    Raises :class:`StaleVersionError` when ``vid`` belongs to an older document
    epoch (the document was withdrawn while this upload was running, and may
    already have a fresh archive under the same id). The stale version is left
    untouched: withdrawal reclamation owns its tables.
    """
    doc = conn.execute(
        "SELECT active_version_id, epoch FROM documents WHERE doc_id = ?",
        (doc_id,),
    ).fetchone()
    if doc is None:
        raise StaleVersionError(
            f"document {doc_id!r} no longer exists when activating version {vid}"
        )
    ver = conn.execute(
        "SELECT epoch, state FROM versions WHERE version_id = ?", (vid,)
    ).fetchone()
    if ver is None:
        raise StaleVersionError(f"version {vid} vanished before activation")
    if int(ver["epoch"]) != int(doc["epoch"]) or ver["state"] == STATE_WITHDRAWN:
        raise StaleVersionError(
            f"version {vid} belongs to withdrawn epoch {int(ver['epoch'])}; "
            f"current epoch of {doc_id!r} is {int(doc['epoch'])}"
        )
    old_vid = int(doc["active_version_id"]) if doc["active_version_id"] else None
    with transaction(conn):
        conn.execute(
            "UPDATE versions SET state = ?, finalized_at = ?, block_count = ? "
            "WHERE version_id = ?",
            (STATE_READY, time.time(), block_count, vid),
        )
        conn.execute(
            "UPDATE documents SET active_version_id = ?, updated_at = ? WHERE doc_id = ?",
            (vid, time.time(), doc_id),
        )
    if old_vid is not None and old_vid != vid:
        with transaction(conn):
            drop_version_tables(conn, old_vid)


def get_active_version(conn: sqlite3.Connection, doc_id: str) -> Optional[int]:
    row = conn.execute(
        "SELECT active_version_id FROM documents WHERE doc_id = ?", (doc_id,)
    ).fetchone()
    if row is None or row["active_version_id"] is None:
        return None
    return int(row["active_version_id"])


def get_version(conn: sqlite3.Connection, vid: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM versions WHERE version_id = ?", (vid,)
    ).fetchone()


def list_documents(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    # Withdrawn documents disappear from the catalog until re-archived under the
    # same id; a fresh upload clears is_withdrawn again.
    return list(
        conn.execute(
            "SELECT d.doc_id, d.active_version_id, d.updated_at, "
            "v.block_count, v.state AS active_state "
            "FROM documents d LEFT JOIN versions v ON v.version_id = d.active_version_id "
            "WHERE d.is_withdrawn = 0 ORDER BY d.doc_id"
        )
    )


def get_document(conn: sqlite3.Connection, doc_id: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM documents WHERE doc_id = ?", (doc_id,)
    ).fetchone()


def get_withdrawal(
    conn: sqlite3.Connection, withdrawal_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM withdrawals WHERE withdrawal_id = ?", (withdrawal_id,)
    ).fetchone()


def list_withdrawal_items(
    conn: sqlite3.Connection, withdrawal_id: str
) -> list[sqlite3.Row]:
    return list(
        conn.execute(
            "SELECT * FROM withdrawal_items WHERE withdrawal_id = ? "
            "ORDER BY version_id",
            (withdrawal_id,),
        )
    )


def withdraw_document(
    conn: sqlite3.Connection, doc_id: str, withdrawal_id: str
) -> list[int]:
    """Execute the withdrawal catalog switch in a single durable transaction.

    The document is hidden and bumped to a new epoch; every version of the
    withdrawn generation is marked ``withdrawn`` and registered as a reclaim
    item (the durable scope of tables to drop). Returns the registered version
    ids. Raises :class:`RuntimeError` if the document does not exist or was
    already withdrawn.
    """
    doc = get_document(conn, doc_id)
    if doc is None:
        raise RuntimeError(f"document {doc_id!r} not found")
    if int(doc["is_withdrawn"]):
        raise RuntimeError(f"document {doc_id!r} is already withdrawn")
    old_epoch = int(doc["epoch"])
    versions = conn.execute(
        "SELECT version_id FROM versions WHERE doc_id = ? AND epoch = ?",
        (doc_id, old_epoch),
    ).fetchall()
    version_ids = [int(r["version_id"]) for r in versions]
    now = time.time()
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE documents SET is_withdrawn = 1, epoch = epoch + 1, "
            "active_version_id = NULL, updated_at = ? WHERE doc_id = ?",
            (now, doc_id),
        )
        conn.executemany(
            "UPDATE versions SET state = ?, finalized_at = ? WHERE version_id = ?",
            [(STATE_WITHDRAWN, now, vid) for vid in version_ids],
        )
        conn.execute(
            "INSERT INTO withdrawals(withdrawal_id, doc_id, epoch, state, "
            "detail, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                withdrawal_id,
                doc_id,
                old_epoch,
                WITHDRAWAL_ACCEPTED,
                "withdrawal accepted; reclaim scheduled",
                now,
                now,
            ),
        )
        conn.executemany(
            "INSERT INTO withdrawal_items(withdrawal_id, version_id, state, "
            "updated_at) VALUES (?, ?, ?, ?)",
            [
                (withdrawal_id, vid, ITEM_PENDING, now)
                for vid in version_ids
            ],
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return version_ids


def list_due_withdrawals(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """Receipts that still have reclaim work outstanding.

    ``failed`` receipts are included on purpose: an error is retried
    automatically on the next pass as long as pending items remain. A receipt
    only reaches ``completed`` once every scoped table has been dropped.
    """
    return list(
        conn.execute(
            "SELECT w.* FROM withdrawals w WHERE w.state <> ? "
            "AND EXISTS (SELECT 1 FROM withdrawal_items i "
            "WHERE i.withdrawal_id = w.withdrawal_id AND i.state = ?) "
            "ORDER BY w.created_at",
            (WITHDRAWAL_COMPLETED, ITEM_PENDING),
        )
    )


def get_pending_item(
    conn: sqlite3.Connection, withdrawal_id: str
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM withdrawal_items WHERE withdrawal_id = ? AND state = ? "
        "ORDER BY version_id LIMIT 1",
        (withdrawal_id, ITEM_PENDING),
    ).fetchone()


def mark_item_done(
    conn: sqlite3.Connection, withdrawal_id: str, vid: int
) -> None:
    with transaction(conn):
        conn.execute(
            "UPDATE withdrawal_items SET state = ?, updated_at = ? "
            "WHERE withdrawal_id = ? AND version_id = ?",
            (ITEM_DONE, time.time(), withdrawal_id, vid),
        )


def set_withdrawal_state(
    conn: sqlite3.Connection,
    withdrawal_id: str,
    state: str,
    error: Optional[str] = None,
    completed: bool = False,
    clear_error: bool = False,
) -> None:
    """Update receipt state.

    ``error=None`` (default) keeps any previous error text; passing an error
    string records it; ``clear_error=True`` wipes it on a successful retry.
    """
    sets = ["state = ?", "updated_at = ?"]
    params: list = [state, time.time()]
    if clear_error:
        sets.append("error = NULL")
    elif error is not None:
        sets.append("error = ?")
        params.append(error[:2000])
    if completed:
        sets.append("completed_at = ?")
        params.append(time.time())
    params.append(withdrawal_id)
    with transaction(conn):
        conn.execute(
            f"UPDATE withdrawals SET {', '.join(sets)} WHERE withdrawal_id = ?",
            params,
        )


def remaining_item_count(
    conn: sqlite3.Connection, withdrawal_id: str
) -> int:
    row = conn.execute(
        "SELECT COUNT(*) AS c FROM withdrawal_items "
        "WHERE withdrawal_id = ? AND state = ?",
        (withdrawal_id, ITEM_PENDING),
    ).fetchone()
    return int(row["c"])


def startup_recovery(conn: sqlite3.Connection) -> list[str]:
    """Recover from interrupted processes.

    Every version that never reached a terminal state is marked FAILED and its
    staging tables are removed -- unless its document has an in-flight
    withdrawal, in which case it belongs to the withdrawn generation and is
    handed to the reclaim workflow. Withdrawal receipts whose scope rows are
    missing are backfilled so cleanup always resumes to completion. Returns
    human-readable recovery messages.
    """
    messages: list[str] = []
    init_schema(conn)
    stuck = conn.execute(
        "SELECT version_id, doc_id, state, epoch FROM versions "
        "WHERE state NOT IN (?, ?, ?)",
        TERMINAL_STATES,
    ).fetchall()
    for row in stuck:
        vid = int(row["version_id"])
        active_w = conn.execute(
            "SELECT w.withdrawal_id FROM withdrawals w "
            "JOIN documents d ON d.doc_id = w.doc_id AND d.epoch > w.epoch "
            "WHERE w.doc_id = ? AND w.state <> ? "
            "AND w.epoch = ? LIMIT 1",
            (row["doc_id"], WITHDRAWAL_COMPLETED, int(row["epoch"])),
        ).fetchone()
        if active_w is not None:
            # The process died while this upload was still running, after (or
            # during) withdrawal. It must never activate; reclaim owns it.
            with transaction(conn):
                conn.execute(
                    "UPDATE versions SET state = ?, finalized_at = ? "
                    "WHERE version_id = ?",
                    (STATE_WITHDRAWN, time.time(), vid),
                )
                conn.execute(
                    "INSERT OR IGNORE INTO withdrawal_items(withdrawal_id, "
                    "version_id, state, updated_at) VALUES (?, ?, ?, ?)",
                    (active_w["withdrawal_id"], vid, ITEM_PENDING, time.time()),
                )
            messages.append(
                f"version {vid} of document {row['doc_id']!r} folded into "
                f"withdrawal {active_w['withdrawal_id']!r} (was {row['state']!r})"
            )
            continue
        fail_version(conn, vid, "interrupted before completion; marked failed on restart")
        messages.append(
            f"version {vid} of document {row['doc_id']!r} marked FAILED "
            f"(was {row['state']!r})"
        )

    # Backfill reclaim scope for any withdrawn version missing an item row
    # (e.g. the withdrawal transaction itself was interrupted historically).
    open_withdrawals = conn.execute(
        "SELECT withdrawal_id, doc_id, epoch FROM withdrawals "
        "WHERE state <> ?", (WITHDRAWAL_COMPLETED,)
    ).fetchall()
    for w in open_withdrawals:
        wid = w["withdrawal_id"]
        scoped = {
            int(r["version_id"])
            for r in conn.execute(
                "SELECT version_id FROM withdrawal_items WHERE withdrawal_id = ?",
                (wid,),
            ).fetchall()
        }
        withdrawn_vids = [
            int(r["version_id"])
            for r in conn.execute(
                "SELECT version_id FROM versions WHERE doc_id = ? AND epoch = ?",
                (w["doc_id"], int(w["epoch"])),
            ).fetchall()
        ]
        missing = [vid for vid in withdrawn_vids if vid not in scoped]
        if missing:
            conn.executemany(
                "INSERT OR IGNORE INTO withdrawal_items(withdrawal_id, "
                "version_id, state, updated_at) VALUES (?, ?, ?, ?)",
                [(wid, vid, ITEM_PENDING, time.time()) for vid in missing],
            )
            conn.commit()
            messages.append(
                f"withdrawal {wid!r}: reclaimed scope backfilled with "
                f"version(s) {missing}"
            )

    # Garbage-collect tables belonging to failed versions (e.g. tables that
    # survived because fail_version itself was interrupted mid-way).
    existing = {
        name
        for (name,) in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    failed_ids = [
        int(r["version_id"])
        for r in conn.execute(
            "SELECT version_id FROM versions WHERE state = ?", (STATE_FAILED,)
        ).fetchall()
    ]
    for vid in failed_ids:
        blocks, unigrams, bigrams = _version_tables(vid)
        with transaction(conn):
            for name in (bigrams, unigrams, blocks):
                if name in existing:
                    conn.execute(f"DROP TABLE {name}")
    return messages
