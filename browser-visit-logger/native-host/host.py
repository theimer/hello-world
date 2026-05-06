"""
Browser Visit Logger — Python DB helpers used by the rebuilder.

This module used to be the Chrome native-messaging host
(`read_message → write/log/DB → write_message`), but that role moved
to the Swift `BVLHost` binary inside `BrowserVisitLoggerHost.app`.
What remains here are the schema and insert helpers that
`visits_rebuilder.py` imports to replay log files into a fresh
database; nothing on the production tagging path runs through Python
any more.

The DDL itself lives in ../schema.sql (single source of truth).
Swift's swift/Sources/BVLCore/Schema.swift duplicates it in compiled
code; tests/test_schema_parity.py asserts they stay in sync.
"""

import os
import sqlite3


# ---------------------------------------------------------------------------
# Paths — default to the user's home directory; overrideable via env vars
# (env var overrides are used by the test suite to isolate test output)
# ---------------------------------------------------------------------------
HOME     = os.path.expanduser('~')
# Per-day visit logs live under LOG_DIR as `browser-visits-<UTC-date>.log`.
LOG_DIR  = os.environ.get('BVL_LOG_DIR',  HOME)
DB_FILE  = os.environ.get('BVL_DB_FILE',  os.path.join(HOME, 'browser-visits.db'))

# Snapshot storage locations.  These are baked into the events tables'
# column defaults at CREATE TABLE time (so ad-hoc INSERTs without a
# directory get a sensible value); the production Swift code records
# the actual on-disk locations explicitly per row.
DOWNLOADS_SNAPSHOTS_DIR = os.environ.get(
    'BVL_DOWNLOADS_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Downloads', 'browser-visit-snapshots'),
)
ICLOUD_SNAPSHOTS_DIR = os.environ.get(
    'BVL_ICLOUD_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Documents', 'browser-visit-logger', 'snapshots'),
)


# ---------------------------------------------------------------------------
# Schema helpers — DDL is loaded from schema.sql (single source of truth).
# The mover_errors table is also defined there but is created lazily by
# snapshot_mover._ensure_mover_errors_table on first error record.
# ---------------------------------------------------------------------------

# schema.sql lives at the repo root next to native-host/.
_SCHEMA_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schema.sql')
_DOWNLOADS_DIR_SENTINEL = '__BVL_DOWNLOADS_SNAPSHOTS_DIR__'


def _load_schema_sql() -> str:
    """Read schema.sql and substitute the downloads-dir sentinel.

    Single quotes in the path are escaped (SQL string literal escaping)
    so unusual home paths don't break the DDL.
    """
    with open(_SCHEMA_PATH, encoding='utf-8') as f:
        sql = f.read()
    escaped = DOWNLOADS_SNAPSHOTS_DIR.replace("'", "''")
    return sql.replace(_DOWNLOADS_DIR_SENTINEL, escaped)


def ensure_db(conn: sqlite3.Connection) -> None:
    """Create / migrate every table the rebuilder may write to.

    The schema file uses CREATE TABLE IF NOT EXISTS for everything, so
    this is safe to run on fresh and existing databases alike.
    """
    conn.executescript(_load_schema_sql())
    conn.commit()


# ---------------------------------------------------------------------------
# Insert / update helpers — used by visits_rebuilder.py during log replay
# ---------------------------------------------------------------------------

def _insert_event(
    conn: sqlite3.Connection, table: str, url: str, timestamp: str,
    visits_col: str, filename: str = '',
) -> bool:
    """Insert a timestamped event for url into table if the URL exists in visits,
    and increment the corresponding counter column in visits by 1.

    The filename is normalized to its basename before being stored (the log
    captures Chrome's relative path under Downloads, e.g. 'browser-visit-
    snapshots/<hash>.mhtml'; the parent directory is captured separately in
    the directory column so the basename alone is sufficient).

    Returns True if the URL exists (event inserted or duplicate ignored),
    False if no visits record exists for the URL.

    table and visits_col are trusted internal constants, never user-supplied.
    """
    exists = conn.execute("SELECT 1 FROM visits WHERE url = ?", (url,)).fetchone()
    if exists:
        basename = os.path.basename(filename)
        cursor = conn.execute(
            f"INSERT OR IGNORE INTO {table} (url, timestamp, filename, directory) "
            f"VALUES (?, ?, ?, ?)",
            (url, timestamp, basename, DOWNLOADS_SNAPSHOTS_DIR),
        )
        if cursor.rowcount > 0:
            conn.execute(
                f"UPDATE visits SET {visits_col} = {visits_col} + 1 WHERE url = ?",
                (url,),
            )
    conn.commit()
    return exists is not None


def insert_visit(conn: sqlite3.Connection, timestamp: str, url: str, title: str) -> None:
    """Insert a new visit row; silently ignored if the URL already exists."""
    conn.execute(
        "INSERT OR IGNORE INTO visits (url, timestamp, title) VALUES (?, ?, ?)",
        (url, timestamp, title),
    )
    conn.commit()


def tag_visit(
    conn: sqlite3.Connection, url: str, tag: str, tag_timestamp: str, filename: str = '',
) -> bool:
    """Set the of_interest, read, or skimmed timestamp on the visit record for url.

    For 'read' and 'skimmed' tags, filename is the snapshot filename as the
    log records it (relative path under ~/Downloads, e.g.
    'browser-visit-snapshots/<hash>.mhtml').  _insert_event normalizes it to
    its basename before storage.

    Returns True if a row was found and updated, False if no record exists for url.
    """
    if tag == 'of_interest':
        cursor = conn.execute(
            "UPDATE visits SET of_interest = 1 WHERE url = ?",
            (url,),
        )
    elif tag in ('read', 'skimmed'):
        table = 'read_events' if tag == 'read' else 'skimmed_events'
        return _insert_event(conn, table, url, tag_timestamp, tag, filename)
    else:
        return False
    conn.commit()
    return cursor.rowcount > 0
