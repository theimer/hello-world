"""
Browser Visit Logger — Python DB helpers used by the rebuilder.

This module used to be the Chrome native-messaging host
(`read_message → write/log/DB → write_message`), but that role moved
to the Swift `BVLHost` binary inside `BrowserVisitLoggerHost.app`.
What remains here are the schema and insert helpers that
`visits_rebuilder.py` imports to replay log files into a fresh
database; nothing on the production tagging path runs through Python
any more.

Schema
------
    visits (
        url         TEXT PRIMARY KEY,
        timestamp   TEXT NOT NULL,           -- first-visit timestamp, never updated
        title       TEXT NOT NULL DEFAULT '',
        of_interest TEXT,                    -- non-NULL if ever tagged of_interest
        read        INTEGER NOT NULL DEFAULT 0,  -- count of read clicks
        skimmed     INTEGER NOT NULL DEFAULT 0   -- count of skimmed clicks
    )

    read_events (
        url       TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        filename  TEXT NOT NULL DEFAULT '',  -- snapshot basename, e.g. <hash>.mhtml
        directory TEXT NOT NULL DEFAULT '<DOWNLOADS_SNAPSHOTS_DIR>',  -- absolute parent dir
        PRIMARY KEY (url, timestamp)
    )

    skimmed_events (
        url       TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        filename  TEXT NOT NULL DEFAULT '',  -- snapshot basename, e.g. <hash>.mhtml
        directory TEXT NOT NULL DEFAULT '<DOWNLOADS_SNAPSHOTS_DIR>',  -- absolute parent dir
        PRIMARY KEY (url, timestamp)
    )

    snapshots (
        date   TEXT PRIMARY KEY,         -- 'YYYY-MM-DD' (UTC) of host activity
        sealed INTEGER NOT NULL DEFAULT 0
    )
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
# Schema helpers
# ---------------------------------------------------------------------------

def ensure_db(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute('PRAGMA table_info(visits)').fetchall()}

    if not cols:
        conn.execute("""
            CREATE TABLE visits (
                url         TEXT PRIMARY KEY,
                timestamp   TEXT NOT NULL,
                title       TEXT NOT NULL DEFAULT '',
                of_interest TEXT,
                read        INTEGER NOT NULL DEFAULT 0,
                skimmed     INTEGER NOT NULL DEFAULT 0
            )
        """)

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_visits_timestamp ON visits(timestamp)"
    )

    # One row per read/skimmed event (individual timestamps).
    _ensure_events_table(conn, 'read_events')
    _ensure_events_table(conn, 'skimmed_events')

    # snapshots table: one row per UTC day with host activity.  The
    # production Swift host inserts (date, sealed=0) on every write;
    # the verifier flips to sealed=1 once it has moved the day's log
    # and written MANIFEST.tsv.  Owned here (rather than only by
    # snapshot_mover) so a brand-new install with no Swift run yet
    # still has the table present.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS snapshots (
            date   TEXT PRIMARY KEY,
            sealed INTEGER NOT NULL DEFAULT 0
        )
    """)

    conn.commit()


def _ensure_events_table(conn: sqlite3.Connection, table: str) -> None:
    """Create an events table (url, timestamp PRIMARY KEY, filename, directory).

    table is a trusted internal constant, never user-supplied.

    The DEFAULT for directory embeds DOWNLOADS_SNAPSHOTS_DIR at table-creation
    time so that ad-hoc INSERTs (e.g. via sqlite3 CLI) get a sensible value;
    _insert_event always specifies the directory explicitly.  Single-quotes in
    the path are escaped to prevent SQL syntax errors on unusual home paths.
    """
    default_dir_lit = "'" + DOWNLOADS_SNAPSHOTS_DIR.replace("'", "''") + "'"
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            url       TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            filename  TEXT NOT NULL DEFAULT '',
            directory TEXT NOT NULL DEFAULT {default_dir_lit},
            PRIMARY KEY (url, timestamp)
        )
    """)


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
