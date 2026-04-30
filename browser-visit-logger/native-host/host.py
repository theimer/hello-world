#!/usr/bin/env python3
"""
Native messaging host for Browser Visit Logger.

Receives a single JSON visit record from Chrome via stdin (native messaging
protocol: 4-byte LE length prefix + UTF-8 JSON), writes the record to a TSV
log file and a SQLite database, then sends a JSON response to stdout.

Chrome launches this script once per sendNativeMessage() call (MV3 one-shot
semantics): read one message → write outputs → respond → exit.

Message types
-------------
Auto-log (from background.js):
    { "timestamp": "...", "url": "...", "title": "..." }
    → INSERT OR IGNORE new row (first visit wins); append 3-field TSV line.

Tag action (from popup.js / background.js):
    { "timestamp": "...", "url": "...", "title": "...", "tag": "of_interest"|"read"|"skimmed"
      [, "filename": "browser-visit-snapshots/<hash>.<ext>"] }
    → INSERT OR IGNORE the visit row first (so tagging always works even if the
      auto-log hasn't fired yet), then update of_interest, read, or skimmed;
      append 4-field TSV line.
      For "read" and "skimmed": Chrome saves a snapshot and sends its filename
      (Chrome's relative path under ~/Downloads). This host normalizes the
      filename to its basename and records both the basename and the parent
      directory (initially the Downloads snapshots dir). A separate periodic
      mover (snapshot_mover.py) later copies the file to the iCloud directory
      and updates the directory column in place.

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
"""

import json
import logging
import os
import sqlite3
import struct
import sys
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------------
# Paths — default to the user's home directory; overrideable via env vars
# (env var overrides are used by the test suite to isolate test output)
# ---------------------------------------------------------------------------
HOME     = os.path.expanduser('~')
LOG_FILE = os.environ.get('BVL_LOG_FILE', os.path.join(HOME, 'browser-visits.log'))
DB_FILE  = os.environ.get('BVL_DB_FILE',  os.path.join(HOME, 'browser-visits.db'))
HOST_LOG = os.environ.get('BVL_HOST_LOG', os.path.join(HOME, 'browser-visits-host.log'))

# Snapshot storage locations.  Chrome writes snapshots to the Downloads dir;
# the periodic mover later copies them to the iCloud-synced Documents dir.
DOWNLOADS_SNAPSHOTS_DIR = os.environ.get(
    'BVL_DOWNLOADS_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Downloads', 'browser-visit-snapshots'),
)
ICLOUD_SNAPSHOTS_DIR = os.environ.get(
    'BVL_ICLOUD_SNAPSHOTS_DIR',
    os.path.join(HOME, 'Documents', 'browser-visit-logger', 'snapshots'),
)

# ---------------------------------------------------------------------------
# Host process logging (errors/debug — never written to stderr)
# ---------------------------------------------------------------------------
_handler = RotatingFileHandler(HOST_LOG, maxBytes=1_048_576, backupCount=3)
_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger = logging.getLogger('bvl')
logger.setLevel(logging.DEBUG)
logger.addHandler(_handler)

# ---------------------------------------------------------------------------
# Native messaging I/O
# ---------------------------------------------------------------------------

def read_message() -> dict:
    """Read one native message from stdin (binary mode)."""
    raw_length = sys.stdin.buffer.read(4)
    if len(raw_length) < 4:
        raise EOFError('stdin closed before length header was complete')
    msg_length = struct.unpack('<I', raw_length)[0]
    raw_message = sys.stdin.buffer.read(msg_length)
    if len(raw_message) < msg_length:
        raise EOFError('stdin closed mid-message')
    return json.loads(raw_message.decode('utf-8'))


def write_message(payload: dict) -> None:
    """Write one native message to stdout (binary mode)."""
    encoded = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    sys.stdout.buffer.write(struct.pack('<I', len(encoded)))
    sys.stdout.buffer.write(encoded)
    sys.stdout.buffer.flush()

# ---------------------------------------------------------------------------
# SQLite helpers
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


def _insert_event(
    conn: sqlite3.Connection, table: str, url: str, timestamp: str,
    visits_col: str, filename: str = '',
) -> bool:
    """Insert a timestamped event for url into table if the URL exists in visits,
    and increment the corresponding counter column in visits by 1.

    The filename is normalized to its basename before being stored (background.js
    sends Chrome's relative path under Downloads, e.g. 'browser-visit-snapshots/
    <hash>.mhtml'; the parent directory is captured separately in the directory
    column so the basename alone is sufficient).

    Returns True if the URL exists (event inserted or duplicate ignored),
    False if no visits record exists for the URL.

    table and visits_col are trusted internal constants, never user-supplied.
    """
    exists = conn.execute("SELECT 1 FROM visits WHERE url = ?", (url,)).fetchone()
    if exists:
        basename = os.path.basename(filename)
        conn.execute(
            f"INSERT OR IGNORE INTO {table} (url, timestamp, filename, directory) "
            f"VALUES (?, ?, ?, ?)",
            (url, timestamp, basename, DOWNLOADS_SNAPSHOTS_DIR),
        )
        conn.execute(
            f"UPDATE visits SET {visits_col} = {visits_col} + 1 WHERE url = ?",
            (url,),
        )
    conn.commit()
    return exists is not None


def _fetch_events(conn: sqlite3.Connection, table: str, url: str) -> list:
    """Return all events for url from table as dicts {timestamp, filename, directory},
    sorted ascending by timestamp.

    table is a trusted internal constant, never user-supplied.
    """
    return [
        {'timestamp': r[0], 'filename': r[1], 'directory': r[2]}
        for r in conn.execute(
            f"SELECT timestamp, filename, directory FROM {table} "
            f"WHERE url = ? ORDER BY timestamp ASC",
            (url,),
        ).fetchall()
    ]


def insert_visit(conn: sqlite3.Connection, timestamp: str, url: str, title: str) -> None:
    """Insert a new visit row; silently ignored if the URL already exists."""
    conn.execute(
        "INSERT OR IGNORE INTO visits (url, timestamp, title) VALUES (?, ?, ?)",
        (url, timestamp, title),
    )
    conn.commit()


def query_visit(conn: sqlite3.Connection, url: str) -> 'dict | None':
    """Return the visit record for url as a dict, or None if no record exists."""
    row = conn.execute(
        "SELECT timestamp, title, of_interest FROM visits WHERE url = ?",
        (url,),
    ).fetchone()
    if row is None:
        return None
    return {
        'timestamp':   row[0],
        'title':       row[1],
        'of_interest': True if row[2] else None,
        'read':        _fetch_events(conn, 'read_events',    url),
        'skimmed':     _fetch_events(conn, 'skimmed_events', url),
    }


def tag_visit(
    conn: sqlite3.Connection, url: str, tag: str, tag_timestamp: str, filename: str = '',
) -> bool:
    """Set the of_interest, read, or skimmed timestamp on the visit record for url.

    For 'read' and 'skimmed' tags, filename is the snapshot filename as Chrome
    reports it (relative path under ~/Downloads, e.g.
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

# ---------------------------------------------------------------------------
# Log file helper
# ---------------------------------------------------------------------------

def append_log(timestamp: str, url: str, title: str, tag: str = '') -> None:
    """Append one TSV line (3 fields for auto-log, 4 fields when tag is set)."""
    def sanitise(s: str) -> str:
        return s.replace('\t', ' ').replace('\n', ' ').replace('\r', '')

    fields = [sanitise(timestamp), sanitise(url), sanitise(title)]
    if tag:
        fields.append(sanitise(tag))
    line = '\t'.join(fields) + '\n'
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line)


def append_result_log(result: str) -> None:
    """Append a single-field result line: 'success' or 'error: <message>'."""
    def sanitise(s: str) -> str:
        return s.replace('\t', ' ').replace('\n', ' ').replace('\r', '')

    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(sanitise(result) + '\n')

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

VALID_TAGS = {'of_interest', 'read', 'skimmed'}


def main() -> None:
    try:
        message = read_message()
        logger.debug('Received: %s', message)
    except Exception as exc:
        logger.error('Failed to read message: %s', exc)
        write_message({'status': 'error', 'message': str(exc)})
        return

    url    = (message.get('url') or '').strip()
    action = (message.get('action') or '').strip()

    # Query action: read-only lookup, no log writes.
    if action == 'query':
        if not url:
            write_message({'status': 'error', 'message': 'url is required'})
            return
        try:
            conn = sqlite3.connect(DB_FILE)
            ensure_db(conn)
            record = query_visit(conn, url)
            conn.close()
        except Exception as exc:
            logger.error('SQLite query failed: %s', exc)
            write_message({'status': 'error', 'message': str(exc)})
            return
        write_message({'status': 'ok', 'record': record})
        return

    timestamp = (message.get('timestamp') or '').strip()
    title     = message.get('title') or ''
    tag       = (message.get('tag')      or '').strip()
    filename  = (message.get('filename') or '').strip()

    if not url:
        write_message({'status': 'error', 'message': 'url is required'})
        return
    if not timestamp:
        write_message({'status': 'error', 'message': 'timestamp is required'})
        return
    if tag and tag not in VALID_TAGS:
        write_message({'status': 'error', 'message': f'invalid tag: {tag}'})
        return

    errors = []

    # First write: record the intended action
    try:
        append_log(timestamp, url, title, tag)
    except Exception as exc:
        logger.error('Log file write failed: %s', exc)
        errors.append(f'log: {exc}')

    # Write to SQLite.  Always insert the visit first (INSERT OR IGNORE, so the
    # original first-visit timestamp wins if the row already exists); then apply
    # the tag if one is present.  This means tagging always works even when the
    # background auto-log hasn't finished writing yet.
    try:
        conn = sqlite3.connect(DB_FILE)
        ensure_db(conn)
        insert_visit(conn, timestamp, url, title)
        if tag:
            tag_visit(conn, url, tag, timestamp, filename)
        conn.close()
    except Exception as exc:
        logger.error('SQLite write failed: %s', exc)
        errors.append(f'db: {exc}')

    # Second write: record the result
    log_result = f'error: {"; ".join(errors)}' if errors else 'success'
    try:
        append_result_log(log_result)
    except Exception as exc:
        logger.error('Log file result write failed: %s', exc)

    if errors:
        write_message({'status': 'error', 'errors': errors})
    else:
        write_message({'status': 'ok'})


if __name__ == '__main__':  # pragma: no cover
    main()
