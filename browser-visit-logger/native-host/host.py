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

Tag action (from popup.js):
    { "timestamp": "...", "url": "...", "title": "...", "tag": "memorable"|"read"|"skimmed" }
    → UPDATE memorable, read, or skimmed column on the row for that URL using
      the message timestamp; append 4-field TSV line.

Schema
------
    visits (
        url       TEXT PRIMARY KEY,
        timestamp TEXT NOT NULL,        -- set on first visit, never updated
        title     TEXT NOT NULL DEFAULT '',
        memorable TEXT,                 -- ISO timestamp, NULL until tagged
        read      TEXT,                 -- ISO timestamp, NULL until tagged
        skimmed   TEXT                  -- ISO timestamp, NULL until tagged
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
        # Fresh database
        conn.execute("""
            CREATE TABLE visits (
                url       TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                title     TEXT NOT NULL DEFAULT '',
                memorable TEXT,
                read      TEXT,
                skimmed   TEXT
            )
        """)
    elif 'id' in cols:
        # Old id-based schema — migrate to url-primary-key schema
        conn.execute("""
            CREATE TABLE visits_new (
                url       TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                title     TEXT NOT NULL DEFAULT '',
                memorable TEXT,
                read      TEXT,
                skimmed   TEXT
            )
        """)
        conn.execute("""
            INSERT OR IGNORE INTO visits_new (url, timestamp, title)
            SELECT url, timestamp, title FROM visits
        """)
        conn.execute("DROP TABLE visits")
        conn.execute("ALTER TABLE visits_new RENAME TO visits")

    else:
        # Add skimmed column to existing url-PK schema if missing
        if 'skimmed' not in cols:
            conn.execute("ALTER TABLE visits ADD COLUMN skimmed TEXT")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_visits_timestamp ON visits(timestamp)"
    )
    conn.commit()


def insert_visit(conn: sqlite3.Connection, timestamp: str, url: str, title: str) -> None:
    """Insert a new visit row; silently ignored if the URL already exists."""
    conn.execute(
        "INSERT OR IGNORE INTO visits (url, timestamp, title) VALUES (?, ?, ?)",
        (url, timestamp, title),
    )
    conn.commit()


def tag_visit(conn: sqlite3.Connection, url: str, tag: str, tag_timestamp: str) -> bool:
    """Set the memorable, read, or skimmed timestamp on the visit record for url.

    Returns True if a row was found and updated, False if no record exists for url.
    """
    if tag == 'memorable':
        cursor = conn.execute(
            "UPDATE visits SET memorable = ? WHERE url = ?",
            (tag_timestamp, url),
        )
    elif tag == 'read':
        cursor = conn.execute(
            "UPDATE visits SET read = ? WHERE url = ?",
            (tag_timestamp, url),
        )
    elif tag == 'skimmed':
        cursor = conn.execute(
            "UPDATE visits SET skimmed = ? WHERE url = ?",
            (tag_timestamp, url),
        )
    else:
        return False
    conn.commit()
    return cursor.rowcount > 0

# ---------------------------------------------------------------------------
# Log file helper
# ---------------------------------------------------------------------------

def append_log(timestamp: str, url: str, title: str, tag: str = '', result: str = '') -> None:
    """Append one TSV line (3 fields for auto-log, 4 with tag, 5 with tag + result)."""
    def sanitise(s: str) -> str:
        return s.replace('\t', ' ').replace('\n', ' ').replace('\r', '')

    fields = [sanitise(timestamp), sanitise(url), sanitise(title)]
    if tag:
        fields.append(sanitise(tag))
    if result:
        fields.append(sanitise(result))
    line = '\t'.join(fields) + '\n'
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

VALID_TAGS = {'memorable', 'read', 'skimmed'}


def main() -> None:
    try:
        message = read_message()
        logger.debug('Received: %s', message)
    except Exception as exc:
        logger.error('Failed to read message: %s', exc)
        write_message({'status': 'error', 'message': str(exc)})
        return

    url       = (message.get('url') or '').strip()
    timestamp = (message.get('timestamp') or '').strip()
    title     = message.get('title') or ''
    tag       = (message.get('tag') or '').strip()

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
    no_record = False
    no_record_msg = 'No record found for this URL — visit the page before tagging'

    # First write: record the intended action
    try:
        append_log(timestamp, url, title, tag)
    except Exception as exc:
        logger.error('Log file write failed: %s', exc)
        errors.append(f'log: {exc}')

    # Write to SQLite
    try:
        conn = sqlite3.connect(DB_FILE)
        ensure_db(conn)
        if tag:
            found = tag_visit(conn, url, tag, timestamp)
            if not found:
                no_record = True
        else:
            insert_visit(conn, timestamp, url, title)
        conn.close()
    except Exception as exc:
        logger.error('SQLite write failed: %s', exc)
        errors.append(f'db: {exc}')

    # Second write: record the result
    if no_record:
        log_result = f'error: {no_record_msg}'
    elif errors:
        log_result = f'error: {"; ".join(errors)}'
    else:
        log_result = 'success'
    try:
        append_log(timestamp, url, title, tag, log_result)
    except Exception as exc:
        logger.error('Log file result write failed: %s', exc)

    if no_record:
        write_message({'status': 'error', 'message': no_record_msg})
    elif errors:
        write_message({'status': 'error', 'errors': errors})
    else:
        write_message({'status': 'ok'})


if __name__ == '__main__':
    main()
