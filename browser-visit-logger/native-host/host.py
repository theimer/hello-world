#!/usr/bin/env python3
"""
Native messaging host for Browser Visit Logger.

Receives a single JSON visit record from Chrome via stdin (native messaging
protocol: 4-byte LE length prefix + UTF-8 JSON), writes the record to a TSV
log file and a SQLite database, then sends a JSON response to stdout.

Chrome launches this script once per sendNativeMessage() call (MV3 one-shot
semantics): read one message → write outputs → respond → exit.
"""

import json
import logging
import os
import sqlite3
import struct
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------------------------------
# Paths (all in the user's home directory)
# ---------------------------------------------------------------------------
HOME     = os.path.expanduser('~')
LOG_FILE = os.path.join(HOME, 'browser-visits.log')
DB_FILE  = os.path.join(HOME, 'browser-visits.db')
HOST_LOG = os.path.join(HOME, 'browser-visits-host.log')

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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS visits (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT    NOT NULL,
            url       TEXT    NOT NULL,
            title     TEXT    NOT NULL DEFAULT ''
        )
    """)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_visits_timestamp ON visits(timestamp)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_visits_url ON visits(url)"
    )
    conn.commit()


def insert_visit(conn: sqlite3.Connection, timestamp: str, url: str, title: str) -> None:
    conn.execute(
        "INSERT INTO visits (timestamp, url, title) VALUES (?, ?, ?)",
        (timestamp, url, title),
    )
    conn.commit()

# ---------------------------------------------------------------------------
# Log file helper
# ---------------------------------------------------------------------------

def append_log(timestamp: str, url: str, title: str) -> None:
    """Append one TSV line. Tabs and newlines in field values are replaced."""
    def sanitise(s: str) -> str:
        return s.replace('\t', ' ').replace('\n', ' ').replace('\r', '')

    line = f"{sanitise(timestamp)}\t{sanitise(url)}\t{sanitise(title)}\n"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        message = read_message()
        logger.debug('Received: %s', message)
    except Exception as exc:
        logger.error('Failed to read message: %s', exc)
        write_message({'status': 'error', 'message': str(exc)})
        return

    timestamp = message.get('timestamp') or datetime.now(timezone.utc).isoformat()
    url       = message.get('url', '')
    title     = message.get('title', '')

    errors = []

    # Write to TSV log (independent of DB write)
    try:
        append_log(timestamp, url, title)
    except Exception as exc:
        logger.error('Log file write failed: %s', exc)
        errors.append(f'log: {exc}')

    # Write to SQLite (independent of log write)
    try:
        conn = sqlite3.connect(DB_FILE)
        ensure_db(conn)
        insert_visit(conn, timestamp, url, title)
        conn.close()
    except Exception as exc:
        logger.error('SQLite write failed: %s', exc)
        errors.append(f'db: {exc}')

    if errors:
        write_message({'status': 'error', 'errors': errors})
    else:
        write_message({'status': 'ok'})


if __name__ == '__main__':
    main()
