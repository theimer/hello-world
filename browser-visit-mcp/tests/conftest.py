"""pytest config for browser-visit-mcp.

Adds the parent directory to sys.path so ``import server`` works from
any test file, and provides a ``seeded_db`` fixture that builds a
temporary SQLite DB from the canonical ``schema.sql`` shipped with
``browser-visit-logger``.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))

_SCHEMA_SQL = _HERE.parent.parent / 'browser-visit-logger' / 'schema.sql'


def _load_schema(conn: sqlite3.Connection) -> None:
    ddl = _SCHEMA_SQL.read_text()
    # The schema uses a sentinel for the snapshots dir default; any
    # non-empty placeholder works for tests since we don't insert into
    # the event tables without specifying ``directory`` ourselves.
    ddl = ddl.replace('__BVL_DOWNLOADS_SNAPSHOTS_DIR__', '/tmp/bvl-test')
    conn.executescript(ddl)


@pytest.fixture
def seeded_db(tmp_path) -> str:
    """Return the path to a temp SQLite DB seeded with the real schema
    and a handful of representative rows."""
    db = tmp_path / 'visits.db'
    conn = sqlite3.connect(db)
    try:
        _load_schema(conn)
        conn.executemany(
            "INSERT INTO visits (url, timestamp, title, of_interest, read, skimmed) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [
                ('https://a.example/',  '2026-05-01T12:00:00Z', 'Alpha',  'star', 0, 0),
                ('https://b.example/',  '2026-05-02T12:00:00Z', 'Beta',   'star', 0, 2),
                ('https://c.example/',  '2026-05-03T12:00:00Z', 'Gamma',  None,   1, 0),
                ('https://d.example/',  '2026-05-04T12:00:00Z', 'Delta',  'star', 1, 1),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    return str(db)
