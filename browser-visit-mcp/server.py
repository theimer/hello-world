#!/usr/bin/env python3
"""
server.py — MCP server exposing read-only SQL access to the browser-visits DB.

Provides two tools:

  * ``query``  — run an arbitrary read-only SQL statement and return
                 columns + rows as JSON.
  * ``schema`` — return the DDL of the database so the model can write
                 well-formed queries without guessing column names.

Read-only is enforced at two layers:

  1. The SQLite connection is opened with ``mode=ro`` via URI.
  2. The SQL string is validated before execution — exactly one
     statement, leading keyword in {SELECT, WITH, EXPLAIN, PRAGMA},
     and PRAGMAs are limited to a safelist of introspection ones.

This is a read-only consumer of the database produced by the sibling
``browser-visit-logger`` project; it imports nothing from sibling
projects.

Usage (stdio MCP transport):
    python3 server.py
    BVL_DB_FILE=/tmp/test.db python3 server.py
    python3 server.py --db /tmp/test.db
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import threading
from typing import Any


HOME = os.path.expanduser('~')
DB_FILE = os.environ.get('BVL_DB_FILE', os.path.join(HOME, 'browser-visits.db'))

DEFAULT_MAX_ROWS = 1000
HARD_MAX_ROWS = 10000
DEFAULT_TIMEOUT_S = 30.0

_PRAGMA_SAFELIST = frozenset({
    'table_info',
    'index_list',
    'index_info',
    'foreign_key_list',
    'database_list',
    'table_xinfo',
})

_LEADING_KEYWORDS = frozenset({'SELECT', 'WITH', 'EXPLAIN', 'PRAGMA'})

# Matches the FIRST keyword (letters/underscore) after leading whitespace
# in a stripped, comment-free SQL string.
_FIRST_TOKEN = re.compile(r'^\s*([A-Za-z_]+)')

# Matches an inline ``--`` line comment to end-of-line and ``/* ... */``
# block comments.  Used by ``_strip_comments`` before validation.
_LINE_COMMENT  = re.compile(r'--[^\n]*')
_BLOCK_COMMENT = re.compile(r'/\*.*?\*/', re.DOTALL)

# String literal: single-quoted, with '' as an embedded quote.
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")


def _strip_comments(sql: str) -> str:
    return _BLOCK_COMMENT.sub(' ', _LINE_COMMENT.sub('', sql))


def _statement_count(sql: str) -> int:
    """Count top-level statements by counting non-string ``;`` chars.

    Replaces string literals with spaces of equal length so semicolons
    inside strings don't confuse the count.  A trailing ``;`` after the
    last real statement is allowed (does not increase the count).
    """
    masked = _STRING_LITERAL.sub(lambda m: ' ' * (m.end() - m.start()), sql)
    # split on ';' and discard a trailing empty/whitespace-only chunk
    parts = masked.split(';')
    if parts and not parts[-1].strip():
        parts = parts[:-1]
    return len([p for p in parts if p.strip()])


def _validate_readonly_sql(sql: str) -> None:
    """Raise ``ValueError`` if ``sql`` is not a single read-only statement.

    Rules:
      * exactly one statement (trailing semicolon allowed),
      * leading keyword is SELECT / WITH / EXPLAIN / PRAGMA,
      * PRAGMA target must be in the introspection safelist.
    """
    if not sql or not sql.strip():
        raise ValueError('SQL is empty')

    stripped = _strip_comments(sql)

    n = _statement_count(stripped)
    if n == 0:
        raise ValueError('SQL contains no statements')
    if n > 1:
        raise ValueError('Multiple statements are not allowed')

    m = _FIRST_TOKEN.match(stripped)
    if not m:
        raise ValueError('Could not identify leading SQL keyword')
    keyword = m.group(1).upper()
    if keyword not in _LEADING_KEYWORDS:
        raise ValueError(
            f'Statement must start with one of '
            f'{sorted(_LEADING_KEYWORDS)}; got {keyword!r}'
        )

    if keyword == 'PRAGMA':
        # capture the pragma name after PRAGMA: e.g. "PRAGMA table_info(visits)"
        pm = re.match(r'\s*PRAGMA\s+([A-Za-z_]+)', stripped, re.IGNORECASE)
        if not pm:
            raise ValueError('PRAGMA missing target')
        pragma = pm.group(1).lower()
        if pragma not in _PRAGMA_SAFELIST:
            raise ValueError(
                f'PRAGMA {pragma!r} is not in the read-only safelist '
                f'({sorted(_PRAGMA_SAFELIST)})'
            )


def _open_ro(db_path: str) -> sqlite3.Connection:
    """Open ``db_path`` in read-only mode.

    Uses the SQLite URI form so the OS-level open is also read-only —
    even a misbehaving statement that bypasses validation will fail at
    the SQLite layer.
    """
    if not os.path.exists(db_path):
        raise FileNotFoundError(f'Database not found: {db_path}')
    uri = f'file:{db_path}?mode=ro'
    return sqlite3.connect(uri, uri=True, isolation_level=None)


def _run_with_timeout(
    conn: sqlite3.Connection, sql: str, timeout_s: float
) -> sqlite3.Cursor:
    """Execute ``sql``; interrupt the connection if it exceeds ``timeout_s``.

    ``conn.interrupt()`` is the only thread-safe operation on a
    connection, which is why we use a watchdog timer rather than
    running the query itself on a worker thread.
    """
    timer = threading.Timer(timeout_s, conn.interrupt)
    timer.daemon = True
    timer.start()
    try:
        return conn.execute(sql)
    finally:
        timer.cancel()


def _do_query(db_path: str, sql: str, max_rows: int) -> dict[str, Any]:
    _validate_readonly_sql(sql)
    if max_rows < 1:
        raise ValueError('max_rows must be >= 1')
    if max_rows > HARD_MAX_ROWS:
        max_rows = HARD_MAX_ROWS

    conn = _open_ro(db_path)
    try:
        cur = _run_with_timeout(conn, sql, DEFAULT_TIMEOUT_S)
        columns = [d[0] for d in (cur.description or [])]
        # fetch one past the cap so we can flag truncation
        rows = cur.fetchmany(max_rows + 1)
        truncated = len(rows) > max_rows
        if truncated:
            rows = rows[:max_rows]
        return {
            'columns': columns,
            'rows': [list(r) for r in rows],
            'row_count': len(rows),
            'truncated': truncated,
        }
    finally:
        conn.close()


def _do_schema(db_path: str) -> dict[str, Any]:
    conn = _open_ro(db_path)
    try:
        rows = list(conn.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index') "
            "  AND name NOT LIKE 'sqlite_%' "
            "ORDER BY type, name"
        ))
    finally:
        conn.close()
    tables = [name for typ, name, _ in rows if typ == 'table']
    ddl = '\n\n'.join(s for _, _, s in rows if s)
    return {
        'tables': tables,
        'ddl': ddl,
    }


def _build_server(db_path: str):
    # Imported lazily so the pure helpers (and unit tests) don't require
    # the `mcp` SDK to be installed.
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP('browser-visits')

    @mcp.tool()
    def query(sql: str, max_rows: int = DEFAULT_MAX_ROWS) -> dict[str, Any]:
        """Run a read-only SQL statement against the browser-visits DB.

        Only ``SELECT`` / ``WITH`` / ``EXPLAIN`` and a small safelist of
        introspection ``PRAGMA``s are allowed.  Multi-statement input is
        rejected.  Returns at most ``max_rows`` rows (default 1000, hard
        cap 10000); ``truncated`` is true if more rows were available.
        """
        return _do_query(db_path, sql, max_rows)

    @mcp.tool()
    def schema() -> dict[str, Any]:
        """Return the DDL of the browser-visits DB.

        Output:
          - ``tables``: list of table names
          - ``ddl``:    concatenated ``CREATE TABLE`` / ``CREATE INDEX``
                        statements
        """
        return _do_schema(db_path)

    return mcp


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog='browser-visit-mcp',
        description='MCP server exposing read-only SQL access to the '
                    'browser-visits SQLite database.',
    )
    p.add_argument('--db', metavar='FILE', default=DB_FILE,
                   help=f'visits database path (default {DB_FILE})')
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = _parse_args(argv)
    mcp = _build_server(args.db)
    mcp.run(transport='stdio')
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
