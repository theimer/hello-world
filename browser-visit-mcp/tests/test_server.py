"""Tests for browser-visit-mcp server.

We exercise the underlying ``_do_query`` / ``_do_schema`` helpers
directly rather than spinning up the MCP transport — the MCP layer is
a thin wrapper, and the SDK has its own tests.  The validation helper
``_validate_readonly_sql`` is also tested in isolation since it is the
primary safety mechanism.
"""
from __future__ import annotations

import sqlite3

import pytest

import server


# ---------------------------------------------------------------------------
# _validate_readonly_sql
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('sql', [
    'SELECT 1',
    'select * from visits',
    '  SELECT url FROM visits;  ',
    'WITH x AS (SELECT 1) SELECT * FROM x',
    'EXPLAIN SELECT * FROM visits',
    'PRAGMA table_info(visits)',
    '-- comment\nSELECT 1',
    '/* hi */ SELECT 1',
    "SELECT 'a;b' FROM visits",  # semicolon inside a string literal
])
def test_validate_accepts_readonly(sql):
    server._validate_readonly_sql(sql)


@pytest.mark.parametrize('sql', [
    '',
    '   ',
    'INSERT INTO visits VALUES (1)',
    'UPDATE visits SET read = 1',
    'DELETE FROM visits',
    'DROP TABLE visits',
    'ATTACH DATABASE "x" AS y',
    'CREATE TABLE t (x INT)',
    'SELECT 1; SELECT 2',
    'SELECT 1; DROP TABLE visits',
    'PRAGMA writable_schema = 1',
    'PRAGMA journal_mode = WAL',
    'VACUUM',
])
def test_validate_rejects_writes_and_multi(sql):
    with pytest.raises(ValueError):
        server._validate_readonly_sql(sql)


@pytest.mark.parametrize('sql,fragment', [
    # n == 0 after comment stripping (line 104)
    ('-- just a comment\n-- another',   'no statements'),
    # n == 1 but leading token isn't a word (line 110)
    ('123 not really sql',              'leading SQL keyword'),
    # PRAGMA without a target (line 122)
    ('PRAGMA',                          'PRAGMA missing target'),
])
def test_validate_specific_error_paths(sql, fragment):
    """Cover each distinct error-message branch in the validator."""
    with pytest.raises(ValueError, match=fragment):
        server._validate_readonly_sql(sql)


# ---------------------------------------------------------------------------
# _do_query
# ---------------------------------------------------------------------------

def test_query_returns_columns_and_rows(seeded_db):
    out = server._do_query(seeded_db,
                           'SELECT url, title FROM visits ORDER BY url', 100)
    assert out['columns'] == ['url', 'title']
    assert out['row_count'] == 4
    assert out['truncated'] is False
    assert out['rows'][0] == ['https://a.example/', 'Alpha']


def test_query_truncates_at_max_rows(seeded_db):
    out = server._do_query(seeded_db, 'SELECT url FROM visits', max_rows=2)
    assert out['row_count'] == 2
    assert out['truncated'] is True


def test_query_rejects_write_statement(seeded_db):
    with pytest.raises(ValueError):
        server._do_query(seeded_db,
                         "INSERT INTO visits (url, timestamp) "
                         "VALUES ('x', 'y')", 100)


def test_query_ro_connection_blocks_writes_even_if_validation_bypassed(seeded_db):
    # Sanity: even reaching SQLite with a write would be refused because
    # the connection is opened with mode=ro.
    conn = server._open_ro(seeded_db)
    try:
        with pytest.raises(sqlite3.OperationalError):
            conn.execute("INSERT INTO visits (url, timestamp) "
                         "VALUES ('z', 'now')")
    finally:
        conn.close()


def test_query_missing_db(tmp_path):
    with pytest.raises(FileNotFoundError):
        server._do_query(str(tmp_path / 'nope.db'), 'SELECT 1', 100)


def test_query_invalid_max_rows(seeded_db):
    with pytest.raises(ValueError):
        server._do_query(seeded_db, 'SELECT 1', max_rows=0)


def test_query_hard_cap_applies(seeded_db, monkeypatch):
    # Ask for more than HARD_MAX_ROWS; result row_count stays bounded.
    monkeypatch.setattr(server, 'HARD_MAX_ROWS', 2)
    out = server._do_query(seeded_db, 'SELECT url FROM visits', max_rows=999)
    assert out['row_count'] == 2
    assert out['truncated'] is True


# ---------------------------------------------------------------------------
# _do_schema
# ---------------------------------------------------------------------------

def test_schema_lists_all_tables(seeded_db):
    out = server._do_schema(seeded_db)
    assert set(out['tables']) == {
        'visits', 'read_events', 'skimmed_events',
        'snapshots', 'mover_errors',
    }
    assert 'CREATE TABLE' in out['ddl']
    assert 'visits' in out['ddl']


# ---------------------------------------------------------------------------
# MCP server wiring: _build_server, _parse_args, main
# ---------------------------------------------------------------------------

def test_build_server_registers_tools_that_dispatch_correctly(seeded_db):
    """The FastMCP wrapper should expose `query` and `schema` tools whose
    bodies delegate to the underlying helpers we already cover above.

    Tools are registered via the @mcp.tool() decorator; FastMCP stashes
    them in ``_tool_manager._tools[name].fn``.  Invoking those closures
    is what makes the wrapper body (and the lazy `mcp` import) reachable
    by line coverage.
    """
    mcp = server._build_server(seeded_db)
    tools = mcp._tool_manager._tools
    assert set(tools) == {'query', 'schema'}

    # `query` closure: hits _do_query and returns the same shape.
    query_fn = tools['query'].fn
    out = query_fn(sql='SELECT url FROM visits ORDER BY url')
    assert out['columns'] == ['url']
    assert out['rows'][0] == ['https://a.example/']

    # `schema` closure: hits _do_schema.
    schema_fn = tools['schema'].fn
    sch = schema_fn()
    assert 'visits' in sch['tables']


def test_parse_args_defaults_to_db_file_env():
    args = server._parse_args([])
    assert args.db == server.DB_FILE


def test_parse_args_db_override():
    args = server._parse_args(['--db', '/tmp/other.db'])
    assert args.db == '/tmp/other.db'


def test_main_invokes_mcp_run(monkeypatch, seeded_db):
    """main() builds the server and calls mcp.run; we mock .run so we
    don't actually spawn a stdio loop in the test process."""
    called = {}

    def fake_run(self, transport):
        called['transport'] = transport

    from mcp.server.fastmcp import FastMCP
    monkeypatch.setattr(FastMCP, 'run', fake_run)

    rc = server.main(['--db', seeded_db])
    assert rc == 0
    assert called == {'transport': 'stdio'}
