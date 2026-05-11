# Browser Visit MCP

A local [Model Context Protocol](https://modelcontextprotocol.io/)
server that exposes **read-only SQL access** to the SQLite database
produced by the sibling
[`browser-visit-logger/`](../browser-visit-logger/) Chrome extension.

It's a read-only consumer of the schema (`visits`, `read_events`,
`skimmed_events`, `snapshots`, `mover_errors`) — no Python imports
cross the directory boundary, so this project can be vendored
independently.

## Tools

| Tool | Input | Output |
|------|-------|--------|
| `query` | `sql: string`, optional `max_rows: int` (default 1000, hard cap 10000) | `{columns, rows, row_count, truncated}` |
| `schema` | — | `{tables, ddl}` — concatenated `CREATE TABLE` / `CREATE INDEX` |

### Read-only safety

Defense in depth:

1. The SQLite connection is opened with `mode=ro` via URI — the OS
   refuses writes.
2. The SQL string is parsed before execution. Exactly one statement
   is allowed; the leading keyword must be `SELECT`, `WITH`,
   `EXPLAIN`, or `PRAGMA`. `PRAGMA` is restricted to a safelist of
   introspection commands (`table_info`, `index_list`, `index_info`,
   `foreign_key_list`, `database_list`, `table_xinfo`).

A 30 s watchdog interrupts runaway queries.

## Install

Requires Python 3.10+ (the `mcp` SDK refuses older Pythons; the
project is developed against 3.13). No other runtime dependencies.

The project assumes a venv at `browser-visit-mcp/.venv/` — that's where
the `.mcp.json` at the repo root expects to find the interpreter. The
venv is gitignored, so you create it once per checkout:

```bash
cd browser-visit-mcp
python3.13 -m venv .venv          # or python3.10 / 3.11 / 3.12
source .venv/bin/activate         # activate so `pip` and `python` mean the venv's
pip install -e .                  # installs `mcp[cli]>=1.0` + this package
```

If you'd rather not activate, you can run `pip` directly with the
venv's path: `.venv/bin/pip install -e .` — equivalent.

## Run

With the venv activated (`source .venv/bin/activate`), or by invoking
the venv's interpreter directly:

```bash
# Default DB at ~/browser-visits.db (override via BVL_DB_FILE or --db).
python server.py

# Point at a test DB.
python server.py --db /tmp/test.db
BVL_DB_FILE=/tmp/test.db python server.py
```

The server speaks MCP over stdio — it is meant to be launched by an
MCP client, not used interactively.

### Inspector

```bash
npx @modelcontextprotocol/inspector .venv/bin/python server.py
```

### Claude Code

This repo ships an `.mcp.json` at the repo root that defines the
`browser-visits` server scoped to this project — open the repo in
Claude Code and approve the server on first prompt (or set
`enableAllProjectMcpServers: true` in your personal settings to
auto-approve). The definition uses absolute paths into
`browser-visit-mcp/.venv/`, so make sure you've run `pip install -e .`
inside that venv first.

To point at a different DB, add an `env` block to the entry in
`.mcp.json`:

```json
{
  "mcpServers": {
    "browser-visits": {
      "command": ".../browser-visit-mcp/.venv/bin/python",
      "args": [".../browser-visit-mcp/server.py"],
      "env": { "BVL_DB_FILE": "/path/to/visits.db" }
    }
  }
}
```

## Tests

With the venv activated:

```bash
pip install pytest pytest-cov
pytest tests/ --cov=server --cov-report=term-missing
```

37 tests, 100% line coverage on `server.py`.

The tests seed a temp DB from
`../browser-visit-logger/schema.sql` and exercise the validation
helper, the query path (including row-cap truncation), and the schema
introspection tool.

## Project layout

```
browser-visit-mcp/
├── server.py                 # MCP server (FastMCP + sqlite3)
├── browser_visits_mcp.sh     # stdio wrapper for MCP clients
├── pyproject.toml
├── tests/
│   ├── conftest.py
│   └── test_server.py
└── README.md
```
