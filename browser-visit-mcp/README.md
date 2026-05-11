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

```bash
cd browser-visit-mcp
pip install -e .         # installs `mcp[cli]>=1.0`
```

Python 3.10+. No other runtime dependencies.

## Run

```bash
# Default DB at ~/browser-visits.db (override via BVL_DB_FILE or --db).
python3 server.py

# Point at a test DB.
python3 server.py --db /tmp/test.db
BVL_DB_FILE=/tmp/test.db python3 server.py
```

The server speaks MCP over stdio — it is meant to be launched by an
MCP client, not used interactively.

### Inspector

```bash
npx @modelcontextprotocol/inspector python3 server.py
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

```bash
pip install pytest
pytest tests/
```

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
