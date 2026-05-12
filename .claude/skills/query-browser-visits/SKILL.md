---
name: query-browser-visits
description: Use when the user asks about their browsing history, pages they've visited/read/skimmed, or wants to search the content of pages they've visited. Data sources are a SQLite metadata DB (via `browser-visits` MCP server) and snapshot files (.mhtml/.pdf) under ~/Documents/browser-visit-logger/snapshots/.
---

# Querying browser visit data

Two data sources. Pick the right one (or combine):

## 1. Metadata / structured questions → SQL via MCP

Use the `browser-visits` MCP server: `mcp__browser-visits__query(sql, max_rows?)` and `mcp__browser-visits__schema()`. Read-only; SELECT/WITH/EXPLAIN/safe-PRAGMA only.

Tables (call `schema()` for full DDL):
- `visits` — `url` (PK), `timestamp`, `title`, `of_interest`, `read`, `skimmed`
- `read_events` — `url`, `timestamp` (composite PK), `filename`, `directory`
- `skimmed_events` — same shape as `read_events`
- `snapshots` — `date` (PK), `sealed`
- `mover_errors` — operational, usually skip

Use for: "what did I read on <date>", "top domains this week", "find the URL of the page titled X", "read vs skimmed counts", etc.

If the `browser-visits` MCP server is not connected in the current session, say so — do not try to open the SQLite file directly.

## 2. Full-text content search → grep / pdfgrep

Snapshots live under `~/Documents/browser-visit-logger/snapshots/YYYY-MM-DD/`:
- `*.mhtml` — search with `grep -ril "pattern" ~/Documents/browser-visit-logger/snapshots/`
- `*.pdf` — search with `pdfgrep -ril "pattern" ~/Documents/browser-visit-logger/snapshots/`
- `MANIFEST.tsv` (per day) — TSV columns: `filename`, `tag` (read/skimmed), `iso-timestamp`, `url`, `title`

Map a matched snapshot filename back to URL/title:
- `grep "<filename>" ~/Documents/browser-visit-logger/snapshots/*/MANIFEST.tsv`, or
- SQL: `SELECT url, timestamp FROM read_events WHERE filename = '<filename>' UNION SELECT url, timestamp FROM skimmed_events WHERE filename = '<filename>';`

**mhtml gotcha:** content is typically quoted-printable encoded — spaces become `=20`, `=` becomes `=3D`, and long lines are wrapped with trailing `=`. Multi-word phrases will be split across lines. Search for single distinctive tokens, not phrases. URLs and headers are usually in plain headers near the top.

## 3. Hybrid (often the right move)

Narrow first with SQL, then grep only the matching files. Example: "find the page from last Tuesday that mentioned redis pipelines" → SQL `read_events` filtered by date to get `directory` + `filename` values, then grep only those.

## Notes
- DB default path: `~/browser-visits.db` (override: `BVL_DB_FILE`).
- Snapshots root: `~/Documents/browser-visit-logger/snapshots/`.
- `pdfgrep` lives at `/opt/homebrew/bin/pdfgrep` on this machine.
