# Browser Visit projects

Three related projects that share a local SQLite database of browser
visit history.  Everything runs locally on your machine; nothing is
sent anywhere.

## [`browser-visit-logger/`](browser-visit-logger/)

A Chrome extension and native-messaging host that records every page
you visit to a local SQLite database and a per-day TSV log file, lets
you tag pages of interest from a popup, and archives full-page
snapshots (MHTML or PDF) into iCloud-synced daily folders sealed with
a tab-delimited manifest once each day has passed.

The address-bar icon turns gray / orange / yellow / green based on the
current tab's tag state (untagged / of-interest / skimmed / read).

## [`browser-visit-tools/`](browser-visit-tools/)

Standalone read-only consumers of the database the logger produces.
Currently:

- **`reading_list.py`** — generates a reading list of every URL
  tagged of_interest but not yet read, split into "Unread URLs that
  have been skimmed" and "Unread URLs" tables.  HTML by default
  (self-contained, openable in a browser); pass `--format markdown`
  for Markdown.

These tools depend only on the DB schema, not on logger code, so the
directories can be developed and vendored independently.

## [`browser-visit-mcp/`](browser-visit-mcp/)

A local [Model Context Protocol](https://modelcontextprotocol.io/)
server that exposes the visits database to MCP clients (Claude Code,
Claude Desktop, MCP Inspector) over stdio.  Provides two tools:
`query` (run an arbitrary read-only SQL statement and get
columns + rows back) and `schema` (return the DDL so the model can
write well-formed queries).  Read-only is enforced both by opening
SQLite with `mode=ro` and by validating each statement before
execution.

Like the tools project, this is a pure read-only consumer of the
schema — no Python imports cross the directory boundary.  A
`.mcp.json` at the repo root registers the server project-locally so
Claude Code picks it up automatically when this repo is opened.
