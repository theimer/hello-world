#!/usr/bin/env bash
# Thin wrapper: launches the MCP server with the bundled Python script.
# Forwards all arguments verbatim.  Use as the `command` for an MCP
# client (Claude Code / Claude Desktop) that expects a single
# executable on PATH.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${DIR}/server.py" "$@"
