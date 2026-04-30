# Browser Visit Logger

A Chrome extension + native-messaging host that records every page you
visit to a local SQLite database (and a TSV log file), lets you tag
pages of interest from a popup, and archives full-page snapshots (MHTML
or PDF) into iCloud-synced daily folders sealed by a tab-delimited
manifest once each day has passed.

Everything runs locally on your machine. Nothing is sent anywhere.

---

## What gets recorded

Two things, on every visit:

1. **An auto-log entry** — URL, page title, and an ISO timestamp, the
   moment the navigation completes. One row per URL (re-visits don't
   duplicate the row, but the original timestamp is preserved).
2. **Optional tags** — when you click the toolbar icon, a popup lets you
   mark the current page as one of:
   - **★ Of Interest** — set a flag on the visit row.
   - **✓ Read** — record a "read" event with its own timestamp and save
     a full snapshot of the page.
   - **~ Skimmed** — record a "skimmed" event with its own timestamp and
     save a full snapshot.

Snapshots are MHTML for normal pages, PDF for `.pdf` URLs (downloaded
directly to avoid Chrome's mangled PDF viewer output). Each file is named
`<YYYY-MM-DDTHH-MM-SSZ>-<sha256>.<ext>`, where the prefix is derived
from the click timestamp so the file's name is permanent and globally
sortable.

Snapshots land first in `~/Downloads/browser-visit-snapshots/`. A
background mover then archives them into
`~/Documents/browser-visit-logger/snapshots/<YYYY-MM-DD>/`, makes them
read-only, and (once the day has fully passed) writes a
`MANIFEST.tsv` summarising the directory.

---

## Storage layout

| Path | What it is |
|------|------------|
| `~/browser-visits.log` | TSV append-only log of every visit and tag action |
| `~/browser-visits.db` | SQLite database — `visits`, `read_events`, `skimmed_events` |
| `~/browser-visits-host.log` | Native host process log (rotated, 1 MiB × 3) |
| `~/browser-visits-mover.log` | Snapshot mover process log (LaunchAgent stdout/stderr) |
| `~/Downloads/browser-visit-snapshots/` | Snapshot staging dir (Chrome writes here) |
| `~/Documents/browser-visit-logger/snapshots/<YYYY-MM-DD>/` | Sealed daily archive (read-only files + read-only `MANIFEST.tsv`) |

All paths can be overridden via `BVL_*` environment variables — see
[Configuration](#configuration).

---

## Database schema

```sql
visits (
    url         TEXT PRIMARY KEY,
    timestamp   TEXT NOT NULL,                 -- first-visit timestamp
    title       TEXT NOT NULL DEFAULT '',
    of_interest TEXT,                          -- non-NULL once tagged
    read        INTEGER NOT NULL DEFAULT 0,    -- count of read clicks
    skimmed     INTEGER NOT NULL DEFAULT 0     -- count of skimmed clicks
)

read_events / skimmed_events (
    url       TEXT NOT NULL,
    timestamp TEXT NOT NULL,                   -- when the user tagged
    filename  TEXT NOT NULL DEFAULT '',        -- snapshot basename
    directory TEXT NOT NULL DEFAULT '<Downloads dir>',
    PRIMARY KEY (url, timestamp)
)

snapshots (
    date   TEXT PRIMARY KEY,                   -- 'YYYY-MM-DD' (UTC)
    sealed INTEGER NOT NULL DEFAULT 0          -- 0 = unsealed, 1 = sealed
)

mover_errors (
    key        TEXT PRIMARY KEY,               -- '<op>:<target>'
    operation  TEXT NOT NULL,                  -- 'move'|'seal'|'rewrite_manifest'|'top_level'
    target     TEXT NOT NULL,                  -- file path / date dir
    message    TEXT NOT NULL,                  -- last exception message
    first_seen TEXT NOT NULL,
    last_seen  TEXT NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 1,
    notified   INTEGER NOT NULL DEFAULT 0,
    immediate  INTEGER NOT NULL DEFAULT 0
)
```

The `visits` / `*_events` tables are owned by `host.py`; the `snapshots`
and `mover_errors` tables are owned by the mover and sealer. The mover
INSERTs a `snapshots` row (`sealed=0`) the first time it places a file
in a new daily directory; the seal pass and the manual sealer flip
`sealed=1`. The seal pass queries this table to find work — no
filesystem rescan needed. The `mover_errors` table tracks unresolved
mover failures so they can be surfaced to the user — see
[When something goes wrong](#when-something-goes-wrong).

---

## Installation

Tested on macOS with Chrome / Chrome Canary / Chromium. Linux works for
the extension and host but not the LaunchAgent-driven mover.

**Requirements:** `python3` ≥ 3.9, `openssl`.

```bash
git clone <repo>
cd browser-visit-logger
bash install.sh
```

`install.sh` is idempotent. It:

1. Generates a stable RSA key pair (once) and pins the extension ID via
   the `key` field in `manifest.json`.
2. Installs the native-messaging host manifest under Chrome's
   `NativeMessagingHosts` directory.
3. On macOS, installs and bootstraps the snapshot mover as a LaunchAgent
   (`com.browser.visit.logger.snapshot_mover`, `StartInterval = 3600`s).
4. Prints the extension ID and where to load the unpacked extension.

After it finishes:

1. Open `chrome://extensions` and enable **Developer mode**.
2. **Load unpacked** → select the `extension/` directory.
3. The extension ID Chrome displays should match the one printed by
   the installer.
4. Visit any page, then verify with:
   ```bash
   tail ~/browser-visits.log
   sqlite3 ~/browser-visits.db "SELECT * FROM visits ORDER BY timestamp DESC LIMIT 10;"
   ```

### Changing the mover cadence (macOS)

Edit `StartInterval` in `~/Library/LaunchAgents/com.browser.visit.logger.snapshot_mover.plist`,
then reload:

```bash
launchctl bootout   gui/$(id -u)/com.browser.visit.logger.snapshot_mover
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.browser.visit.logger.snapshot_mover.plist
```

---

## Using the extension

**Auto-logging is automatic.** Every page you finish navigating to is
logged. There's nothing to click.

**Tagging** — click the toolbar icon to open the popup. It shows the
current visit's history (first visited / of-interest / read / skimmed
timestamps) and three buttons:

- **★ Of Interest** — quick boolean flag.
- **✓ Read** — captures a snapshot, records a read event.
- **~ Skimmed** — captures a snapshot, records a skimmed event.

Read and Skimmed can each be clicked multiple times on the same URL —
each click creates a separate event row (its own timestamp + snapshot).

The popup stays open while the snapshot saves; on success it closes
itself, on failure it shows the error and re-enables the buttons.

---

## CLI scripts

All scripts live at the repo root or under `native-host/`. They share
the same `BVL_*` env-var conventions and accept overriding flags so
they're safe to point at test data.

### `install.sh`

One-shot installer; see [Installation](#installation). Re-run it any
time the extension or host changes location, or to refresh the
LaunchAgent.

```bash
bash install.sh
```

### `native-host/host.py`

The native messaging host invoked by Chrome. **Don't run this directly**
— it speaks Chrome's framed-stdio protocol (4-byte length prefix +
JSON), not a normal CLI. It's launched once per `sendNativeMessage` call.

### `native-host/snapshot_mover.py`

The periodic archiver. The LaunchAgent runs it hourly; you can also
invoke it by hand for ad-hoc runs or testing. Each invocation does one
move pass + one seal pass, then exits.

```bash
# Default run (uses configured paths)
python3 native-host/snapshot_mover.py

# Show what would happen without touching files or the DB
python3 native-host/snapshot_mover.py --dry-run --verbose

# Move every file regardless of age (default threshold is 60 s)
python3 native-host/snapshot_mover.py --min-age-seconds 0

# Operate on isolated paths (useful in tests / experiments)
python3 native-host/snapshot_mover.py \
    --db /tmp/test.db --source /tmp/src --dest /tmp/dst
```

Flags:

| Flag | Effect |
|------|--------|
| `--dry-run` | Log intentions but don't write or modify anything |
| `-v`, `--verbose` | DEBUG log level |
| `--min-age-seconds N` | Override the file-age threshold (default 60) |
| `--error-threshold N` | Override the persistent-error notification threshold (default 3) |
| `--source DIR` | Override `BVL_DOWNLOADS_SNAPSHOTS_DIR` |
| `--dest DIR` | Override `BVL_ICLOUD_SNAPSHOTS_DIR` |
| `--db FILE` | Override `BVL_DB_FILE` |
| `--show-errors` | Print pending mover errors and exit (skips the move/seal pass) |
| `--clear-errors` | Wipe every row from the `mover_errors` table and exit |
| `--clear-error N` | Delete the Nth row (1-indexed, matching `--show-errors` order) and exit |

`--show-errors`, `--clear-errors`, and `--clear-error N` are mutually
exclusive.

**What it does each run:**

1. **Move pass** — for every file in `~/Downloads/browser-visit-snapshots/`
   matching the snapshot filename format and at least `MIN_AGE_SECONDS`
   old: copy it to `<dest>/<YYYY-MM-DD>/`, chmod read-only, update the
   `directory` column in `read_events`/`skimmed_events`, and
   `INSERT OR IGNORE` a `(date, sealed=0)` row into the `snapshots`
   table; then unlink the source. **Straggler handling:** if the
   destination day's `snapshots` row was already `sealed=1`, the
   existing read-only `MANIFEST.tsv` is removed and rewritten to
   include the just-moved file (sealed flag stays 1, manifest stays
   `0o444`). Crash-safe and idempotent — interrupted runs are
   recovered on the next tick.
2. **Seal pass** — DB-driven, no filesystem rescan. Queries
   `snapshots WHERE sealed = 0 AND date < today_utc`. For each row:
   verifies the on-disk directory exists (warns and skips if not),
   writes a tab-delimited `MANIFEST.tsv` enumerating the directory's
   contents, chmods it `0o444`, then flips `sealed = 1`. If the
   manifest already exists (recovery from a partial prior seal that
   crashed before the DB update), the file is left untouched and only
   the DB flag is flipped.

### `native-host/snapshot_sealer.py`

Manual complement to the mover's seal pass. Forces sealing of one
specified directory immediately, regardless of whether its date has
passed. Useful for testing or for closing a directory early.

```bash
# Seal by date (resolved under ICLOUD_SNAPSHOTS_DIR)
python3 native-host/snapshot_sealer.py 2026-04-30

# Seal by absolute or relative path
python3 native-host/snapshot_sealer.py /Users/me/.../snapshots/2026-04-30

# Show what would happen without writing
python3 native-host/snapshot_sealer.py --dry-run 2026-04-30

# Override paths
python3 native-host/snapshot_sealer.py \
    --db /tmp/test.db --dest /tmp/snapshots 2026-04-30
```

Flags:

| Flag | Effect |
|------|--------|
| `--dry-run` | Log what would be sealed without writing the manifest |
| `-v`, `--verbose` | DEBUG log level |
| `--dest DIR` | Override `BVL_ICLOUD_SNAPSHOTS_DIR` |
| `--db FILE` | Override `BVL_DB_FILE` |

Refuses to overwrite an existing manifest (exit code 1). To re-seal,
delete the manifest first.

If the directory's basename is a valid `YYYY-MM-DD` date, the sealer
also upserts a `(date, sealed=1)` row into the `snapshots` table — so
the auto seal pass won't re-process it. Non-date-named directories
get a manifest but no table row.

**Manifest format** — `MANIFEST.tsv`, one header row + one row per file:

```
filename	tag	timestamp	url	title
2026-04-30T10-00-00Z-abc.mhtml	read	2026-04-30T10:00:00Z	https://example.com	Example Page
```

Files in the directory with no matching DB row appear with empty
metadata fields. Tabs / newlines / CRs in titles are sanitised to
spaces.

### `reset.py`

Wipes local data the extension produced. Asks for confirmation by default.

```bash
# Reset everything (visit log, host log, mover log, DB, both snapshot dirs)
python3 reset.py

# Skip confirmation
python3 reset.py -f

# Reset one thing only
python3 reset.py --log         # ~/browser-visits.log
python3 reset.py --host-log    # host log + mover log
python3 reset.py --db          # ~/browser-visits.db
python3 reset.py --snapshots   # ~/Downloads/browser-visit-snapshots/
python3 reset.py --icloud      # ~/Documents/browser-visit-logger/
```

Respects the same `BVL_*` env vars as the host, so custom paths Just
Work. Safe to run with no targets present — missing files/dirs are
reported and skipped.

### `point-to-worktree.py`

Developer convenience. Repoints the installed native-host manifest at
`host.py` inside a `.claude/worktrees/<branch>/` worktree so you can
test pre-merge changes without re-running `install.sh`. Restart Chrome
fully (Cmd-Q) for the change to take effect; `bash install.sh` reverts.

```bash
python3 point-to-worktree.py
```

---

## When something goes wrong

The mover catches `(OSError, sqlite3.Error)` from each per-file move,
per-directory seal, and per-directory straggler-rewrite, and logs to
`~/browser-visits-mover.log` (captured by the LaunchAgent's stdout/stderr).
Transient failures (one bad tick) clear themselves on the next successful
attempt; the user never sees them.

For everything else, the mover writes a row into the `mover_errors`
table keyed by `<operation>:<target>` and escalates it to a macOS
notification once the row qualifies:

- **Persistent failure**: the same `(operation, target)` has failed
  `BVL_MOVER_ERROR_THRESHOLD` times in a row (default 3, configurable
  via env var or `--error-threshold N`). One notification per streak.
- **Catastrophic failure**: notified on the first occurrence regardless
  of attempts. Includes:
  - Top-level uncaught exception (`operation = 'top_level'`).
  - `OSError` with errno in `{ENOSPC, EROFS, EDQUOT}` — disk full,
    read-only filesystem, or quota exceeded.
  - `sqlite3.DatabaseError` other than `OperationalError` — typically
    means the DB file is corrupt.

A row stays in the table until the underlying problem is resolved.
Three paths clear it:

1. **Automatic** — the next successful run of the same operation
   `_clear_error`s the row. Most users never need to touch the table.
2. **Manual, all rows** — `python3 native-host/snapshot_mover.py
   --clear-errors`. Use after acknowledging a batch of stale rows.
3. **Manual, one row** — `python3 native-host/snapshot_mover.py
   --clear-error N`, where `N` is the index from `--show-errors`.

To inspect: `python3 native-host/snapshot_mover.py --show-errors`.
Skips the move/seal pass and prints a numbered list of currently
pending errors with `attempts`, `first_seen`, `last_seen`, message,
and whether the user has been notified.

If macOS Notification Center can't be reached (`osascript` missing,
non-Darwin platform), the mover falls back to creating
`~/browser-visits-mover-needs-attention` so the user can spot it via
shell or Finder.

---

## Configuration

Every script honours these environment variables. CLI flags override
env vars; env vars override defaults.

| Variable | Default | Used by |
|----------|---------|---------|
| `BVL_LOG_FILE` | `~/browser-visits.log` | host, reset |
| `BVL_HOST_LOG` | `~/browser-visits-host.log` | host, reset |
| `BVL_MOVER_LOG` | `~/browser-visits-mover.log` | reset (mover writes via LaunchAgent stdout/stderr) |
| `BVL_DB_FILE` | `~/browser-visits.db` | host, mover, sealer, reset |
| `BVL_DOWNLOADS_SNAPSHOTS_DIR` | `~/Downloads/browser-visit-snapshots` | host, mover, reset |
| `BVL_ICLOUD_SNAPSHOTS_DIR` | `~/Documents/browser-visit-logger/snapshots` | host, mover, sealer |
| `BVL_MOVER_MIN_AGE_SECONDS` | `60` | mover |
| `BVL_MOVER_ERROR_THRESHOLD` | `3` | mover (consecutive failures before persistent-error notification) |

---

## Development

```bash
# Python tests (host, mover, sealer)
pip install -r requirements-test.txt
python3 -m pytest tests/ -v

# Coverage report
python3 -m pytest tests/ --cov=native-host --cov-report=term-missing

# JS tests (background, popup) with coverage
npm install
npm test -- --coverage
```

The full suite is 190 Python + 94 JS tests, with 100% line/branch
coverage on every shipped module.

### Project layout

```
browser-visit-logger/
├── extension/                              # Chrome MV3 extension
│   ├── background.js                       # Service worker
│   ├── manifest.json.template              # → manifest.json (built by install.sh)
│   ├── popup.html
│   └── popup.js
├── native-host/
│   ├── host.py                             # Native messaging host
│   ├── snapshot_mover.py                   # Periodic archiver (mover + seal)
│   ├── snapshot_sealer.py                  # Manual sealer CLI
│   ├── com.browser.visit.logger.json       # Host manifest (template-installed)
│   └── com.browser.visit.logger.snapshot_mover.plist.template
├── tests/                                  # Python (pytest) + JS (jest)
├── install.sh
├── reset.py
├── point-to-worktree.py
├── package.json                            # JS test deps + jest config
└── requirements-test.txt                   # Python test deps
```

---

## License

Personal project. No license declared — all rights reserved by the author.
