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

Chrome's downloads API forces every file under `~/Downloads`, so each
snapshot lands first in `~/Downloads/browser-visit-snapshots/`. The
native host (`host.py`) immediately relocates it to a non-Downloads
staging dir at
`~/Library/Application Support/browser-visit-logger/inbox/`. A
background mover (`snapshot_mover.py`) then archives staged files into
`~/Documents/browser-visit-logger/snapshots/<YYYY-MM-DD>/`, makes them
read-only, and (once the day has fully passed) writes a
`MANIFEST.tsv` summarising the directory.

The relocate hop exists because `~/Downloads` is TCC-protected on
macOS: the launchd-spawned mover cannot read it, but `host.py` (spawned
by Chrome via native messaging) inherits Chrome's TCC grant and can.
Each `host.py` invocation also sweeps any files left in the Downloads
dir by a prior crashed run, so stragglers self-heal on the next user
action.

---

## Storage layout

| Path | What it is |
|------|------------|
| `~/browser-visits-<YYYY-MM-DD>.log` | Per-day TSV append-only log of every visit and tag action.  One file per UTC day; the sealer collects each completed day's log into the matching iCloud sealed dir |
| `~/browser-visits.db` | SQLite database — `visits`, `read_events`, `skimmed_events`, `snapshots`, `mover_errors` |
| `~/browser-visits-host.log` | Native host process log (rotated, 1 MiB × 3) |
| `~/browser-visits-mover.log` | Snapshot mover process log (LaunchAgent stdout/stderr) |
| `~/browser-visits-verifier.log` | Snapshot verifier process log (LaunchAgent stdout/stderr) |
| `~/Downloads/browser-visit-snapshots/` | Chrome's drop point (host.py clears it on each invocation) |
| `~/Library/Application Support/browser-visit-logger/inbox/` | Snapshot staging dir (host.py moves files here from Downloads; mover reads from here) |
| `~/Documents/browser-visit-logger/snapshots/<YYYY-MM-DD>/` | Sealed daily archive: read-only snapshot files + read-only `MANIFEST.tsv` + read-only `browser-visits-<YYYY-MM-DD>.log` |

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
    directory TEXT NOT NULL DEFAULT '<staging dir>',
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
3. On macOS, installs and bootstraps two LaunchAgents:
   - `com.browser.visit.logger.snapshot_mover` (`StartInterval = 3600`s,
     hourly) — runs the move + seal pass.
   - `com.browser.visit.logger.snapshot_verifier` (`StartInterval =
     604800`s, weekly) — anti-entropy: re-checks every sealed
     directory's manifest against disk + DB; failures escalate
     immediately via Notification Center.
4. Prints the extension ID and where to load the unpacked extension.

After it finishes:

1. Open `chrome://extensions` and enable **Developer mode**.
2. **Load unpacked** → select the `extension/` directory.
3. The extension ID Chrome displays should match the one printed by
   the installer.
4. Visit any page, then verify with:
   ```bash
   tail ~/browser-visits-$(date -u +%Y-%m-%d).log
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

The five user-facing Python scripts each have an executable Bash
wrapper at the repo root for convenience — e.g. `./move_snapshot
--show-errors` instead of `python3 native-host/snapshot_mover.py
--show-errors`. Each wrapper forwards all arguments verbatim and
intercepts `--help` / `-h` to print a one-line wrapper note before
delegating to the Python script's own argparse `--help`.

| Wrapper | Underlying script |
|---------|-------------------|
| `./move_snapshot` | `native-host/snapshot_mover.py` |
| `./seal_snapshot_directory` | `native-host/snapshot_sealer.py` |
| `./verify_snapshot_directory` | `native-host/snapshot_verifier.py` |
| `./reset_visits_data` | `reset.py` |
| `./rebuild_visits_data` | `native-host/visits_rebuilder.py` |

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

1. **Move pass** — for every file in
   `~/Library/Application Support/browser-visit-logger/inbox/` matching
   the snapshot filename format and at least `MIN_AGE_SECONDS` old:
   copy it to `<dest>/<YYYY-MM-DD>/`, chmod read-only, update the
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
   creates `<dest>/<YYYY-MM-DD>/` if it doesn't yet exist (covers
   activity-only days where host wrote a per-day log but no snapshot
   files landed), writes a tab-delimited `MANIFEST.tsv` enumerating
   the directory's contents and chmods it `0o444`, **moves
   `<BVL_LOG_DIR>/browser-visits-<date>.log` into the dir and chmods
   it `0o444`**, then flips `sealed = 1`.  If the manifest write or
   log move fails, the row stays at `sealed=0` and the next tick
   retries.
3. **Orphan-log merge pass** — anti-entropy for the rare case where a
   host invocation in flight at seal time leaves a result line in
   `<BVL_LOG_DIR>` after the seal already moved its action half into
   iCloud.  Each tick the mover scans `BVL_LOG_DIR` for past-day
   `browser-visits-<date>.log` files; for any whose iCloud
   counterpart already exists, the orphan is appended into the iCloud
   copy (chmod 0o644 → append → chmod 0o444) and the orphan is
   deleted.  For any whose iCloud counterpart doesn't exist (host
   crashed mid-startup before it could insert the snapshots row),
   `INSERT OR IGNORE INTO snapshots (date, sealed=0)` so the next
   normal seal pass picks it up.

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
the auto seal pass won't re-process it.  When a per-day log file
exists in `BVL_LOG_DIR` for that date, it's moved into the sealed dir
(chmod `0o444`) as part of the seal — same flow as the auto sealer.
The orphan-log merge pass also runs after the manual seal, so
ad-hoc seals clean up any race orphans in `BVL_LOG_DIR`.
Non-date-named directories get a manifest but no table row and no log.

**Manifest format** — `MANIFEST.tsv`, one header row + one row per file:

```
filename	tag	timestamp	url	title
2026-04-30T10-00-00Z-abc.mhtml	read	2026-04-30T10:00:00Z	https://example.com	Example Page
```

Files in the directory with no matching DB row appear with empty
metadata fields. Tabs / newlines / CRs in titles are sanitised to
spaces.

### `native-host/snapshot_verifier.py`

Checks that a sealed daily directory's `MANIFEST.tsv` is correct and
consistent with the database. Designed to be invoked from either a
terminal (ad-hoc auditing) or a background process (a periodic
LaunchAgent / cron job).

```bash
# Verify by date (resolved under ICLOUD_SNAPSHOTS_DIR)
python3 native-host/snapshot_verifier.py 2026-04-30

# Verify by absolute or relative path
python3 native-host/snapshot_verifier.py /Users/me/.../snapshots/2026-04-30

# Verify every sealed directory in the snapshots table
python3 native-host/snapshot_verifier.py --all

# Background mode: silent on success, record failures into mover_errors
# so they surface via the standard notification pipeline
python3 native-host/snapshot_verifier.py --quiet --record --all
```

Flags:

| Flag | Effect |
|------|--------|
| (positional `directory`) | Date or path to verify |
| `--all` | Verify every `sealed=1` row in the `snapshots` table (mutually exclusive with the positional) |
| `--quiet` | Print only failure summaries — suitable for background invocation |
| `--record` | UPSERT each failure into `mover_errors` as op `manifest_invalid` (and clear the row on subsequent success) |
| `-v`, `--verbose` | DEBUG log level |
| `--dest DIR` | Override `BVL_ICLOUD_SNAPSHOTS_DIR` |
| `--db FILE` | Override `BVL_DB_FILE` |

Checks performed against each target directory:

1. `MANIFEST.tsv` exists.
2. Manifest is read-only (mode `0o444`).
3. First line is the canonical header.
4. Every data row has exactly 5 tab-delimited columns; no duplicate filenames.
5. **Every file in the directory** (other than `MANIFEST.tsv` itself)
   has a conforming snapshot filename — non-conforming files are
   flagged whether or not they appear in the manifest.
6. The set of conforming snapshot files in the directory equals the
   set of filenames listed in the manifest.
7. **Every manifest row has a corresponding events row in the DB**, and
   its `(tag, timestamp, url, title)` matches. Orphan rows (manifest
   entries with no DB backing) are always invalid.
8. **Every conforming file in the directory has an events row in the
   DB.** Orphan files are always invalid, even when correctly excluded
   from the manifest.

Exit codes: `0` if every directory verified passes; `1` if any
directory fails verification, if the target doesn't exist, or on
argument errors.

**Anti-entropy schedule (macOS, installed by `install.sh`)**

`install.sh` registers a second LaunchAgent —
`com.browser.visit.logger.snapshot_verifier` — that invokes
`snapshot_verifier.py --quiet --record --all` on a configurable
`StartInterval` (default `604800` seconds = **1 week**).  Each tick:

- Audits every `sealed=1` row in the `snapshots` table.
- Records failures into `mover_errors` as op `manifest_invalid`
  (immediate-class, so notification fires on first occurrence).
- Clears `manifest_invalid` rows for directories that re-pass.
- Drains the notification queue via `_escalate_errors`, so any
  finding (and any other unread mover_errors row past its
  threshold) reaches the user immediately — no waiting for the
  next mover tick.

Logs land in `~/browser-visits-verifier.log`.

To change the cadence, edit `StartInterval` in
`~/Library/LaunchAgents/com.browser.visit.logger.snapshot_verifier.plist`
and reload:

```bash
launchctl bootout   gui/$(id -u)/com.browser.visit.logger.snapshot_verifier
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.browser.visit.logger.snapshot_verifier.plist
```

Common values: `86400` (daily), `604800` (weekly, default),
`2592000` (~monthly).  To audit on demand without waiting for the
next tick, just run the script directly with `--all`.

### `reset.py`

Wipes local data the extension produced. Asks for confirmation by default.

```bash
# Reset everything (per-day visit logs, host log, mover log, DB, both snapshot dirs)
python3 reset.py

# Skip confirmation
python3 reset.py -f

# Reset one thing only
python3 reset.py --log         # all ~/browser-visits-<date>.log files in BVL_LOG_DIR
python3 reset.py --host-log    # host log + mover log
python3 reset.py --db          # ~/browser-visits.db
python3 reset.py --snapshots   # staging dir + ~/Downloads/browser-visit-snapshots/
python3 reset.py --icloud      # ~/Documents/browser-visit-logger/  (also wipes per-day logs sealed in iCloud)
```

`--log` only deletes per-day logs that still live in `BVL_LOG_DIR`.
Per-day logs that have already been moved into iCloud sealed dirs are
wiped by `--icloud` — same separation between log and snapshot data.

Respects the same `BVL_*` env vars as the host, so custom paths Just
Work. Safe to run with no targets present — missing files/dirs are
reported and skipped.

### `native-host/visits_rebuilder.py`

Reconstructs `~/browser-visits.db` from two side-channels that survive
DB loss: the per-day on-disk logs (`~/browser-visits-<date>.log` plus
each sealed iCloud subdir's bundled log) and the iCloud snapshot
archive.  Two phases run by default:

1. **Log replay** — every host invocation writes a UUID-prefixed
   action line followed by a result line, pinned to the day the
   invocation started.  The rebuilder enumerates per-day logs from
   `BVL_LOG_DIR` *and* every sealed iCloud subdir, sorts them
   chronologically, and pairs action+result UUIDs across files (so
   the rare cross-day seal-race orphan is handled correctly).
   Successful pairs are re-applied via the same `host.py` helpers
   used at write-time; error pairs are skipped (the DB write didn't
   happen); orphan / malformed lines are counted and trip a non-zero
   exit.  Each replayed log file's date also produces a `snapshots`
   row (mirrors the live INSERT host.py does).
2. **Filesystem rehydration** — iterates every `YYYY-MM-DD`
   subdirectory under the iCloud root, upserts a `snapshots` row
   (`sealed = 1` if `MANIFEST.tsv` exists), and updates each event
   row's `directory` column from the staging dir to the date subdir
   for files that have already been moved.

```bash
# Rebuild against the configured paths (DROPs and recreates
# visits / read_events / skimmed_events / snapshots first)
./rebuild_visits_data

# Skip the wipe; rely on idempotency
./rebuild_visits_data --no-truncate

# Phase 1 only (e.g. logs present, iCloud unreachable)
./rebuild_visits_data --log-only

# Phase 2 only (logs lost, iCloud archive intact)
./rebuild_visits_data --rehydrate-only

# Operate on isolated paths (useful in tests / experiments)
./rebuild_visits_data \
    --log-dir /tmp/logs --db /tmp/test.db \
    --source /tmp/dl --dest /tmp/icloud
```

Flags:

| Flag | Effect |
|------|--------|
| `--truncate` (default) / `--no-truncate` | DROP and recreate the four rebuildable tables before phase 1.  `mover_errors` is left alone in either case. |
| `--log-only` / `--rehydrate-only` | Skip phase 2 / phase 1 (mutually exclusive). |
| `--log-dir DIR` | Override `BVL_LOG_DIR` (the per-day logs root) |
| `--db FILE` | Override `BVL_DB_FILE` |
| `--source DIR` | Override `BVL_DOWNLOADS_SNAPSHOTS_DIR` |
| `--dest DIR` | Override `BVL_ICLOUD_SNAPSHOTS_DIR` |
| `-v`, `--verbose` | DEBUG log level |

`mover_errors` is intentionally not log-recoverable; the rebuild
leaves it alone.  Filesystem-derived rows (`orphan_file`,
`invalid_filename`, `missing_directory`, `manifest_invalid`)
repopulate naturally on the next mover/sealer/verifier pass.

Exit codes: 0 on success, 1 on input errors (missing log/DB path,
orphan or malformed lines skipped during phase 1), 2 on an
unexpected exception.  See [docs/rebuild-visits-from-log.md](docs/rebuild-visits-from-log.md)
for the full design.

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
  Used for ops whose natural retry loop re-attempts the same target
  every tick:
  - `move` — `_move_one` failure (copy/chmod/UPDATE/INSERT/unlink).
  - `seal` — `_seal_directory` failure (manifest write/chmod/DB update).
  - `missing_directory` — the `snapshots` table has a row for a date
    whose on-disk directory has been deleted.  The row auto-clears
    if the user re-creates the directory.
- **Immediate failure**: notified on the first occurrence regardless
  of attempts.  Used for ops whose retry loop won't re-encounter the
  same target on subsequent ticks (so threshold-based escalation
  would never fire), and for catastrophic conditions:
  - `top_level` — uncaught exception in `main()`.
  - `rewrite_manifest` — straggler-rewrite failure (only triggered by
    a fresh straggler arriving in the same dir).
  - `manifest_invalid` — `snapshot_verifier.py --record` found a
    sealed directory whose `MANIFEST.tsv` failed one or more checks.
    Auto-clears on the next successful verification.
  - `invalid_filename` — a file in the staging dir or in a daily
    snapshot directory has a name that doesn't match the canonical
    `<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>` format.  The staging file is
    left in place; date-dir files are excluded from the manifest.  The
    row auto-clears when the user removes or renames the file.
  - `orphan_file` — a conforming-named snapshot file in a daily
    directory has no matching `read_events` / `skimmed_events` row.
    The file is excluded from the manifest.  The row auto-clears when
    the user removes the file or re-tags its URL via the popup so an
    events row is recorded.
  - `OSError` with errno in `{ENOSPC, EROFS, EDQUOT}` — disk full,
    read-only filesystem, or quota exceeded.
  - `sqlite3.DatabaseError` other than `OperationalError` — typically
    means the DB file is corrupt.

Both notification banner bodies and `--show-errors` rows include a
per-op `Fix:` hint pointing at the user action that resolves the
problem (e.g. "Rename the file to match …", "Run `snapshot_sealer.py
<date>` to rebuild the manifest …").

A row stays in the table until the underlying problem is resolved.
Three paths clear it:

1. **Automatic** — the next successful run of the same operation
   `_clear_error`s the row. For `invalid_filename` and
   `missing_directory`, a directory-scoped reconcile additionally
   clears rows whose target file/dir no longer exists. Most users
   never need to touch the table.
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
| `BVL_LOG_DIR` | `~` | host, mover (orphan-merge + log move during seal), sealer, rebuilder, reset.  The directory holding per-day `browser-visits-<UTC-date>.log` files. |
| `BVL_HOST_LOG` | `~/browser-visits-host.log` | host, reset |
| `BVL_MOVER_LOG` | `~/browser-visits-mover.log` | reset (mover writes via LaunchAgent stdout/stderr) |
| `BVL_VERIFIER_LOG` | `~/browser-visits-verifier.log` | reset (verifier writes via LaunchAgent stdout/stderr) |
| `BVL_DB_FILE` | `~/browser-visits.db` | host, mover, sealer, rebuilder, reset |
| `BVL_DOWNLOADS_SNAPSHOTS_DIR` | `~/Downloads/browser-visit-snapshots` | host (relocate source), reset.  Where Chrome drops snapshots before host.py moves them to the staging dir. |
| `BVL_STAGING_SNAPSHOTS_DIR` | `~/Library/Application Support/browser-visit-logger/inbox` | host (relocate dest, events row directory), mover (read source), rebuilder, reset |
| `BVL_ICLOUD_SNAPSHOTS_DIR` | `~/Documents/browser-visit-logger/snapshots` | host, mover, sealer, rebuilder |
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
│   ├── snapshot_verifier.py                # Manifest verifier CLI
│   ├── visits_rebuilder.py                 # DB rebuilder (log replay + FS rehydrate)
│   ├── com.browser.visit.logger.json       # Host manifest (template-installed)
│   ├── com.browser.visit.logger.snapshot_mover.plist.template
│   └── com.browser.visit.logger.snapshot_verifier.plist.template
├── tests/                                  # Python (pytest) + JS (jest)
├── install.sh
├── reset.py
├── point-to-worktree.py
├── move_snapshot                           # Bash wrapper → snapshot_mover.py
├── seal_snapshot_directory                 # Bash wrapper → snapshot_sealer.py
├── verify_snapshot_directory               # Bash wrapper → snapshot_verifier.py
├── reset_visits_data                       # Bash wrapper → reset.py
├── rebuild_visits_data                     # Bash wrapper → visits_rebuilder.py
├── package.json                            # JS test deps + jest config
└── requirements-test.txt                   # Python test deps
```

---

## License

Personal project. No license declared — all rights reserved by the author.
