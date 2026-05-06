# Browser Visit Tools

Standalone command-line tools that consume the SQLite database produced
by the sibling [`browser-visit-logger/`](../browser-visit-logger/)
project.  Read-only consumers — they never write back to the database.

These tools depend only on the DB schema (`visits`, `read_events`,
`skimmed_events`).  No Python imports cross the directory boundary,
so this project can be vendored or copied without dragging the logger
along.

## CLI scripts

All scripts live at the project root.  They share the same `BVL_*`
env-var conventions as `browser-visit-logger` (currently just
`BVL_DB_FILE`) and accept overriding flags so they're safe to point
at test data.

Each shell wrapper delegates to a Python script.  The wrapper forwards
all arguments verbatim and intercepts `--help` / `-h` to print a
one-line wrapper note before delegating.

| Wrapper | Underlying tool |
|---------|------------------|
| `./generate_reading_list` | `reading_list.py` (Python) |

### `generate_reading_list`

Generates a Markdown reading list at
`~/Documents/browser-visit-logger/reading_list.md` containing every URL
tagged **★ Of Interest** that has not yet been **✓ Read**.  The list is
split into two clickable tables:

| Table | URLs included | Sort |
|-------|---------------|------|
| Unread URLs that have been skimmed | of_interest = set, read = 0, skimmed > 0 | most-recent skimmed first |
| Unread URLs                         | of_interest = set, read = 0, skimmed = 0 | most-recent first-visit first |

URLs render as Markdown links — the visible label is the page title
(falling back to the URL itself when title is empty).  Pipe / bracket
characters in titles are escaped, parens / spaces in URLs are
percent-encoded, and tabs / newlines collapse to spaces so each row
stays on a single line.

```bash
# Default — read ~/browser-visits.db, write to
# ~/Documents/browser-visit-logger/reading_list.md
./generate_reading_list

# Override paths (useful for tests / experiments)
./generate_reading_list --db /tmp/test.db --output /tmp/reading_list.md

# Skip the wrapper (equivalent)
python3 reading_list.py
```

Flags:

| Flag | Effect |
|------|--------|
| `--db FILE` | Override `BVL_DB_FILE` (default `~/browser-visits.db`) |
| `--output FILE` | Override the default output path |
| `-v`, `--verbose` | DEBUG log level |

The output file is overwritten on every run.  Parent directory is
created if missing.  Exit codes: `0` on success, `1` if the database
file is missing.

## Development

```bash
pip install -r requirements-test.txt
python3 -m pytest tests/ --cov=reading_list --cov-report=term-missing
```

23 tests, 100% line coverage.

## Project layout

```
browser-visit-tools/
├── reading_list.py          # generate the markdown reading list
├── generate_reading_list    # bash wrapper → reading_list.py
├── tests/
│   ├── conftest.py
│   └── test_reading_list.py
├── requirements-test.txt
└── README.md
```
