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

Generates a reading list of every URL tagged **★ Of Interest** that
has not yet been **✓ Read**.  Output format defaults to **HTML**
(a self-contained file with inline CSS, openable directly in a
browser); pass `--format markdown` for Markdown.  Default output:

| Format | Default path |
|--------|--------------|
| html (default) | `~/Documents/browser-visit-logger/reading_list.html` |
| markdown       | `~/Documents/browser-visit-logger/reading_list.md` |

The list is split into two clickable tables:

| Table | URLs included |
|-------|---------------|
| Unread URLs that have been skimmed | of_interest = set, read = 0, skimmed > 0 |
| Unread URLs                         | of_interest = set, read = 0, skimmed = 0 |

Both tables are sorted by **first-visited timestamp, most recent
first**.  Date-time values are rendered in the user's **local time
zone** (the database stores UTC; the tool converts at format time).

URLs render as clickable links — the visible label is the page title
(falling back to the URL itself when title is empty).  Special
characters are escaped per format: HTML uses `html.escape` for both
the link label and the `href` value; Markdown escapes `|`, `[`, `]`
and percent-encodes parens / spaces in URLs.  Tabs / newlines in
titles collapse to spaces so rows stay on one line.

```bash
# Default — HTML to ~/Documents/browser-visit-logger/reading_list.html
./generate_reading_list

# Markdown instead (default path becomes reading_list.md)
./generate_reading_list --format markdown

# Override paths (useful for tests / experiments)
./generate_reading_list --db /tmp/test.db --output /tmp/reading_list.html

# Skip the wrapper (equivalent)
python3 reading_list.py
```

Flags:

| Flag | Effect |
|------|--------|
| `--format {html,markdown}` | Output format (default `html`) |
| `--db FILE` | Override `BVL_DB_FILE` (default `~/browser-visits.db`) |
| `--output FILE` | Override the default output path; takes precedence over the format-derived default |
| `-v`, `--verbose` | DEBUG log level |

The output file is overwritten on every run.  Parent directory is
created if missing.  Exit codes: `0` on success, `1` if the database
file is missing.

## Development

```bash
pip install -r requirements-test.txt
python3 -m pytest tests/ --cov=reading_list --cov-report=term-missing
```

45 tests, 100% line coverage.

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
