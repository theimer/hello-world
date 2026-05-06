# Browser Visit projects

Two related projects that share a local SQLite database of browser
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

- **`reading_list.py`** — generates a Markdown reading list of every
  URL tagged of_interest but not yet read, split into "Unread URLs
  that have been skimmed" and "Unread URLs" tables.

These tools depend only on the DB schema, not on logger code, so the
two directories can be developed and vendored independently.
