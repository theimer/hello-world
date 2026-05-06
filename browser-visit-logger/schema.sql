-- Canonical SQL schema for ~/browser-visits.db.
--
-- This file is the single source of truth for DDL.  Three places consume it:
--
--   1. browser-visit-logger/native-host/host.py      (Python rebuilder)
--   2. browser-visit-logger/native-host/snapshot_mover.py
--                                                    (Python sealer helpers)
--   3. browser-visit-tools/tests/test_reading_list.py
--                                                    (test schema for tools)
--
-- The Swift production writer (swift/Sources/BVLCore/Schema.swift) duplicates
-- these statements in compiled code; tests/test_schema_parity.py asserts the
-- two stay in sync.
--
-- Sentinel: '__BVL_DOWNLOADS_SNAPSHOTS_DIR__' is replaced with the absolute
-- BVL_DOWNLOADS_SNAPSHOTS_DIR path at load time so ad-hoc INSERTs without a
-- directory column get a sensible value (production code always supplies it
-- explicitly).

CREATE TABLE IF NOT EXISTS visits (
    url         TEXT PRIMARY KEY,
    timestamp   TEXT NOT NULL,
    title       TEXT NOT NULL DEFAULT '',
    of_interest TEXT,
    read        INTEGER NOT NULL DEFAULT 0,
    skimmed     INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_visits_timestamp ON visits(timestamp);

CREATE TABLE IF NOT EXISTS read_events (
    url       TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    filename  TEXT NOT NULL DEFAULT '',
    directory TEXT NOT NULL DEFAULT '__BVL_DOWNLOADS_SNAPSHOTS_DIR__',
    PRIMARY KEY (url, timestamp)
);

CREATE TABLE IF NOT EXISTS skimmed_events (
    url       TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    filename  TEXT NOT NULL DEFAULT '',
    directory TEXT NOT NULL DEFAULT '__BVL_DOWNLOADS_SNAPSHOTS_DIR__',
    PRIMARY KEY (url, timestamp)
);

CREATE TABLE IF NOT EXISTS snapshots (
    date   TEXT PRIMARY KEY,
    sealed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mover_errors (
    key        TEXT PRIMARY KEY,
    operation  TEXT NOT NULL,
    target     TEXT NOT NULL,
    message    TEXT NOT NULL,
    first_seen TEXT NOT NULL,
    last_seen  TEXT NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 1,
    notified   INTEGER NOT NULL DEFAULT 0,
    immediate  INTEGER NOT NULL DEFAULT 0
);
