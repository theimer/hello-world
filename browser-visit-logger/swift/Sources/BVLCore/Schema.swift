import Foundation

/// SQL schema definitions.  The canonical DDL lives in the repo's
/// top-level `schema.sql` (used by Python and the browser-visit-tools
/// test suite); the statements below are compiled-in mirrors of
/// those, kept in sync by `tests/test_schema_parity.py`.  Both
/// implementations open the same `~/browser-visits.db` file and need
/// to agree column-for-column.
public enum Schema {

    /// Create / migrate the host-owned tables (visits, read_events,
    /// skimmed_events, snapshots).  Idempotent — uses
    /// `CREATE TABLE IF NOT EXISTS` for everything.
    ///
    /// The events tables' `directory` column DEFAULT embeds
    /// `Paths.downloadsSnapshotsDir` at table-creation time so ad-hoc
    /// inserts get a sensible value; `_insert_event` always supplies
    /// the column explicitly.
    public static func ensureHostTables(_ db: Database) throws {
        try db.execute("""
            CREATE TABLE IF NOT EXISTS visits (
                url         TEXT PRIMARY KEY,
                timestamp   TEXT NOT NULL,
                title       TEXT NOT NULL DEFAULT '',
                of_interest TEXT,
                read        INTEGER NOT NULL DEFAULT 0,
                skimmed     INTEGER NOT NULL DEFAULT 0
            )
        """)
        try db.execute("""
            CREATE INDEX IF NOT EXISTS idx_visits_timestamp ON visits(timestamp)
        """)
        try ensureEventsTable(db, name: "read_events")
        try ensureEventsTable(db, name: "skimmed_events")
        try ensureSnapshotsTable(db)
    }

    /// Create the snapshots table if absent.
    public static func ensureSnapshotsTable(_ db: Database) throws {
        try db.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                date   TEXT PRIMARY KEY,
                sealed INTEGER NOT NULL DEFAULT 0
            )
        """)
    }

    /// Create the mover_errors table if absent.
    public static func ensureMoverErrorsTable(_ db: Database) throws {
        try db.execute("""
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
            )
        """)
    }

    /// Create one of the two events tables (read_events / skimmed_events).
    private static func ensureEventsTable(_ db: Database, name: String) throws {
        // Embed Paths.downloadsSnapshotsDir as the default directory.
        // Single-quote the value so the SQL is well-formed even on home
        // dirs that contain quotes (very rare, but the Python side
        // handles it and we should match).
        let escaped = Paths.downloadsSnapshotsDir.replacingOccurrences(
            of: "'", with: "''")
        let defaultDirLit = "'\(escaped)'"
        try db.execute("""
            CREATE TABLE IF NOT EXISTS \(name) (
                url       TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                filename  TEXT NOT NULL DEFAULT '',
                directory TEXT NOT NULL DEFAULT \(defaultDirLit),
                PRIMARY KEY (url, timestamp)
            )
        """)
    }
}
