import Foundation

/// Per-event row data returned by ``EventsTable.fetch``.
public struct EventRow: Equatable {
    public let timestamp: String
    public let filename: String
    public let directory: String
}

/// One of the two events tables (read_events / skimmed_events).  Used
/// internally by the typed helpers below to avoid string-interpolating
/// table names into SQL.  Only `tagFor`/`columnFor` produce trusted
/// constants; user input never reaches these.
public enum EventsTable: String {
    case read    = "read_events"
    case skimmed = "skimmed_events"

    /// Tag string corresponding to this table — "read" or "skimmed".
    public var tag: String {
        switch self {
        case .read:    return "read"
        case .skimmed: return "skimmed"
        }
    }

    /// Visits-table counter column name.
    var visitsCounter: String {
        switch self {
        case .read:    return "read"
        case .skimmed: return "skimmed"
        }
    }

    /// Map a tag string ("read" / "skimmed") to its EventsTable.
    public static func forTag(_ tag: String) -> EventsTable? {
        switch tag {
        case "read":    return .read
        case "skimmed": return .skimmed
        default:        return nil
        }
    }
}

/// Helpers for the visits + events table writes.  Mirrors host.py's
/// `insert_visit`, `tag_visit`, `_insert_event`, `query_visit`.
public enum Events {

    /// Insert a new visit row; silently ignored if the URL already exists.
    public static func insertVisit(
        _ db: Database, timestamp: String, url: String, title: String
    ) throws {
        try db.run("""
            INSERT OR IGNORE INTO visits (url, timestamp, title)
            VALUES (?, ?, ?)
            """, [url, timestamp, title])
    }

    /// Apply a tag to an existing visit row.  Returns true iff the URL
    /// existed and was updated.  For "read"/"skimmed" tags, inserts a
    /// row in the corresponding events table (basename + Downloads dir
    /// directory) and bumps the visits counter.  For "of_interest",
    /// flips the visits.of_interest column.
    @discardableResult
    public static func tagVisit(
        _ db: Database, url: String, tag: String, timestamp: String,
        filename: String = ""
    ) throws -> Bool {
        if tag == "of_interest" {
            let changed = try db.run(
                "UPDATE visits SET of_interest = 1 WHERE url = ?",
                [url])
            return changed > 0
        }
        guard let table = EventsTable.forTag(tag) else {
            return false
        }
        return try insertEvent(
            db, table: table, url: url, timestamp: timestamp,
            filename: filename)
    }

    /// Insert a row into one of the events tables and increment the
    /// matching visits counter.  Returns true iff the visits row
    /// exists for `url` (event row inserted, or a duplicate row was
    /// dropped by INSERT OR IGNORE).
    @discardableResult
    public static func insertEvent(
        _ db: Database, table: EventsTable, url: String, timestamp: String,
        filename: String
    ) throws -> Bool {
        let exists = try db.queryOne(
            "SELECT 1 FROM visits WHERE url = ?", [url],
            map: { _ in true }) ?? false
        if !exists { return false }
        let basename = (filename as NSString).lastPathComponent
        let inserted = try db.run("""
            INSERT OR IGNORE INTO \(table.rawValue)
              (url, timestamp, filename, directory)
            VALUES (?, ?, ?, ?)
            """, [url, timestamp, basename, Paths.downloadsSnapshotsDir])
        if inserted > 0 {
            try db.run("""
                UPDATE visits SET \(table.visitsCounter)
                = \(table.visitsCounter) + 1 WHERE url = ?
                """, [url])
        }
        return true
    }

    /// Fetch all events for a URL from one of the events tables,
    /// sorted ascending by timestamp.
    public static func fetchEvents(
        _ db: Database, table: EventsTable, url: String
    ) throws -> [EventRow] {
        try db.queryAll("""
            SELECT timestamp, filename, directory FROM \(table.rawValue)
            WHERE url = ? ORDER BY timestamp ASC
            """, [url], map: { row in
                EventRow(timestamp: row.string(0),
                         filename: row.string(1),
                         directory: row.string(2))
            })
    }
}

/// One visit's full record, returned by ``Visits.query``.
public struct VisitRecord {
    public let timestamp: String
    public let title: String
    public let ofInterest: Bool
    public let read: [EventRow]
    public let skimmed: [EventRow]
}

/// Visits-table queries.  Mirrors host.py's `query_visit`.
public enum Visits {
    public static func query(_ db: Database, url: String) throws -> VisitRecord? {
        let basics = try db.queryOne("""
            SELECT timestamp, title, of_interest FROM visits WHERE url = ?
            """, [url], map: { row in
                (row.string(0), row.string(1), row.optionalString(2))
            })
        guard let basics = basics else { return nil }
        let read    = try Events.fetchEvents(db, table: .read,    url: url)
        let skimmed = try Events.fetchEvents(db, table: .skimmed, url: url)
        return VisitRecord(
            timestamp: basics.0, title: basics.1,
            ofInterest: basics.2 != nil,
            read: read, skimmed: skimmed)
    }
}
