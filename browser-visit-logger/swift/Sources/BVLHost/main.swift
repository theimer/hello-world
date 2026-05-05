// BVLHost — Browser Visit Logger native messaging host.
//
// Replaces native-host/host.py.  The bundle's Contents/MacOS/<name> is
// this binary directly (Mach-O), so when Chrome's native-messaging
// subsystem exec's it, the running process IS the bundle's signed
// executable and TCC attributes file accesses to the bundle's
// identity.  The previous shell-script entrypoint exec'd through
// /bin/bash → env → python3 and lost the bundle's TCC attribution by
// the time host.py was actually running.
//
// Per-invocation flow (matches host.py exactly):
//   1. Read one native message from stdin.
//   2. Write a per-day log "action line" describing the request,
//      pinned to today's UTC date so the matching result line lands
//      in the same file even across midnight UTC.
//   3. Open / create the SQLite DB; run the CREATE TABLE IF NOT EXISTS
//      statements idempotently.
//   4. Dispatch on the action / tag:
//        action=query  → look up the visit and return the record.
//        no action    → INSERT OR IGNORE the visits row, then apply
//                        the tag (of_interest / read / skimmed).  For
//                        read+skimmed with a filename, synchronously
//                        archive the just-downloaded snapshot from
//                        Downloads to its iCloud date subdir.
//   5. Append the matching result line to the per-day log.
//   6. Write a JSON response to stdout and exit.

import BVLCore
import Foundation

let log = HostLog(path: Paths.hostLog)

// MARK: - Helpers

func writeError(_ message: String) {
    do {
        try NativeMessaging.write(["status": "error", "message": message])
    } catch {
        // Pipe broke; nothing actionable.
    }
}

func writeError(_ errors: [String]) {
    do {
        try NativeMessaging.write(["status": "error", "errors": errors])
    } catch {
        // Pipe broke; nothing actionable.
    }
}

func writeOK(_ extra: [String: Any] = [:]) {
    var payload: [String: Any] = ["status": "ok"]
    for (k, v) in extra { payload[k] = v }
    do {
        try NativeMessaging.write(payload)
    } catch {
        // Pipe broke; nothing actionable.
    }
}

func recordToDict(_ record: VisitRecord) -> [String: Any] {
    func eventList(_ events: [EventRow]) -> [[String: Any]] {
        events.map { [
            "timestamp": $0.timestamp,
            "filename": $0.filename,
            "directory": $0.directory,
        ] }
    }
    return [
        "timestamp": record.timestamp,
        "title": record.title,
        "of_interest": record.ofInterest ? true : NSNull(),
        "read":    eventList(record.read),
        "skimmed": eventList(record.skimmed),
    ]
}

let validTags: Set<String> = ["of_interest", "read", "skimmed"]

// MARK: - Main

let message: [String: Any]
do {
    message = try NativeMessaging.read()
    log.debug("Received: \(message)")
} catch {
    log.error("Failed to read message: \(error)")
    writeError("\(error)")
    exit(0)
}

let recordId = UUID().uuidString.replacingOccurrences(of: "-", with: "").lowercased()
let todayISO = todayUTCString()

let url = (message["url"] as? String ?? "").trimmingCharacters(in: .whitespaces)
let action = (message["action"] as? String ?? "").trimmingCharacters(in: .whitespaces)

// MARK: query
if action == "query" {
    if url.isEmpty {
        writeError("url is required")
        exit(0)
    }
    do {
        let db = try Database(path: Paths.dbFile)
        try Schema.ensureHostTables(db)
        let record = try Visits.query(db, url: url)
        let payload: [String: Any] = record.map {
            ["status": "ok", "record": recordToDict($0)]
        } ?? ["status": "ok", "record": NSNull()]
        try NativeMessaging.write(payload)
    } catch {
        log.error("SQLite query failed: \(error)")
        writeError("\(error)")
    }
    exit(0)
}

// MARK: write paths
let timestamp = (message["timestamp"] as? String ?? "").trimmingCharacters(in: .whitespaces)
let title = message["title"] as? String ?? ""
let tag = (message["tag"] as? String ?? "").trimmingCharacters(in: .whitespaces)
let filename = (message["filename"] as? String ?? "").trimmingCharacters(in: .whitespaces)

if url.isEmpty {
    writeError("url is required")
    exit(0)
}
if timestamp.isEmpty {
    writeError("timestamp is required")
    exit(0)
}
if !tag.isEmpty && !validTags.contains(tag) {
    writeError("invalid tag: \(tag)")
    exit(0)
}

var errors: [String] = []

// First write: record the intended action.
do {
    try VisitLog.appendAction(
        recordId: recordId, dateISO: todayISO,
        timestamp: timestamp, url: url, title: title,
        tag: tag, filename: filename)
} catch {
    log.error("Log file write failed: \(error)")
    errors.append("log: \(error)")
}

// DB writes + synchronous archive for read/skimmed tags.
do {
    let db = try Database(path: Paths.dbFile)
    try Schema.ensureHostTables(db)
    try Schema.ensureMoverErrorsTable(db)
    try db.run("""
        INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, 0)
        """, [todayISO])
    try Events.insertVisit(db, timestamp: timestamp, url: url, title: title)
    if !tag.isEmpty {
        try Events.tagVisit(db, url: url, tag: tag, timestamp: timestamp,
                            filename: filename)
        if (tag == "read" || tag == "skimmed") && !filename.isEmpty {
            Archive.forTag(db, filename: filename, log: log)
        }
    }
} catch {
    log.error("SQLite write failed: \(error)")
    errors.append("db: \(error)")
}

// Second write: record the result.
let resultLine = errors.isEmpty
    ? "success"
    : "error: " + errors.joined(separator: "; ")
do {
    try VisitLog.appendResult(
        recordId: recordId, dateISO: todayISO, result: resultLine)
} catch {
    log.error("Log file result write failed: \(error)")
}

// Response.
if errors.isEmpty {
    writeOK()
} else {
    writeError(errors)
}
exit(0)
