import Foundation

/// Recording / clearing of `mover_errors` rows.  Mirrors the Python
/// helpers in snapshot_mover.py.  The schema is owned by
/// ``Schema.ensureMoverErrorsTable``; callers in BVLHost / BVLVerifier
/// are responsible for ensuring the table exists before invoking these.
public enum MoverErrors {

    /// Operation kinds that warrant immediate notification (instead of
    /// waiting for the persistent-error threshold).  Same set the
    /// Python side classifies as "single-shot" or catastrophic.
    private static let immediateOps: Set<String> = [
        "top_level", "rewrite_manifest", "invalid_filename",
        "orphan_file", "manifest_invalid",
    ]

    /// True iff a (op, error) pair should escalate on the first occurrence.
    public static func isImmediate(op: String, error: Error?) -> Bool {
        if immediateOps.contains(op) { return true }
        if let nsErr = error as NSError?,
           nsErr.domain == NSPOSIXErrorDomain,
           [Int(POSIXError.ENOSPC.rawValue),
            Int(POSIXError.EROFS.rawValue),
            Int(POSIXError.EDQUOT.rawValue)].contains(nsErr.code) {
            return true
        }
        return false
    }

    /// UPSERT a mover_errors row for this failure.  On insert:
    /// attempts=1, first_seen=last_seen=now, notified=0.  On conflict:
    /// attempts incremented, last_seen + message refreshed,
    /// first_seen + notified preserved (one notification per streak).
    /// `immediate` is monotonically promoted (0 → 1, never 1 → 0).
    public static func record(
        _ db: Database, op: String, target: String, error: Error
    ) throws {
        let key = "\(op):\(target)"
        let now = isoNow()
        let message = String(tsvSanitise("\(error)").prefix(500))
        let immediate = isImmediate(op: op, error: error) ? 1 : 0
        try db.run("""
            INSERT INTO mover_errors
              (key, operation, target, message, first_seen, last_seen,
               attempts, immediate)
            VALUES (?, ?, ?, ?, ?, ?, 1, ?)
            ON CONFLICT(key) DO UPDATE SET
              attempts = attempts + 1,
              last_seen = excluded.last_seen,
              message = excluded.message,
              immediate = MAX(immediate, excluded.immediate)
            """,
            [key, op, target, message, now, now, immediate])
    }

    /// Best-effort wrapper around ``record`` for use inside per-op
    /// catch blocks: log on failure, never raise.
    public static func tryRecord(
        _ db: Database, op: String, target: String, error: Error,
        log: HostLog?
    ) {
        do {
            try record(db, op: op, target: target, error: error)
        } catch {
            log?.error("Could not record \(op) error for \(target): \(error)")
        }
    }

    /// DELETE the mover_errors row matching (op, target).  Idempotent —
    /// no-op if no row exists.
    public static func clear(
        _ db: Database, op: String, target: String
    ) throws {
        try db.run("DELETE FROM mover_errors WHERE key = ?",
                   ["\(op):\(target)"])
    }

    /// Best-effort wrapper around ``clear``.
    public static func tryClear(
        _ db: Database, op: String, target: String, log: HostLog?
    ) {
        do {
            try clear(db, op: op, target: target)
        } catch {
            log?.error("Could not clear \(op) error for \(target): \(error)")
        }
    }

    /// Current UTC time, ISO-8601 with second precision.  Indirected so
    /// tests can patch via dependency injection if needed.
    public static func isoNow(_ now: Date = Date()) -> String {
        let f = ISO8601DateFormatter()
        f.timeZone = TimeZone(identifier: "UTC")
        f.formatOptions = [.withInternetDateTime]
        return f.string(from: now)
    }
}
