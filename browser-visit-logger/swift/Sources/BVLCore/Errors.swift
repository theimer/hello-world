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

    /// Per-op actionable hint surfaced in notifications and
    /// `--show-errors` output.  Same wording as Python's `_FIX_HINTS`.
    public static let fixHints: [String: String] = [
        "move":
            "Check that the iCloud destination is writable and has free "
            + "space, then wait for the next mover run.",
        "seal":
            "Check the iCloud destination, then wait for the next mover "
            + "run — the seal pass retries every tick until it succeeds.",
        "rewrite_manifest":
            "Run `snapshot_sealer.py <date>` to rebuild the manifest, "
            + "then `verify_snapshot_directory --clear-error N` to clear "
            + "this row.",
        "invalid_filename":
            "Rename the file to match "
            + "'<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>' or remove it; the "
            + "row clears on the next mover run.",
        "orphan_file":
            "Snapshot file has no matching events row.  Either delete "
            + "the file, or re-tag its URL via the popup to recreate "
            + "the row; either way clears on the next mover run.",
        "missing_directory":
            "Re-create the directory, OR remove the snapshots row "
            + "(`sqlite3 <db> \"DELETE FROM snapshots WHERE date='<YYYY-MM-DD>'\"`).",
        "top_level":
            "Check ~/browser-visits-verifier.log for the traceback, "
            + "then `verify_snapshot_directory --clear-errors` once the "
            + "bug is fixed.",
        "manifest_invalid":
            "Re-seal the directory: delete MANIFEST.tsv and run "
            + "`snapshot_sealer.py <date>`, then "
            + "`verify_snapshot_directory --clear-error N` to clear "
            + "this row.",
    ]

    /// One pending error row, returned by ``fetchPending``.
    public struct Pending {
        public let key: String
        public let operation: String
        public let target: String
        public let message: String
        public let attempts: Int
        public let firstSeen: String
        public let lastSeen: String
        public let notified: Bool
    }

    /// Return all rows from `mover_errors`, ordered by (first_seen, key)
    /// for stable indexing in the show-errors / clear-error CLI ops.
    public static func fetchPending(_ db: Database) throws -> [Pending] {
        try db.queryAll("""
            SELECT key, operation, target, message, attempts,
                   first_seen, last_seen, notified
            FROM mover_errors
            ORDER BY first_seen ASC, key ASC
            """, map: { row in
                Pending(
                    key: row.string(0), operation: row.string(1),
                    target: row.string(2), message: row.string(3),
                    attempts: row.int(4),
                    firstSeen: row.string(5), lastSeen: row.string(6),
                    notified: row.int(7) != 0)
            })
    }

    /// Walk currently-unresolved error rows and notify the user about
    /// ones that warrant it.  Best-effort — logs and returns on any
    /// failure.  A row escalates when notified=0 AND (immediate=1 OR
    /// attempts >= threshold).  After notifying, notified is flipped
    /// to 1 so the same row isn't re-surfaced.
    public static func escalate(_ db: Database, log: HostLog?) {
        let threshold = Paths.moverErrorThreshold
        let rows: [(key: String, op: String, target: String, message: String,
                    attempts: Int, firstSeen: String)]
        do {
            rows = try db.queryAll("""
                SELECT key, operation, target, message, attempts, first_seen
                FROM mover_errors
                WHERE notified = 0 AND (immediate = 1 OR attempts >= ?)
                ORDER BY first_seen ASC, key ASC
                """, [threshold], map: { row in
                    (row.string(0), row.string(1), row.string(2),
                     row.string(3), row.int(4), row.string(5))
                })
        } catch {
            log?.error("Could not query mover_errors during escalation: \(error)")
            return
        }
        for r in rows {
            let title = "Browser Visit Logger: mover error"
            var body: String
            if r.op == "top_level" {
                body = "Mover crashed: \(r.message)"
            } else {
                let target = r.target.isEmpty ? "(no target)" : r.target
                body = "\(r.op) failed \(r.attempts)× since \(r.firstSeen): "
                    + "\(target) — \(r.message)"
            }
            if let hint = fixHints[r.op] {
                body = "\(body)  Fix: \(hint)"
            }
            Notify.user(title: title, body: body)
            do {
                try db.run(
                    "UPDATE mover_errors SET notified = 1 WHERE key = ?",
                    [r.key])
            } catch {
                log?.error("Could not mark \(r.op) error notified: \(error)")
            }
        }
    }

    /// Auto-heal `op` rows whose target lives under `dirPath`.  Removes
    /// rows whose target is under `dirPath` but isn't in `currentStrays`
    /// — i.e. files the user has since renamed, removed, or otherwise
    /// resolved.  Used by the sweep pass with op='invalid_filename' on
    /// the Downloads dir, and by the seal pass with 'invalid_filename'
    /// / 'orphan_file' on each date subdir.
    public static func reconcileDirScoped(
        _ db: Database, op: String, dirPath: String,
        currentStrays: Set<String>, log: HostLog?
    ) {
        do {
            let prior = try db.queryAll(
                "SELECT key, target FROM mover_errors WHERE operation = ?",
                [op], map: { ($0.string(0), $0.string(1)) })
            // Use a trailing-separator prefix so a target under dirPath
            // only matches if it's actually under that directory.
            let prefix = (dirPath as NSString).appendingPathComponent("/")
                .trimmingCharacters(in: .init(charactersIn: "/")) + "/"
            for (key, target) in prior {
                if target.hasPrefix(prefix) && !currentStrays.contains(target) {
                    try db.run(
                        "DELETE FROM mover_errors WHERE key = ?", [key])
                }
            }
        } catch {
            log?.error("Could not reconcile \(op) errors under \(dirPath): \(error)")
        }
    }
}
