import Foundation

/// Filename of the per-directory manifest written by the seal pass.
public let manifestFilename = "MANIFEST.tsv"

/// Header columns of the manifest (must stay in sync with
/// ``Seal.buildManifestRows``).
public let manifestHeader: [String] = [
    "filename", "tag", "timestamp", "url", "title",
]

/// Matches the daily snapshot subdir name (UTC date, ISO format).
private let dateDirRegex: NSRegularExpression = {
    // swiftlint:disable:next force_try
    try! NSRegularExpression(pattern: #"^\d{4}-\d{2}-\d{2}$"#)
}()

/// Returns true iff `basename` is in YYYY-MM-DD form.
public func isDateDirName(_ basename: String) -> Bool {
    let range = NSRange(basename.startIndex..<basename.endIndex, in: basename)
    return dateDirRegex.firstMatch(in: basename, range: range) != nil
}

/// Seal pass — write a read-only MANIFEST.tsv into each daily snapshot
/// directory whose UTC date is past, then flip its sealed flag to 1.
/// Mirrors snapshot_mover.py's `_seal_pass`.
public enum Seal {

    /// Run the seal pass.  DB-driven: queries
    /// `snapshots WHERE sealed=0 AND date<today_utc` and processes
    /// each row.  Errors are logged and recorded but never propagate.
    public static func pass(
        _ db: Database, dryRun: Bool = false, log: HostLog?
    ) {
        let today = todayUTCString()
        let dates: [String]
        do {
            dates = try db.queryAll("""
                SELECT date FROM snapshots
                WHERE sealed = 0 AND date < ?
                ORDER BY date
                """, [today], map: { $0.string(0) })
        } catch {
            log?.error("Seal: could not query snapshots table: \(error)")
            return
        }
        for date in dates {
            let subdir = (Paths.icloudSnapshotsDir as NSString)
                .appendingPathComponent(date)
            // The seal pass tolerates a missing date subdir — covers
            // activity-only days (host-inserted snapshots row, no
            // file ever landed for the date).
            if !FileManager.default.fileExists(atPath: subdir) {
                if dryRun {
                    log?.info("[dry-run] would create \(subdir) for "
                              + "activity-only day")
                } else {
                    do {
                        try FileManager.default.createDirectory(
                            atPath: subdir,
                            withIntermediateDirectories: true)
                    } catch {
                        log?.error("Failed to create \(subdir): \(error)")
                        MoverErrors.tryRecord(
                            db, op: "seal", target: subdir,
                            error: error, log: log)
                        continue
                    }
                }
            }
            MoverErrors.tryClear(
                db, op: "missing_directory", target: subdir, log: log)
            sealDirectory(
                db, dateSubdir: subdir, dryRun: dryRun,
                dateKey: date, log: log)
        }
    }

    /// Seal one directory: write MANIFEST.tsv, move the per-day log
    /// into the directory, flip the snapshots row to sealed=1.
    /// `dateKey`, when non-nil, is the YYYY-MM-DD primary key whose
    /// row gets flipped.
    public static func sealDirectory(
        _ db: Database, dateSubdir: String, dryRun: Bool,
        dateKey: String?, log: HostLog?
    ) {
        if dryRun {
            log?.info("[dry-run] would seal \(dateSubdir)")
            return
        }
        do {
            let count = try writeManifestFile(
                db, dateSubdir: dateSubdir, log: log)
            // Move the day's log file into the sealed dir if one
            // exists in LOG_DIR.  Done before flipping sealed=1 so a
            // failure leaves the row at sealed=0 and the next tick
            // retries.
            if let key = dateKey {
                try moveLogIntoSealedDir(dateSubdir: dateSubdir, dateISO: key)
            }
            log?.info("Sealed \(dateSubdir) (\(count) entries)")
            if let key = dateKey {
                try db.run("""
                    INSERT INTO snapshots (date, sealed) VALUES (?, 1)
                    ON CONFLICT(date) DO UPDATE SET sealed = 1
                    """, [key])
            }
            MoverErrors.tryClear(
                db, op: "seal", target: dateSubdir, log: log)
        } catch {
            log?.error("Failed to seal \(dateSubdir): \(error)")
            MoverErrors.tryRecord(
                db, op: "seal", target: dateSubdir, error: error, log: log)
        }
    }

    /// Build, write, and chmod 0o444 the manifest in `dateSubdir`.
    /// Returns the number of data rows written (excluding the header).
    /// Removes any existing manifest first so we can overwrite a
    /// previously-read-only file.  Caller is responsible for catching
    /// errors.
    @discardableResult
    public static func writeManifestFile(
        _ db: Database, dateSubdir: String, log: HostLog?
    ) throws -> Int {
        let path = (dateSubdir as NSString)
            .appendingPathComponent(manifestFilename)
        if FileManager.default.fileExists(atPath: path) {
            // Make writable so we can unlink even if mode 0o444.
            try? FileManager.default.setAttributes(
                [.posixPermissions: 0o644], ofItemAtPath: path)
            try FileManager.default.removeItem(atPath: path)
        }
        let rows = try buildManifestRows(
            db, dateSubdir: dateSubdir, log: log)
        var body = manifestHeader.joined(separator: "\t") + "\n"
        for row in rows {
            body += row.joined(separator: "\t") + "\n"
        }
        try body.write(toFile: path, atomically: true, encoding: .utf8)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o444], ofItemAtPath: path)
        return rows.count
    }

    /// Build per-file rows for the manifest.  Excludes
    /// non-conforming filenames and files with no matching events row
    /// (orphans), recording each as the corresponding mover_errors op.
    static func buildManifestRows(
        _ db: Database, dateSubdir: String, log: HostLog?
    ) throws -> [[String]] {
        // Skip the manifest itself + the per-day log if the dir is
        // date-named.
        let basename = ((dateSubdir as NSString).lastPathComponent)
        let expectedLog: String? =
            isDateDirName(basename) ? Paths.logFilename(for: basename) : nil

        var files: [String] = []
        let entries: [String]
        do {
            entries = try FileManager.default.contentsOfDirectory(atPath: dateSubdir)
        } catch {
            throw error
        }
        for f in entries {
            if f == manifestFilename { continue }
            if let log = expectedLog, f == log { continue }
            let full = (dateSubdir as NSString).appendingPathComponent(f)
            var isDir: ObjCBool = false
            if FileManager.default.fileExists(atPath: full, isDirectory: &isDir),
               !isDir.boolValue {
                files.append(f)
            }
        }
        files.sort()

        var rows: [[String]] = []
        var currentInvalid = Set<String>()
        var currentOrphan  = Set<String>()
        for filename in files {
            let full = (dateSubdir as NSString).appendingPathComponent(filename)
            if !SnapshotName.isValid(filename) {
                log?.error("Excluding \(full) from manifest — does not "
                           + "match snapshot filename format")
                MoverErrors.tryRecord(
                    db, op: "invalid_filename", target: full,
                    error: ArchiveError.invalidFilename, log: log)
                currentInvalid.insert(full)
                continue
            }
            let info = try lookupEvent(
                db, filename: filename, directory: dateSubdir)
            guard let info = info else {
                log?.error("Excluding \(full) from manifest — no events "
                           + "row in DB for this file (orphan)")
                MoverErrors.tryRecord(
                    db, op: "orphan_file", target: full,
                    error: OrphanFile(), log: log)
                currentOrphan.insert(full)
                continue
            }
            rows.append([
                tsvSanitise(filename),
                info.tag,
                tsvSanitise(info.timestamp),
                tsvSanitise(info.url),
                tsvSanitise(info.title),
            ])
        }

        // Auto-heal: clear invalid_filename / orphan_file rows for
        // files in this dir that the user has since fixed.
        MoverErrors.reconcileDirScoped(
            db, op: "invalid_filename", dirPath: dateSubdir,
            currentStrays: currentInvalid, log: log)
        MoverErrors.reconcileDirScoped(
            db, op: "orphan_file", dirPath: dateSubdir,
            currentStrays: currentOrphan, log: log)
        return rows
    }

    /// Look up the events row for `filename` in `directory` from
    /// either read_events or skimmed_events.  Joins with visits to
    /// pull the page title.  Returns nil if no match.
    static func lookupEvent(
        _ db: Database, filename: String, directory: String
    ) throws -> EventLookup? {
        for (table, tag) in [("read_events", "read"),
                             ("skimmed_events", "skimmed")] {
            let row = try db.queryOne("""
                SELECT e.url, e.timestamp, COALESCE(v.title, '')
                FROM \(table) e LEFT JOIN visits v ON v.url = e.url
                WHERE e.filename = ? AND e.directory = ?
                """, [filename, directory],
                map: { (tag, $0.string(0), $0.string(1), $0.string(2)) })
            if let r = row {
                return EventLookup(
                    tag: r.0, url: r.1, timestamp: r.2, title: r.3)
            }
        }
        return nil
    }

    /// Move <LOG_DIR>/browser-visits-<date>.log into `dateSubdir` as
    /// part of the seal flow.  No-op if no such log file exists.  The
    /// moved file is chmod 0o444.
    static func moveLogIntoSealedDir(
        dateSubdir: String, dateISO: String
    ) throws {
        let src = Paths.logPath(for: dateISO)
        if !FileManager.default.fileExists(atPath: src) { return }
        let dst = (dateSubdir as NSString)
            .appendingPathComponent(Paths.logFilename(for: dateISO))
        try FileManager.default.moveItem(atPath: src, toPath: dst)
        try FileManager.default.setAttributes(
            [.posixPermissions: 0o444], ofItemAtPath: dst)
    }

    /// Rewrite the manifest after a straggler file lands in a
    /// directory whose snapshots row is already sealed=1.  Sealed
    /// flag stays 1 — the dir is still sealed, just with a refreshed
    /// manifest.
    public static func rewriteManifestForStraggler(
        _ db: Database, dateSubdir: String, log: HostLog?
    ) {
        let path = (dateSubdir as NSString)
            .appendingPathComponent(manifestFilename)
        do {
            let count = try writeManifestFile(
                db, dateSubdir: dateSubdir, log: log)
            log?.info("Rewrote \(path) after straggler arrival "
                      + "(\(count) entries)")
            MoverErrors.tryClear(
                db, op: "rewrite_manifest", target: dateSubdir, log: log)
        } catch {
            log?.error("Failed to rewrite \(path) after straggler "
                       + "arrival: \(error)")
            MoverErrors.tryRecord(
                db, op: "rewrite_manifest", target: dateSubdir,
                error: error, log: log)
        }
    }
}

/// One row's worth of metadata fetched from a read_events or
/// skimmed_events lookup, used by both the seal pass and the verify
/// pass.
public struct EventLookup {
    public let tag: String
    public let url: String
    public let timestamp: String
    public let title: String
}

private struct OrphanFile: Error, CustomStringConvertible {
    var description: String {
        "snapshot file has no matching events row"
    }
}
