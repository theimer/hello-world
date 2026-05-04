import Foundation

/// Synchronous archive: copy one snapshot file from
/// ``Paths.downloadsSnapshotsDir`` into its iCloud date subdir, chmod
/// the destination read-only, update the events row's `directory`
/// column, insert a snapshots row, and unlink the source.  Mirrors the
/// Python side's `_move_one` + `archive_for_tag` end-to-end.
///
/// Best-effort: failures are recorded in the `mover_errors` table and
/// logged, but the function never throws — callers (BVLHost.main) must
/// continue processing the rest of the request even if archiving fails.
public enum Archive {

    /// Archive a single file by Chrome-relative path.  `filename` is
    /// what Chrome reported in the native message — typically
    /// "browser-visit-snapshots/<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>".
    /// Only the basename is used for path resolution.
    ///
    /// Three short-circuit paths:
    ///   - empty basename: silent no-op.
    ///   - non-conforming filename: record `invalid_filename` error.
    ///   - source file missing: log warning, no error row (the next
    ///     verifier sweep is the natural retry mechanism).
    public static func forTag(
        _ db: Database, filename: String, log: HostLog?
    ) {
        let basename = (filename as NSString).lastPathComponent
        if basename.isEmpty { return }
        let source = (Paths.downloadsSnapshotsDir as NSString)
            .appendingPathComponent(basename)
        guard let dateStr = SnapshotName.dateString(forBasename: basename) else {
            log?.error(
                "archive_for_tag: \(source) does not match snapshot filename format")
            MoverErrors.tryRecord(
                db, op: "invalid_filename", target: source,
                error: ArchiveError.invalidFilename, log: log)
            return
        }
        if !FileManager.default.fileExists(atPath: source) {
            log?.warning(
                "archive_for_tag: source \(source) not present; skipping")
            return
        }
        moveOne(db, source: source, basename: basename,
                dateStr: dateStr, log: log)
    }

    /// Copy + chmod + DB update + unlink for one file whose source
    /// path is already known and present.  Used by archive_for_tag and
    /// (in a follow-up PR) by the verifier's sweep pass.
    static func moveOne(
        _ db: Database, source: String, basename: String,
        dateStr: String, log: HostLog?
    ) {
        let dateSubdir = (Paths.icloudSnapshotsDir as NSString)
            .appendingPathComponent(dateStr)
        let dest = (dateSubdir as NSString).appendingPathComponent(basename)

        do {
            // (a) Ensure the date subdir exists.
            try FileManager.default.createDirectory(
                atPath: dateSubdir, withIntermediateDirectories: true)
            // (b) Copy source → dest.  shutil.copy2 preserves mtime; we
            //     match by setting attributes after the copy.
            if FileManager.default.fileExists(atPath: dest) {
                // Make the existing dest writable so we can overwrite —
                // mirrors Python's copy2 over a 0o444 file.
                try? FileManager.default.setAttributes(
                    [.posixPermissions: 0o644], ofItemAtPath: dest)
                try FileManager.default.removeItem(atPath: dest)
            }
            try FileManager.default.copyItem(atPath: source, toPath: dest)
            // Preserve mtime explicitly (Foundation's copyItem doesn't
            // guarantee it cross-volume).
            if let mtime = try? FileManager.default.attributesOfItem(atPath: source)[.modificationDate] as? Date {
                try? FileManager.default.setAttributes(
                    [.modificationDate: mtime], ofItemAtPath: dest)
            }
            // (c) Make dest read-only.
            try FileManager.default.setAttributes(
                [.posixPermissions: 0o444], ofItemAtPath: dest)
            // (d) Update DB rows that still record this file as living
            //     in Downloads.  Rows already pointing to iCloud (from
            //     a partial prior run) are untouched.
            for table in ["read_events", "skimmed_events"] {
                try db.run("""
                    UPDATE \(table) SET directory = ?
                    WHERE filename = ? AND directory = ?
                    """, [dateSubdir, basename, Paths.downloadsSnapshotsDir])
            }
            // (e) INSERT OR IGNORE the snapshots row.  We don't need to
            //     detect stragglers in the host's per-tag path (sealed
            //     dirs only happen in the verifier's seal pass, and
            //     only past dates can be sealed) — straggler handling
            //     stays in the verifier.
            try db.run("""
                INSERT OR IGNORE INTO snapshots (date, sealed) VALUES (?, 0)
                """, [dateStr])
            // (f) Remove the source.
            try FileManager.default.removeItem(atPath: source)
            log?.info("Moved \(source) -> \(dest) (read-only)")
            // (g) Move succeeded — clear any prior 'move' error.
            MoverErrors.tryClear(db, op: "move", target: source, log: log)
        } catch {
            log?.error("Failed to move \(source): \(error)")
            MoverErrors.tryRecord(
                db, op: "move", target: source, error: error, log: log)
        }
    }
}

enum ArchiveError: Error, CustomStringConvertible {
    case invalidFilename
    var description: String { "filename does not match snapshot format" }
}
