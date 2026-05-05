import Foundation

/// Per-day visit-log filename pattern: `browser-visits-YYYY-MM-DD.log`.
private let logFilenameRegex: NSRegularExpression = {
    // swiftlint:disable:next force_try
    try! NSRegularExpression(pattern: #"^browser-visits-(\d{4}-\d{2}-\d{2})\.log$"#)
}()

private func extractDate(fromLogFilename name: String) -> String? {
    let r = NSRange(name.startIndex..<name.endIndex, in: name)
    guard let m = logFilenameRegex.firstMatch(in: name, range: r),
          let dateRange = Range(m.range(at: 1), in: name)
    else { return nil }
    return String(name[dateRange])
}

/// Anti-entropy pass over the per-day log files in LOG_DIR.  Mirrors
/// snapshot_mover.py's `_orphan_log_merge_pass`.  Two scenarios it
/// handles:
///
///   1. Race orphan — the iCloud counterpart already exists for a
///      past-day log.  This means a host invocation in flight at
///      seal time recreated the file in LOG_DIR after the seal moved
///      its earlier copy.  Append the orphan's contents into the
///      iCloud log (chmod 0o644 → append → chmod 0o444) and unlink.
///
///   2. Lost snapshots row — no iCloud counterpart and no snapshots
///      row.  Backfill an `(date, sealed=0)` row so the next normal
///      seal pass picks it up.
///
/// Skips today's UTC log file — it's still being written.
public enum OrphanLog {

    public static func mergePass(_ db: Database, log: HostLog?) {
        let dir = Paths.logDir
        guard FileManager.default.fileExists(atPath: dir) else { return }
        let todayISO = todayUTCString()

        let entries: [String]
        do {
            entries = try FileManager.default
                .contentsOfDirectory(atPath: dir)
                .sorted()
        } catch {
            log?.error("orphan-log-merge: could not list \(dir): \(error)")
            return
        }

        for entry in entries {
            guard let dateISO = extractDate(fromLogFilename: entry),
                  dateISO < todayISO
            else { continue }
            let src = (dir as NSString).appendingPathComponent(entry)
            let dateSubdir = (Paths.icloudSnapshotsDir as NSString)
                .appendingPathComponent(dateISO)
            let dst = (dateSubdir as NSString).appendingPathComponent(entry)

            do {
                if FileManager.default.fileExists(atPath: dst) {
                    // Race orphan — append into the iCloud log.
                    try FileManager.default.setAttributes(
                        [.posixPermissions: 0o644], ofItemAtPath: dst)
                    let body = try String(contentsOfFile: src, encoding: .utf8)
                    let handle = try FileHandle(
                        forWritingTo: URL(fileURLWithPath: dst))
                    try handle.seekToEnd()
                    try handle.write(contentsOf: Data(body.utf8))
                    try handle.close()
                    try FileManager.default.setAttributes(
                        [.posixPermissions: 0o444], ofItemAtPath: dst)
                    try FileManager.default.removeItem(atPath: src)
                    log?.info("Merged orphan log \(src) into \(dst)")
                    MoverErrors.tryClear(
                        db, op: "seal", target: dateSubdir, log: log)
                } else {
                    // No iCloud counterpart — make sure the seal pass
                    // picks it up next tick.
                    try db.run("""
                        INSERT OR IGNORE INTO snapshots (date, sealed)
                        VALUES (?, 0)
                        """, [dateISO])
                }
            } catch {
                log?.error("orphan-merge for \(src): \(error)")
                MoverErrors.tryRecord(
                    db, op: "seal", target: dateSubdir,
                    error: error, log: log)
            }
        }
    }
}
