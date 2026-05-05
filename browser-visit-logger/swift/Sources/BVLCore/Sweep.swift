import Foundation

/// Periodic sweep of the Downloads snapshots dir.  Mirrors
/// snapshot_mover.py's `_move_pass`: every file at least
/// `BVL_MOVER_MIN_AGE_SECONDS` old (default 60) gets archived to its
/// matching iCloud date subdir.  Non-conforming filenames record an
/// `invalid_filename` error and are left in place.
///
/// The age gate is what keeps the sweep from racing against an
/// in-flight Chrome download — once a file's mtime is at least 60 s
/// old, the download is definitely complete.
public enum Sweep {

    public static func pass(
        _ db: Database,
        dryRun: Bool = false,
        log: HostLog?
    ) {
        let dir = Paths.downloadsSnapshotsDir
        guard FileManager.default.fileExists(atPath: dir),
              (try? FileManager.default.attributesOfItem(atPath: dir)[.type]) as? FileAttributeType == .typeDirectory
        else {
            return
        }
        let now = Date().timeIntervalSince1970
        let minAge = TimeInterval(
            Int(ProcessInfo.processInfo.environment["BVL_MOVER_MIN_AGE_SECONDS"] ?? "60") ?? 60)

        let entries: [String]
        do {
            entries = try FileManager.default.contentsOfDirectory(atPath: dir)
        } catch {
            log?.error("Sweep: could not list \(dir): \(error)")
            return
        }

        var currentInvalid = Set<String>()
        for name in entries {
            let source = (dir as NSString).appendingPathComponent(name)
            var isDir: ObjCBool = false
            FileManager.default.fileExists(atPath: source, isDirectory: &isDir)
            if isDir.boolValue { continue }

            guard let dateStr = SnapshotName.dateString(forBasename: name) else {
                log?.error("Skipping \(source) — does not match snapshot "
                           + "filename format; leaving in Downloads")
                MoverErrors.tryRecord(
                    db, op: "invalid_filename", target: source,
                    error: ArchiveError.invalidFilename, log: log)
                currentInvalid.insert(source)
                continue
            }

            let mtime = (try? FileManager.default.attributesOfItem(atPath: source)[.modificationDate] as? Date)
                ?? Date()
            let age = now - mtime.timeIntervalSince1970
            if age < minAge {
                log?.debug("Skipping \(name) — only \(Int(age))s old "
                           + "(< \(Int(minAge))s)")
                continue
            }

            if dryRun {
                log?.info("[dry-run] would move \(source) -> "
                          + "\(Paths.icloudSnapshotsDir)/\(dateStr)/\(name)")
                continue
            }
            Archive.moveOne(
                db, source: source, basename: name,
                dateStr: dateStr, log: log)
        }

        // Auto-heal: clear invalid_filename rows for files in the
        // Downloads dir that the user has since renamed or removed.
        MoverErrors.reconcileDirScoped(
            db, op: "invalid_filename", dirPath: dir,
            currentStrays: currentInvalid, log: log)
    }
}
