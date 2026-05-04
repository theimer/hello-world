import Foundation

/// Result of verifying one sealed daily directory.
public struct VerifyResult {
    public let isValid: Bool
    public let issues: [String]
}

/// Run all 10 manifest-correctness checks against a sealed daily
/// snapshot directory.  Mirrors snapshot_verifier.py's
/// `verify_directory` exactly so failures land identically in the
/// `manifest_invalid` mover_errors row.
public enum Verify {

    public static func directory(
        _ db: Database, dateSubdir: String
    ) -> VerifyResult {
        var issues: [String] = []
        let manifestPath = (dateSubdir as NSString)
            .appendingPathComponent(manifestFilename)

        // 1. Existence
        guard FileManager.default.fileExists(atPath: manifestPath) else {
            issues.append("Manifest file not found at \(manifestPath)")
            return VerifyResult(isValid: false, issues: issues)
        }

        // 2. Read-only.  Format the mode the same way Python's
        //    "{mode:#o}" does, e.g. "0o644".
        let mode = (try? FileManager.default.attributesOfItem(
            atPath: manifestPath)[.posixPermissions] as? Int) ?? 0
        if mode != 0o444 {
            issues.append(
                "Manifest is not read-only (mode 0o\(String(mode, radix: 8)))")
        }

        // 3. Read content.  Trailing newline produces a trailing
        //    empty element from `split` — drop it before iterating.
        var lines: [String]
        do {
            let body = try String(contentsOfFile: manifestPath, encoding: .utf8)
            lines = body.split(
                separator: "\n", omittingEmptySubsequences: false
            ).map(String.init)
        } catch {
            issues.append("Could not read manifest: \(error)")
            return VerifyResult(isValid: false, issues: issues)
        }
        if lines.last == "" { lines.removeLast() }

        // 4. Header
        let expectedHeader = manifestHeader.joined(separator: "\t")
        guard let header = lines.first else {
            issues.append("Manifest file is empty")
            return VerifyResult(isValid: false, issues: issues)
        }
        if header != expectedHeader {
            issues.append(
                "Header mismatch: expected '\(expectedHeader)', got '\(header)'")
            return VerifyResult(isValid: false, issues: issues)
        }

        // 5. Parse data rows.
        typealias ManifestEntry = (
            tag: String, ts: String, url: String, title: String)
        var manifestEntries: [String: ManifestEntry] = [:]
        for (idx, line) in lines.dropFirst().enumerated() {
            let rowNum = idx + 2
            let fields = line.split(
                separator: "\t", omittingEmptySubsequences: false
            ).map(String.init)
            if fields.count != 5 {
                issues.append(
                    "Row \(rowNum): expected 5 columns, got \(fields.count)")
                continue
            }
            let filename = fields[0]
            if manifestEntries[filename] != nil {
                issues.append(
                    "Row \(rowNum): duplicate filename '\(filename)'")
                continue
            }
            manifestEntries[filename] = (
                fields[1], fields[2], fields[3], fields[4])
        }

        // Determine the expected per-day log filename for this directory.
        let basename = (dateSubdir as NSString).lastPathComponent
        let expectedLog: String? =
            isDateDirName(basename) ? Paths.logFilename(for: basename) : nil

        // 6. File-level check: every file in the directory (other than
        //    the manifest and the per-day log) has a conforming
        //    snapshot filename.
        var conformingOnDisk = Set<String>()
        let entries: [String]
        do {
            entries = try FileManager.default.contentsOfDirectory(atPath: dateSubdir)
        } catch {
            issues.append("Could not list \(dateSubdir): \(error)")
            return VerifyResult(isValid: false, issues: issues)
        }
        for f in entries {
            if f == manifestFilename { continue }
            if let log = expectedLog, f == log { continue }
            let full = (dateSubdir as NSString).appendingPathComponent(f)
            var isDir: ObjCBool = false
            FileManager.default.fileExists(atPath: full, isDirectory: &isDir)
            if isDir.boolValue { continue }
            if !SnapshotName.isValid(f) {
                issues.append("Non-conforming file in directory: \(f)")
                if manifestEntries[f] != nil {
                    issues.append(
                        "Manifest also contains non-conforming filename '\(f)'")
                }
                continue
            }
            conformingOnDisk.insert(f)
        }

        // 7. Set comparison: manifest filenames vs on-disk conforming.
        let manifestFilenames = Set(manifestEntries.keys)
        for f in manifestFilenames.subtracting(conformingOnDisk).sorted() {
            // Non-conforming filenames already flagged above.
            if !SnapshotName.isValid(f) { continue }
            issues.append(
                "Manifest references \(f) but no such file in directory")
        }
        for f in conformingOnDisk.subtracting(manifestFilenames).sorted() {
            issues.append(
                "File \(f) is in directory but not listed in manifest")
        }

        // 8. Per-row check: every manifest row has a corresponding
        //    events row, and (tag, ts, url, title) fields match.
        for filename in manifestFilenames.intersection(conformingOnDisk).sorted() {
            let m = manifestEntries[filename]!
            do {
                guard let info = try Seal.lookupEvent(
                    db, filename: filename, directory: dateSubdir
                ) else {
                    issues.append(
                        "\(filename): manifest row has no corresponding "
                        + "events row in DB")
                    continue
                }
                let expectedTag = info.tag
                let expectedTs    = tsvSanitise(info.timestamp)
                let expectedUrl   = tsvSanitise(info.url)
                let expectedTitle = tsvSanitise(info.title)
                if m.tag != expectedTag || m.ts != expectedTs
                    || m.url != expectedUrl || m.title != expectedTitle {
                    issues.append(
                        "\(filename): metadata mismatch — "
                        + "manifest=(\(m.tag), \(m.ts), \(m.url), \(m.title)) "
                        + "DB=(\(expectedTag), \(expectedTs), "
                        + "\(expectedUrl), \(expectedTitle))")
                }
            } catch {
                issues.append(
                    "\(filename): events lookup failed: \(error)")
            }
        }

        // 9. Orphan-file check: every conforming file in the
        //    directory must also have an events row.
        for filename in conformingOnDisk.subtracting(manifestFilenames).sorted() {
            do {
                if try Seal.lookupEvent(
                    db, filename: filename, directory: dateSubdir
                ) == nil {
                    issues.append(
                        "\(filename): conforming file in directory has "
                        + "no corresponding events row in DB")
                }
            } catch {
                issues.append(
                    "\(filename): events lookup failed: \(error)")
            }
        }

        // 10. Per-day log file — must be present, regular file, 0o444.
        if let logName = expectedLog {
            let logPath = (dateSubdir as NSString)
                .appendingPathComponent(logName)
            if !FileManager.default.fileExists(atPath: logPath) {
                issues.append("Per-day log file not found at \(logPath)")
            } else {
                var isDir: ObjCBool = false
                FileManager.default.fileExists(atPath: logPath,
                                               isDirectory: &isDir)
                if isDir.boolValue {
                    issues.append(
                        "Per-day log \(logPath) is not a regular file")
                } else {
                    let logMode = (try? FileManager.default.attributesOfItem(
                        atPath: logPath)[.posixPermissions] as? Int) ?? 0
                    if logMode != 0o444 {
                        issues.append(
                            "Per-day log is not read-only "
                            + "(mode 0o\(String(logMode, radix: 8)))")
                    }
                }
            }
        }

        return VerifyResult(isValid: issues.isEmpty, issues: issues)
    }
}
