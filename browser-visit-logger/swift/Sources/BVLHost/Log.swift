import BVLCore
import Foundation

/// Per-day visit-log writer.  Mirrors host.py's append_log /
/// append_result_log: each invocation emits one action line (TSV:
/// record_id, timestamp, url, title, [tag, [filename]]) followed by
/// one result line (record_id + tab + "success"|"error: ...").  The
/// same date_iso is pinned by the caller for both halves so they
/// always land in the same per-day log file even across midnight UTC.
public enum VisitLog {

    /// Append the action line.  Field count varies with the message
    /// shape:
    ///   auto-log:    record_id, timestamp, url, title             (4)
    ///   of_interest: record_id, timestamp, url, title, tag        (5)
    ///   read/skim:   record_id, timestamp, url, title, tag, file  (6)
    public static func appendAction(
        recordId: String, dateISO: String,
        timestamp: String, url: String, title: String,
        tag: String, filename: String
    ) throws {
        var fields = [
            tsvSanitise(recordId), tsvSanitise(timestamp),
            tsvSanitise(url), tsvSanitise(title),
        ]
        if !tag.isEmpty {
            fields.append(tsvSanitise(tag))
            if tag == "read" || tag == "skimmed" {
                fields.append(tsvSanitise(filename))
            }
        }
        try appendLine(dateISO: dateISO, fields.joined(separator: "\t"))
    }

    /// Append the matching result line: "<record_id>\t<result>".
    public static func appendResult(
        recordId: String, dateISO: String, result: String
    ) throws {
        let line = "\(tsvSanitise(recordId))\t\(tsvSanitise(result))"
        try appendLine(dateISO: dateISO, line)
    }

    private static func appendLine(dateISO: String, _ line: String) throws {
        let path = Paths.logPath(for: dateISO)
        let url = URL(fileURLWithPath: path)
        if !FileManager.default.fileExists(atPath: path) {
            try FileManager.default.createDirectory(
                at: url.deletingLastPathComponent(),
                withIntermediateDirectories: true)
            FileManager.default.createFile(atPath: path, contents: nil)
        }
        let handle = try FileHandle(forWritingTo: url)
        defer { try? handle.close() }
        try handle.seekToEnd()
        try handle.write(contentsOf: Data((line + "\n").utf8))
    }
}
