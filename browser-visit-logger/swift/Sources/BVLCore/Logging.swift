import Foundation

/// File logger that mirrors the Python side's RotatingFileHandler
/// (1 MiB max per file, up to 3 backups).  Rotates when the active
/// log exceeds the size limit by renaming `host.log → host.log.1`,
/// `host.log.1 → host.log.2`, …, dropping any beyond `backupCount`.
///
/// Used by BVLHost; tests construct one with a tmp path.  Thread-safe
/// for the single-process / single-invocation usage pattern host.py
/// has — each Chrome `sendNativeMessage` spawns a new host process,
/// no concurrent writers within a process.
public final class HostLog {

    private let path: String
    private let maxBytes: Int
    private let backupCount: Int

    public init(path: String, maxBytes: Int = 1_048_576, backupCount: Int = 3) {
        self.path = path
        self.maxBytes = maxBytes
        self.backupCount = backupCount
    }

    /// Append one line (newline added automatically) to the log.  Format:
    ///   `<ISO timestamp> <LEVEL> <message>`
    /// Errors during the write are deliberately swallowed — logging is
    /// best-effort and must never abort an in-progress invocation.
    public func write(level: String, _ message: String) {
        let line = "\(Self.timestamp()) \(level) \(message)\n"
        guard let data = line.data(using: .utf8) else { return }
        rotateIfNeeded(addingBytes: data.count)
        do {
            try ensureFileExists()
            let handle = try FileHandle(forWritingTo: URL(fileURLWithPath: path))
            defer { try? handle.close() }
            try handle.seekToEnd()
            try handle.write(contentsOf: data)
        } catch {
            // Best-effort: a failing log write must not derail the
            // caller's request handling.  Swallow.
        }
    }

    public func debug(_ s: String)   { write(level: "DEBUG", s) }
    public func info(_ s: String)    { write(level: "INFO",  s) }
    public func warning(_ s: String) { write(level: "WARNING", s) }
    public func error(_ s: String)   { write(level: "ERROR", s) }

    // MARK: - Internals

    /// ISO-8601-like timestamp matching the Python format
    /// `%(asctime)s` produces, e.g. "2026-05-04 13:14:15,123".  Local
    /// time, three-digit millisecond suffix.  Matched so existing log
    /// parsers keep working.
    static func timestamp(_ now: Date = Date()) -> String {
        let cal = Calendar(identifier: .gregorian)
        let comps = cal.dateComponents(
            [.year, .month, .day, .hour, .minute, .second, .nanosecond],
            from: now)
        let ms = (comps.nanosecond ?? 0) / 1_000_000
        return String(format: "%04d-%02d-%02d %02d:%02d:%02d,%03d",
                      comps.year ?? 0, comps.month ?? 0, comps.day ?? 0,
                      comps.hour ?? 0, comps.minute ?? 0, comps.second ?? 0,
                      ms)
    }

    private func ensureFileExists() throws {
        let url = URL(fileURLWithPath: path)
        if !FileManager.default.fileExists(atPath: path) {
            // Create the parent dir if needed and an empty file.
            let parent = url.deletingLastPathComponent()
            try FileManager.default.createDirectory(
                at: parent, withIntermediateDirectories: true)
            FileManager.default.createFile(atPath: path, contents: nil)
        }
    }

    /// Check the current log size; if appending the new line would push
    /// it over `maxBytes`, rotate.  Mirrors RotatingFileHandler's
    /// pre-write rotation behaviour.
    private func rotateIfNeeded(addingBytes adding: Int) {
        let size = (try? FileManager.default.attributesOfItem(atPath: path)[.size] as? Int) ?? 0
        guard size + adding > maxBytes else { return }
        // Drop the oldest backup if it exists, then shift each one up.
        let last = "\(path).\(backupCount)"
        try? FileManager.default.removeItem(atPath: last)
        for i in stride(from: backupCount - 1, through: 1, by: -1) {
            let src = "\(path).\(i)"
            let dst = "\(path).\(i + 1)"
            if FileManager.default.fileExists(atPath: src) {
                try? FileManager.default.moveItem(atPath: src, toPath: dst)
            }
        }
        if FileManager.default.fileExists(atPath: path) {
            try? FileManager.default.moveItem(atPath: path, toPath: "\(path).1")
        }
    }
}

/// Sanitise `s` for safe TSV output: strip tabs, newlines, carriage
/// returns by replacing each with a space.  Matches the Python side's
/// `_tsv_sanitise` so log replay produces identical results.
public func tsvSanitise(_ s: String) -> String {
    var out = ""
    out.reserveCapacity(s.count)
    for ch in s {
        switch ch {
        case "\t", "\n":
            out.append(" ")
        case "\r":
            continue
        default:
            out.append(ch)
        }
    }
    return out
}

/// Today's UTC date as YYYY-MM-DD.
public func todayUTCString(_ now: Date = Date()) -> String {
    let f = DateFormatter()
    f.dateFormat = "yyyy-MM-dd"
    f.timeZone = TimeZone(identifier: "UTC")
    f.locale = Locale(identifier: "en_US_POSIX")
    return f.string(from: now)
}
