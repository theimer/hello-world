import Foundation

/// User-visible notification surface.  Mirrors snapshot_mover.py's
/// `_notify_user` / `_applescript_quote`.  On macOS, posts a
/// Notification Center banner via `osascript`.  When that's
/// unavailable (non-Darwin, missing osascript, sandbox denial), falls
/// back to appending a line to ~/browser-visits-mover-needs-attention
/// so the user can still spot the failure.
public enum Notify {

    /// Surface an unresolvable error to the user.  Best-effort — never
    /// raises; logging failures inside this code can't be allowed to
    /// derail the caller.
    public static func user(title: String, body: String) {
        let truncated = String(body.prefix(240))   // NC truncates ~256
        #if os(macOS)
        if postNotification(title: title, body: truncated) { return }
        #endif
        // Fallback: touch the attention file.
        let line = "\(MoverErrors.isoNow())\t\(title)\t\(truncated)\n"
        guard let data = line.data(using: .utf8) else { return }
        let path = Paths.attentionFile
        if !FileManager.default.fileExists(atPath: path) {
            FileManager.default.createFile(atPath: path, contents: nil)
        }
        if let handle = try? FileHandle(
            forWritingTo: URL(fileURLWithPath: path)) {
            defer { try? handle.close() }
            try? handle.seekToEnd()
            try? handle.write(contentsOf: data)
        }
    }

    #if os(macOS)
    /// Run `osascript -e 'display notification …'` to pop a Notification
    /// Center banner.  Returns true on success, false on any failure
    /// (osascript missing, exit non-zero, timeout).
    private static func postNotification(title: String, body: String) -> Bool {
        let script = "display notification \(applescriptQuote(body)) "
            + "with title \(applescriptQuote(title))"
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/usr/bin/osascript")
        process.arguments = ["-e", script]
        process.standardOutput = FileHandle.nullDevice
        process.standardError  = FileHandle.nullDevice
        do {
            try process.run()
        } catch {
            return false
        }
        process.waitUntilExit()
        return process.terminationStatus == 0
    }
    #endif

    /// Escape a Swift string for safe interpolation into AppleScript.
    /// Mirrors the Python helper of the same name.
    static func applescriptQuote(_ s: String) -> String {
        let escaped = s
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "\"", with: "\\\"")
        return "\"\(escaped)\""
    }
}
