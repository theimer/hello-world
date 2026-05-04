import Foundation

/// Path constants — read once from the environment at startup so callers
/// can reference them without re-reading.  Mirrors the Python side's
/// BVL_* env var convention so existing tooling and tests can override
/// paths the same way.
public enum Paths {

    /// Re-read at every property access so unit tests can mutate the
    /// environment between cases without re-launching the process.
    private static func env(_ key: String, _ fallback: @autoclosure () -> String) -> String {
        ProcessInfo.processInfo.environment[key] ?? fallback()
    }

    public static var home: String {
        NSHomeDirectory()
    }

    public static var logDir: String {
        env("BVL_LOG_DIR", home)
    }

    public static var dbFile: String {
        env("BVL_DB_FILE", (home as NSString).appendingPathComponent("browser-visits.db"))
    }

    public static var hostLog: String {
        env("BVL_HOST_LOG",
            (home as NSString).appendingPathComponent("browser-visits-host.log"))
    }

    public static var downloadsSnapshotsDir: String {
        env("BVL_DOWNLOADS_SNAPSHOTS_DIR",
            (home as NSString)
                .appendingPathComponent("Downloads/browser-visit-snapshots"))
    }

    public static var icloudSnapshotsDir: String {
        env("BVL_ICLOUD_SNAPSHOTS_DIR",
            (home as NSString)
                .appendingPathComponent("Documents/browser-visit-logger/snapshots"))
    }

    public static var moverErrorThreshold: Int {
        Int(env("BVL_MOVER_ERROR_THRESHOLD", "3")) ?? 3
    }

    /// Per-day visit log filename for a UTC date, e.g.
    /// "browser-visits-2026-04-30.log".
    public static func logFilename(for dateISO: String) -> String {
        "browser-visits-\(dateISO).log"
    }

    /// Absolute path of the per-day visit log inside `logDir`.
    public static func logPath(for dateISO: String) -> String {
        (logDir as NSString).appendingPathComponent(logFilename(for: dateISO))
    }

    /// Marker file written when we can't reach Notification Center.
    public static var attentionFile: String {
        (home as NSString).appendingPathComponent("browser-visits-mover-needs-attention")
    }
}
