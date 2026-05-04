import Foundation

/// Parsing of the canonical snapshot filename format,
/// `<YYYY-MM-DDTHH-MM-SSZ>-<hash>.<ext>`.
///
/// host.py renames each Chrome download to this format at record time.
/// The 10-character date prefix determines which iCloud date subdir
/// the file lands in.
public enum SnapshotName {

    private static let pattern = #"^(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}Z-.+$"#
    private static let regex: NSRegularExpression = {
        // Force-try is acceptable here — pattern is a compile-time
        // constant and is verified by tests.
        // swiftlint:disable:next force_try
        try! NSRegularExpression(pattern: pattern)
    }()

    /// Returns the UTC date string (YYYY-MM-DD) embedded in `basename`,
    /// or nil if `basename` does not match the snapshot filename format.
    public static func dateString(forBasename basename: String) -> String? {
        let range = NSRange(basename.startIndex..<basename.endIndex, in: basename)
        guard let match = regex.firstMatch(in: basename, options: [], range: range),
              let dateRange = Range(match.range(at: 1), in: basename)
        else {
            return nil
        }
        return String(basename[dateRange])
    }

    /// True iff `basename` matches the snapshot filename format.
    public static func isValid(_ basename: String) -> Bool {
        dateString(forBasename: basename) != nil
    }
}
