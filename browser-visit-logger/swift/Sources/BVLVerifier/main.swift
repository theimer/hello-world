// BVLVerifier — the sole background agent.
//
// Replaces native-host/snapshot_verifier.py.  The bundle's
// Contents/MacOS/<name> is this binary directly (Mach-O), so when
// launchd spawns the bundle's executable, the running process IS the
// bundle's signed code and TCC attributes file accesses to the
// bundle's identity.
//
// Default mode (no operation flag): run a full housekeeping tick —
//   1. sweep — relocate stragglers in ~/Downloads/browser-visit-snapshots
//   2. seal  — write MANIFEST.tsv for completed UTC days, move per-day
//              logs into the sealed dirs
//   3. orphan-log-merge — reconcile per-day logs in LOG_DIR with iCloud
//   4. verify — re-check every sealed daily directory's manifest
//   5. escalate — surface unresolved errors via Notification Center
//
// CLI flags:
//   --verify DIR_OR_DATE     verify one dir (skips sweep/seal/escalate)
//   --verify-all             verify every sealed dir
//   --show-errors            print pending mover_errors and exit
//   --clear-errors           wipe mover_errors and exit
//   --clear-error N          delete the Nth row (1-indexed)
//   --dry-run                plan a tick without writing
//   --quiet                  print only failure summaries
//   --record                 (for --verify / --verify-all) UPSERT
//                            failures into mover_errors
//   -v, --verbose            DEBUG log level
//   --source DIR             override BVL_DOWNLOADS_SNAPSHOTS_DIR
//   --dest   DIR             override BVL_ICLOUD_SNAPSHOTS_DIR
//   --db     FILE            override BVL_DB_FILE
//   --min-age-seconds N      override BVL_MOVER_MIN_AGE_SECONDS
//
// Exit codes: 0 on success, 1 on any verify failure / argument error.

import BVLCore
import Foundation

// MARK: - CLI parsing (hand-rolled — no third-party deps)

struct CLIOptions {
    enum Mode {
        case tick
        case verifyOne(String)
        case verifyAll
        case showErrors
        case clearErrors
        case clearError(Int)
    }
    var mode: Mode = .tick
    var dryRun: Bool = false
    var quiet: Bool = false
    var record: Bool = false
    var verbose: Bool = false
}

enum CLIError: Error, CustomStringConvertible {
    case usage(String)
    var description: String {
        if case let .usage(msg) = self { return msg }
        return "usage error"
    }
}

func parseArgs(_ argv: [String]) throws -> CLIOptions {
    var opts = CLIOptions()
    var operationsSet = 0
    func setMode(_ m: CLIOptions.Mode, flag: String) throws {
        operationsSet += 1
        if operationsSet > 1 {
            throw CLIError.usage(
                "operation flags are mutually exclusive (got: \(flag))")
        }
        opts.mode = m
    }

    var i = 0
    while i < argv.count {
        let a = argv[i]
        switch a {
        case "--verify":
            i += 1
            guard i < argv.count else {
                throw CLIError.usage("--verify requires a date or path")
            }
            try setMode(.verifyOne(argv[i]), flag: "--verify")
        case "--verify-all":
            try setMode(.verifyAll, flag: "--verify-all")
        case "--show-errors":
            try setMode(.showErrors, flag: "--show-errors")
        case "--clear-errors":
            try setMode(.clearErrors, flag: "--clear-errors")
        case "--clear-error":
            i += 1
            guard i < argv.count, let n = Int(argv[i]) else {
                throw CLIError.usage("--clear-error requires an integer")
            }
            try setMode(.clearError(n), flag: "--clear-error")
        case "--dry-run":
            opts.dryRun = true
        case "--quiet":
            opts.quiet = true
        case "--record":
            opts.record = true
        case "-v", "--verbose":
            opts.verbose = true
        case "--source":
            i += 1
            guard i < argv.count else {
                throw CLIError.usage("--source requires a path")
            }
            setenv("BVL_DOWNLOADS_SNAPSHOTS_DIR", argv[i], 1)
        case "--dest":
            i += 1
            guard i < argv.count else {
                throw CLIError.usage("--dest requires a path")
            }
            setenv("BVL_ICLOUD_SNAPSHOTS_DIR", argv[i], 1)
        case "--db":
            i += 1
            guard i < argv.count else {
                throw CLIError.usage("--db requires a path")
            }
            setenv("BVL_DB_FILE", argv[i], 1)
        case "--min-age-seconds":
            i += 1
            guard i < argv.count, Int(argv[i]) != nil else {
                throw CLIError.usage("--min-age-seconds requires an integer")
            }
            setenv("BVL_MOVER_MIN_AGE_SECONDS", argv[i], 1)
        case "-h", "--help":
            printUsage()
            exit(0)
        default:
            throw CLIError.usage("unrecognized argument: \(a)")
        }
        i += 1
    }
    return opts
}

func printUsage() {
    let usage = """
    BVLVerifier — Browser Visit Logger background agent.

    Default (no flag): full tick — sweep + seal + orphan-merge +
                        verify + escalate.

    Operation flags (mutually exclusive):
      --verify DIR_OR_DATE   verify one directory (no sweep / seal / escalate)
      --verify-all           verify every sealed directory
      --show-errors          print pending mover_errors and exit
      --clear-errors         delete every mover_errors row
      --clear-error N        delete the Nth row (1-indexed)

    Common flags:
      --dry-run              plan without writing
      --quiet                print only failures
      --record               UPSERT verify failures into mover_errors
      -v, --verbose          DEBUG log level

    Path overrides:
      --source DIR
      --dest DIR
      --db FILE
      --min-age-seconds N
    """
    print(usage)
}

// MARK: - Main

let argv = Array(CommandLine.arguments.dropFirst())
let opts: CLIOptions
do {
    opts = try parseArgs(argv)
} catch let err {
    FileHandle.standardError.write(Data("Error: \(err)\n".utf8))
    exit(1)
}

// Stderr is captured by the LaunchAgent's StandardErrorPath, which
// points at ~/browser-visits-verifier.log.  For interactive runs the
// user sees the messages directly.
let log = HostLog(path: ProcessInfo.processInfo.environment["BVL_VERIFIER_LOG"]
                       ?? Paths.hostLog)

func resolveTarget(_ arg: String) -> String {
    // Bare 'YYYY-MM-DD' joins under ICLOUD_SNAPSHOTS_DIR; anything
    // that looks like a path (absolute, or contains a separator) is
    // used verbatim.
    if (arg as NSString).isAbsolutePath || arg.contains("/") {
        return arg
    }
    return (Paths.icloudSnapshotsDir as NSString).appendingPathComponent(arg)
}

func verifyOne(target: String, opts: CLIOptions) -> Int32 {
    if !FileManager.default.fileExists(atPath: target) {
        FileHandle.standardError.write(Data("No such directory: \(target)\n".utf8))
        return 1
    }
    do {
        let db = try Database(path: Paths.dbFile)
        try Schema.ensureMoverErrorsTable(db)
        let result = Verify.directory(db, dateSubdir: target)
        if opts.record {
            updateErrorState(
                db, target: target, result: result, log: log)
            MoverErrors.escalate(db, log: log)
        }
        printResult(target: target, result: result, quiet: opts.quiet)
        return result.isValid ? 0 : 1
    } catch {
        FileHandle.standardError.write(
            Data("verify failed: \(error)\n".utf8))
        return 1
    }
}

func verifyAllSealed(
    db: Database, record: Bool, quiet: Bool, dryRun: Bool
) -> Bool {
    if dryRun {
        log.info("[dry-run] would verify every sealed snapshot directory")
        return true
    }
    let dates: [String]
    do {
        dates = try db.queryAll(
            "SELECT date FROM snapshots WHERE sealed = 1 ORDER BY date",
            map: { $0.string(0) })
    } catch {
        log.error("verify-all: could not query snapshots: \(error)")
        return false
    }
    if dates.isEmpty {
        if !quiet { print("No sealed directories to verify.") }
        return true
    }
    var anyFailed = false
    for date in dates {
        let target = (Paths.icloudSnapshotsDir as NSString)
            .appendingPathComponent(date)
        if !FileManager.default.fileExists(atPath: target) {
            // Already covered by the seal pass's missing_directory error.
            continue
        }
        let result = Verify.directory(db, dateSubdir: target)
        if record {
            updateErrorState(
                db, target: target, result: result, log: log)
        }
        printResult(target: target, result: result, quiet: quiet)
        if !result.isValid { anyFailed = true }
    }
    return !anyFailed
}

func updateErrorState(
    _ db: Database, target: String, result: VerifyResult, log: HostLog?
) {
    if result.isValid {
        MoverErrors.tryClear(
            db, op: "manifest_invalid", target: target, log: log)
    } else {
        let message = result.issues.joined(separator: "; ")
        MoverErrors.tryRecord(
            db, op: "manifest_invalid", target: target,
            error: VerifyError(message: message), log: log)
    }
}

struct VerifyError: Error, CustomStringConvertible {
    let message: String
    var description: String { message }
}

func printResult(target: String, result: VerifyResult, quiet: Bool) {
    if result.isValid {
        if !quiet { print("\(target): OK") }
        return
    }
    let n = result.issues.count
    print("\(target): FAILED (\(n) issue\(n == 1 ? "" : "s"))")
    for i in result.issues { print("  - \(i)") }
}

func runTick(opts: CLIOptions) -> Int32 {
    do {
        try FileManager.default.createDirectory(
            atPath: Paths.icloudSnapshotsDir,
            withIntermediateDirectories: true)
    } catch {
        FileHandle.standardError.write(Data(
            "Could not create iCloud snapshots dir \(Paths.icloudSnapshotsDir): \(error)\n".utf8))
        return 1
    }
    if !FileManager.default.fileExists(atPath: Paths.dbFile) {
        FileHandle.standardError.write(Data(
            "No DB at \(Paths.dbFile)\n".utf8))
        return 1
    }
    let db: Database
    do {
        db = try Database(path: Paths.dbFile)
        try Schema.ensureSnapshotsTable(db)
        try Schema.ensureMoverErrorsTable(db)
    } catch {
        log.error("Could not open DB: \(error)")
        return 1
    }
    do {
        Sweep.pass(db, dryRun: opts.dryRun, log: log)
        Seal.pass(db, dryRun: opts.dryRun, log: log)
        if !opts.dryRun {
            OrphanLog.mergePass(db, log: log)
        }
        let allOK = verifyAllSealed(
            db: db, record: true, quiet: opts.quiet, dryRun: opts.dryRun)
        if !opts.dryRun {
            MoverErrors.escalate(db, log: log)
        }
        return allOK ? 0 : 1
    }
}

func showErrors() -> Int32 {
    do {
        let db = try Database(path: Paths.dbFile)
        try Schema.ensureMoverErrorsTable(db)
        let rows = try MoverErrors.fetchPending(db)
        if rows.isEmpty {
            print("No pending mover errors.")
            return 0
        }
        print("Pending mover errors (\(rows.count)):")
        print()
        for (idx, r) in rows.enumerated() {
            let n = idx + 1
            let target = r.target.isEmpty ? "(no target)" : r.target
            print("  [\(n)] \(r.operation): \(target)")
            print("      attempts: \(r.attempts) "
                  + "(since \(r.firstSeen), last \(r.lastSeen))")
            print("      error:    \(r.message)")
            if let hint = MoverErrors.fixHints[r.operation] {
                print("      fix:      \(hint)")
            }
            print("      notified: \(r.notified ? "yes" : "no")")
            print()
        }
        return 0
    } catch {
        FileHandle.standardError.write(
            Data("show-errors failed: \(error)\n".utf8))
        return 1
    }
}

func clearErrors() -> Int32 {
    do {
        let db = try Database(path: Paths.dbFile)
        try Schema.ensureMoverErrorsTable(db)
        let n = try db.run("DELETE FROM mover_errors")
        print("Cleared \(n) error row\(n == 1 ? "" : "s").")
        return 0
    } catch {
        FileHandle.standardError.write(
            Data("clear-errors failed: \(error)\n".utf8))
        return 1
    }
}

func clearError(n: Int) -> Int32 {
    do {
        let db = try Database(path: Paths.dbFile)
        try Schema.ensureMoverErrorsTable(db)
        let rows = try MoverErrors.fetchPending(db)
        if rows.isEmpty {
            FileHandle.standardError.write(
                Data("No pending mover errors to clear.\n".utf8))
            return 1
        }
        if n < 1 || n > rows.count {
            let msg = "No error at index \(n) (table has \(rows.count) "
                + "row\(rows.count == 1 ? "" : "s")).\n"
            FileHandle.standardError.write(Data(msg.utf8))
            return 1
        }
        let row = rows[n - 1]
        try db.run(
            "DELETE FROM mover_errors WHERE key = ?", [row.key])
        let target = row.target.isEmpty ? "(no target)" : row.target
        print("Cleared error [\(n)]: \(row.operation): \(target)")
        return 0
    } catch {
        FileHandle.standardError.write(
            Data("clear-error failed: \(error)\n".utf8))
        return 1
    }
}

// Dispatch.
let rc: Int32
switch opts.mode {
case .tick:
    rc = runTick(opts: opts)
case let .verifyOne(arg):
    rc = verifyOne(target: resolveTarget(arg), opts: opts)
case .verifyAll:
    do {
        let db = try Database(path: Paths.dbFile)
        try Schema.ensureSnapshotsTable(db)
        try Schema.ensureMoverErrorsTable(db)
        let ok = verifyAllSealed(
            db: db, record: opts.record, quiet: opts.quiet,
            dryRun: opts.dryRun)
        if opts.record { MoverErrors.escalate(db, log: log) }
        rc = ok ? 0 : 1
    } catch {
        FileHandle.standardError.write(
            Data("verify-all failed: \(error)\n".utf8))
        rc = 1
    }
case .showErrors:
    rc = showErrors()
case .clearErrors:
    rc = clearErrors()
case let .clearError(n):
    rc = clearError(n: n)
}
exit(rc)
