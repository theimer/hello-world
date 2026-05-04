// BVLVerifier — TCC validation binary.
//
// This deliberately minimal Swift program replaces the verifier bundle's
// shell-script entrypoint for one reason: to test whether a Swift Mach-O
// binary inside a code-signed `.app` bundle inherits the bundle's TCC
// grants when launchd-spawned, the way the previous /bin/bash → env →
// python3 chain did NOT.
//
// What it does each launchd tick:
//
//   1. Try to listdir ~/Downloads/browser-visit-snapshots — the canonical
//      TCC-protected target the production verifier needs to read.
//   2. Try to listdir ~/Documents — the other TCC-protected target the
//      production verifier writes into.
//   3. Print a structured result to stdout.  The LaunchAgent plist's
//      StandardOutPath captures it to ~/browser-visits-verifier.log.
//   4. Exit 0 on success, 1 on any TCC denial.
//
// Once we've confirmed both reads succeed (no EPERM), the rest of the
// port can replace this binary with the real BVLVerifier that does
// sweep + seal + verify + escalate.  If even this minimal binary still
// hits EPERM, ad-hoc-signed bundles aren't enough on this macOS version
// and the project pivots to one of the non-bundle alternatives
// (changing Chrome's downloads dir, or extension-side byte streaming).
//
// While this binary is in place, the verifier is in *test mode* — none
// of the production housekeeping (sweep / seal / verify) runs.  That's
// fine; the daily LaunchAgent will continue to fire, log a result, and
// exit.  The host bundle (which actually archives snapshots at tag time)
// is unaffected by this change.

import Foundation

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Prefix every line so the captured log is unambiguous about which
/// process produced it (Swift validation vs. the Python verifier we may
/// be co-running with during the cutover).
func log(_ s: String) {
    let ts = ISO8601DateFormatter().string(from: Date())
    print("[BVLVerifier-swift \(ts)] \(s)")
}

/// Try to list a TCC-protected directory.  Returns nil on success, or
/// a human-readable failure description on EPERM / other I/O errors.
func probe(_ path: String) -> String? {
    do {
        let entries = try FileManager.default.contentsOfDirectory(atPath: path)
        log("OK: \(path) — \(entries.count) entries")
        return nil
    } catch let error as NSError {
        log("FAIL: \(path) — \(error.localizedDescription) "
            + "(domain=\(error.domain), code=\(error.code))")
        return error.localizedDescription
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

let home = NSHomeDirectory()
let downloadsSnapshots = (home as NSString)
    .appendingPathComponent("Downloads/browser-visit-snapshots")
let documents = (home as NSString)
    .appendingPathComponent("Documents")

log("starting TCC validation — pid=\(getpid()) uid=\(getuid())")
log("running binary: \(CommandLine.arguments[0])")

// Probe Downloads (where Chrome drops snapshots — the EPERM target the
// previous shell-script bundle hit).
let downloadsErr = probe(downloadsSnapshots)

// Probe Documents (where the iCloud archive lives).  This is also
// TCC-protected on modern macOS; the production code writes here.
let documentsErr = probe(documents)

if downloadsErr == nil && documentsErr == nil {
    log("RESULT: Swift bundle has TCC for both Downloads and Documents.")
    log("Next step: replace this binary with the real BVLVerifier port.")
    exit(0)
} else {
    log("RESULT: at least one probe failed — bundle TCC is not in effect.")
    log("Next step: confirm bundle is in System Settings → Privacy & "
        + "Security → Full Disk Access (or Files and Folders).  If it is "
        + "and probes still fail, ad-hoc signing is insufficient on this "
        + "macOS version and the project should pivot away from bundles.")
    exit(1)
}
