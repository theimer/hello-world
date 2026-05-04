// swift-tools-version:5.7
//
// Swift Package Manager manifest for the Browser Visit Logger native helpers.
//
// Targets:
//   BVLCore     — shared library: SQLite wrapper, schema, archive logic,
//                 error tracking, snapshot filename parsing.
//   BVLHost     — Chrome native-messaging host.  Bundle's executable is
//                 a code-signed Mach-O so TCC attributes file accesses
//                 to the bundle's identity (whereas a shell-script
//                 entrypoint would lose attribution at exec time).
//   BVLVerifier — daily LaunchAgent.  Currently a TCC-validation probe;
//                 will absorb the verifier's tick logic in a follow-up.
//
// XCTest isn't available with the standalone Xcode Command Line Tools
// install, so there's no .testTarget here.  Test coverage for the
// ported logic comes from two complementary sources:
//   1. The Python suite exercises the equivalent code paths via
//      tests/test_*.py — same DB schema, same on-disk artifacts.
//   2. install.sh end-to-end smoke-tests the BVLHost binary against
//      a real Chrome native-messaging round-trip during the validation
//      step.
//
import PackageDescription

let package = Package(
    name: "browser-visit-logger",
    platforms: [
        .macOS(.v12),
    ],
    targets: [
        .target(
            name: "BVLCore",
            path: "Sources/BVLCore"
        ),
        .executableTarget(
            name: "BVLHost",
            dependencies: ["BVLCore"],
            path: "Sources/BVLHost"
        ),
        .executableTarget(
            name: "BVLVerifier",
            dependencies: ["BVLCore"],
            path: "Sources/BVLVerifier"
        ),
    ]
)
