// swift-tools-version:5.7
//
// Swift Package Manager manifest for the Browser Visit Logger native helpers.
//
// Right now this only contains BVLVerifier, a deliberately minimal target
// whose sole purpose is to validate that a Swift Mach-O binary inside a
// code-signed app bundle inherits TCC permissions the way Python scripts
// don't.  Once that's confirmed end-to-end, this package grows to host
// the full port — see the PR description for the migration plan.
//
// Build:    swift build -c release
// Output:   .build/release/BVLVerifier  (Mach-O for the host architecture)
//
import PackageDescription

let package = Package(
    name: "browser-visit-logger",
    platforms: [
        .macOS(.v12),
    ],
    targets: [
        .executableTarget(
            name: "BVLVerifier",
            path: "Sources/BVLVerifier"
        ),
    ]
)
