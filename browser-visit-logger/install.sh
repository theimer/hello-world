#!/usr/bin/env bash
# install.sh — Sets up the Browser Visit Logger native messaging host
# and the verifier LaunchAgent.
#
# What this script does:
#   1. Generates a 2048-bit RSA key pair (once) so the Chrome extension
#      gets a deterministic, stable ID based on the public key.
#   2. Embeds the base64 public key into extension/manifest.json.
#   3. Computes the resulting Extension ID (SHA-256 of DER pubkey,
#      nibble-mapped to letters a-p, first 32 chars).
#   4. On macOS, builds two ad-hoc-signed .app bundles:
#        BrowserVisitLoggerHost.app — wraps native-host/host.py.  Chrome
#          spawns this when the extension sends a native message; the
#          bundle gives host.py a stable TCC identity so the user can
#          grant it ~/Downloads + ~/Documents access.
#        BrowserVisitLoggerVerifier.app — wraps native-host/snapshot_verifier.py.
#          launchd spawns this on the verifier's interval; the bundle
#          gives the verifier a separate stable TCC identity for the
#          same reason.
#   5. Installs the native messaging host manifest (with the host
#      bundle's executable as `path` + the extension ID).
#   6. Cleans up any previous-generation snapshot_mover LaunchAgent
#      (the mover is no longer a separate background process).
#   7. Installs the verifier LaunchAgent (default cadence: 86400 s = 1 day),
#      pointing it at the verifier bundle's executable.
#   8. Kicks the verifier once interactively so its first run triggers
#      the macOS Files & Folders prompt while the user is at the keyboard.
#   9. Prints next-step instructions.
#
# Usage:
#   bash install.sh
#
# Requirements: python3, openssl.  On macOS, codesign (always available).

set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve the repo root (directory containing this script)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXTENSION_DIR="$SCRIPT_DIR/extension"
NATIVE_DIR="$SCRIPT_DIR/native-host"
SWIFT_DIR="$SCRIPT_DIR/swift"
MANIFEST_TEMPLATE="$EXTENSION_DIR/manifest.json.template"
MANIFEST_JSON="$EXTENSION_DIR/manifest.json"
HOST_MANIFEST_TEMPLATE="$NATIVE_DIR/com.browser.visit.logger.json"
VERIFIER_PLIST_TEMPLATE="$NATIVE_DIR/com.browser.visit.logger.snapshot_verifier.plist.template"
VERIFIER_PLIST_LABEL="com.browser.visit.logger.snapshot_verifier"
# Cleaned up below if present from a previous-generation install:
MOVER_PLIST_LABEL="com.browser.visit.logger.snapshot_mover"
KEY_PEM="$NATIVE_DIR/generated_key.pem"
HOST_NAME="com.browser.visit.logger"

# Where the .app bundles are materialized on macOS.  Picked under
# Application Support so they're per-user, out of the way, and not
# subject to anyone's iCloud sync.
APP_PARENT="$HOME/Library/Application Support/browser-visit-logger"
HOST_APP_NAME="BrowserVisitLoggerHost"
HOST_APP="$APP_PARENT/$HOST_APP_NAME.app"
HOST_APP_BUNDLE_ID="com.browser.visit.logger.host"
VERIFIER_APP_NAME="BrowserVisitLoggerVerifier"
VERIFIER_APP="$APP_PARENT/$VERIFIER_APP_NAME.app"
VERIFIER_APP_BUNDLE_ID="com.browser.visit.logger.verifier"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "[install] $*"; }
error() { echo "[install] ERROR: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || error "'$1' is required but not found in PATH."
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
require_cmd python3
require_cmd openssl

OS="$(uname -s)"
if [[ "$OS" == "Darwin" ]]; then
  require_cmd codesign
  require_cmd swift
fi

# ---------------------------------------------------------------------------
# Step 1: Generate RSA key pair (skip if already done)
# ---------------------------------------------------------------------------
if [[ ! -f "$KEY_PEM" ]]; then
  info "Generating 2048-bit RSA key pair at $KEY_PEM ..."
  openssl genrsa -out "$KEY_PEM" 2048 2>/dev/null
  info "Key pair generated."
else
  info "RSA key already exists at $KEY_PEM, skipping generation."
fi

# ---------------------------------------------------------------------------
# Step 2: Extract DER-encoded public key and base64-encode it for manifest.json
# ---------------------------------------------------------------------------
DER_TMP="$(mktemp)"
trap 'rm -f "$DER_TMP"' EXIT

openssl rsa -in "$KEY_PEM" -pubout -outform DER -out "$DER_TMP" 2>/dev/null

# Base64-encode the DER (no line breaks — Chrome wants a single long string)
PUBLIC_KEY_B64="$(base64 < "$DER_TMP" | tr -d '\n')"

# Generate manifest.json from the template, embedding the public key
info "Generating $MANIFEST_JSON from template ..."
python3 - "$MANIFEST_TEMPLATE" "$MANIFEST_JSON" "$PUBLIC_KEY_B64" <<'PYEOF'
import json, sys
template_path, out_path, key_b64 = sys.argv[1], sys.argv[2], sys.argv[3]
with open(template_path) as f:
    data = json.load(f)
data['key'] = key_b64
with open(out_path, 'w') as f:
    json.dump(data, f, indent=2)
    f.write('\n')
PYEOF

# ---------------------------------------------------------------------------
# Step 3: Compute the Extension ID from the DER public key
# ---------------------------------------------------------------------------
EXTENSION_ID="$(python3 - "$DER_TMP" <<'PYEOF'
import sys, hashlib
with open(sys.argv[1], 'rb') as f:
    der = f.read()
digest = hashlib.sha256(der).digest()
# Chrome maps each nibble to the letters a-p (a=0, b=1, ..., p=15)
chars = ''.join(
    chr(ord('a') + b)
    for byte in digest[:16]
    for b in (byte >> 4, byte & 0xf)
)
print(chars)
PYEOF
)"

info "Extension ID: $EXTENSION_ID"

# ---------------------------------------------------------------------------
# Step 4 (macOS): Build the Swift Mach-O binaries up front so the
# bundles can be assembled with their final executables in one shot.
#
# Why a Mach-O binary, not a shell-script entrypoint: TCC attributes
# file accesses against the *running binary's* code signature.  When
# the bundle's executable is a shell script, the chain is bash → env
# → python3, and by the time the actual work runs the running binary
# is /usr/bin/python3 — which has its own (Apple) signature, not the
# bundle's, so the user's FDA grant for the bundle doesn't apply.  A
# Mach-O binary signed as part of the bundle keeps the bundle's
# identity all the way to syscall time.
# ---------------------------------------------------------------------------
if [[ "$OS" == "Darwin" ]]; then
  info "Building Swift binaries (this may take ~20 s on first run)..."
  ( cd "$SWIFT_DIR" && swift build -c release ) >/dev/null \
    || error "swift build failed; cd $SWIFT_DIR && swift build to see the error."
  SWIFT_HOST_BIN="$SWIFT_DIR/.build/release/BVLHost"
  SWIFT_VERIFIER_BIN="$SWIFT_DIR/.build/release/BVLVerifier"
  [[ -x "$SWIFT_HOST_BIN"     ]] || error "BVLHost binary missing at $SWIFT_HOST_BIN"
  [[ -x "$SWIFT_VERIFIER_BIN" ]] || error "BVLVerifier binary missing at $SWIFT_VERIFIER_BIN"
fi

# ---------------------------------------------------------------------------
# Step 5 (macOS): Build code-signed .app bundles for host and verifier
# with the freshly-built Mach-O binaries as their main executables.
# ---------------------------------------------------------------------------
build_app_bundle() {
  local app_dir="$1"           # e.g. .../BrowserVisitLoggerHost.app
  local bundle_id="$2"         # e.g. com.browser.visit.logger.host
  local exec_name="$3"         # e.g. BrowserVisitLoggerHost
  local source_bin="$4"        # path to a built Mach-O binary

  mkdir -p "$app_dir/Contents/MacOS"
  mkdir -p "$app_dir/Contents/Resources"

  # Info.plist — minimum required for TCC to recognize the bundle as a
  # first-class app.  LSUIElement=true keeps it out of the Dock /
  # Cmd-Tab; it's a background helper, not a user-facing app.
  cat > "$app_dir/Contents/Info.plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleIdentifier</key>
  <string>$bundle_id</string>
  <key>CFBundleName</key>
  <string>$exec_name</string>
  <key>CFBundleExecutable</key>
  <string>$exec_name</string>
  <key>CFBundleVersion</key>
  <string>1</string>
  <key>CFBundleShortVersionString</key>
  <string>1.0</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>LSUIElement</key>
  <true/>
</dict>
</plist>
EOF

  cp "$source_bin" "$app_dir/Contents/MacOS/$exec_name"
  chmod +x "$app_dir/Contents/MacOS/$exec_name"

  # Ad-hoc sign the bundle.  --force overwrites any prior signature;
  # --deep covers any nested binaries (none here, but harmless).
  codesign --force --deep --sign - "$app_dir" 2>/dev/null
  info "Built and signed $app_dir"
}

if [[ "$OS" == "Darwin" ]]; then
  mkdir -p "$APP_PARENT"
  build_app_bundle \
    "$HOST_APP" "$HOST_APP_BUNDLE_ID" "$HOST_APP_NAME" "$SWIFT_HOST_BIN"
  build_app_bundle \
    "$VERIFIER_APP" "$VERIFIER_APP_BUNDLE_ID" "$VERIFIER_APP_NAME" \
    "$SWIFT_VERIFIER_BIN"
  HOST_BUNDLE_EXEC="$HOST_APP/Contents/MacOS/$HOST_APP_NAME"
  VERIFIER_BUNDLE_EXEC="$VERIFIER_APP/Contents/MacOS/$VERIFIER_APP_NAME"
else
  # On Linux there's no TCC, but the Swift port is macOS-only.  Linux
  # users would need to provide their own native-messaging host; for
  # now the install just wires the manifest at the unbuilt Swift dir,
  # which won't actually run anything.  Real Linux support is out of
  # scope.
  HOST_BUNDLE_EXEC="$SWIFT_DIR/.build/release/BVLHost"
  VERIFIER_BUNDLE_EXEC="$SWIFT_DIR/.build/release/BVLVerifier"
fi

# ---------------------------------------------------------------------------
# Step 6: Build the filled-in native host manifest and install it.
#
# The manifest's `path` field is what Chrome execs when the extension
# sends a native message.  Pointing at the host bundle (rather than
# host.py directly) is what gives the spawned process its TCC identity.
# ---------------------------------------------------------------------------
FILLED_MANIFEST="$(python3 - "$HOST_MANIFEST_TEMPLATE" "$HOST_BUNDLE_EXEC" "$EXTENSION_ID" <<'PYEOF'
import json, sys
template_path, host_path, ext_id = sys.argv[1], sys.argv[2], sys.argv[3]
with open(template_path) as f:
    data = json.load(f)
data['path'] = host_path
data['allowed_origins'] = [f'chrome-extension://{ext_id}/']
print(json.dumps(data, indent=2))
PYEOF
)"

INSTALLED=0

if [[ "$OS" == "Darwin" ]]; then
  install_manifest() {
    local dir="$1"
    local label="$2"
    mkdir -p "$dir"
    echo "$FILLED_MANIFEST" > "$dir/$HOST_NAME.json"
    info "Installed native host manifest for $label at $dir/$HOST_NAME.json"
    INSTALLED=1
  }

  CHROME_APP="$HOME/Library/Application Support/Google/Chrome"
  CANARY_APP="$HOME/Library/Application Support/Google/Chrome Canary"
  CHROMIUM_APP="$HOME/Library/Application Support/Chromium"

  [[ -d "$CHROME_APP" ]]   && install_manifest "$CHROME_APP/NativeMessagingHosts"   "Chrome"
  [[ -d "$CANARY_APP" ]]   && install_manifest "$CANARY_APP/NativeMessagingHosts"   "Chrome Canary"
  [[ -d "$CHROMIUM_APP" ]] && install_manifest "$CHROMIUM_APP/NativeMessagingHosts" "Chromium"

  # Fallback: Chrome wasn't open yet so its profile dir doesn't exist
  if [[ $INSTALLED -eq 0 ]]; then
    install_manifest "$CHROME_APP/NativeMessagingHosts" "Chrome (pre-created)"
  fi

else
  # Linux
  CHROME_DIR="$HOME/.config/google-chrome/NativeMessagingHosts"
  CHROMIUM_DIR="$HOME/.config/chromium/NativeMessagingHosts"

  if [[ -d "$HOME/.config/google-chrome" ]] || command -v google-chrome >/dev/null 2>&1 || command -v google-chrome-stable >/dev/null 2>&1; then
    mkdir -p "$CHROME_DIR"
    echo "$FILLED_MANIFEST" > "$CHROME_DIR/$HOST_NAME.json"
    info "Installed native host manifest for Chrome at $CHROME_DIR/$HOST_NAME.json"
    INSTALLED=1
  fi

  if [[ -d "$HOME/.config/chromium" ]] || command -v chromium >/dev/null 2>&1 || command -v chromium-browser >/dev/null 2>&1; then
    mkdir -p "$CHROMIUM_DIR"
    echo "$FILLED_MANIFEST" > "$CHROMIUM_DIR/$HOST_NAME.json"
    info "Installed native host manifest for Chromium at $CHROMIUM_DIR/$HOST_NAME.json"
    INSTALLED=1
  fi

  if [[ $INSTALLED -eq 0 ]]; then
    mkdir -p "$CHROME_DIR" "$CHROMIUM_DIR"
    echo "$FILLED_MANIFEST" > "$CHROME_DIR/$HOST_NAME.json"
    echo "$FILLED_MANIFEST" > "$CHROMIUM_DIR/$HOST_NAME.json"
    info "No browser config dir detected; installed manifest to both Chrome and Chromium locations."
  fi
fi

# ---------------------------------------------------------------------------
# Step 7 (macOS only): Cleanup of previous-generation snapshot_mover
# LaunchAgent.  The mover is no longer a separate background process —
# its responsibilities are split between host.py (synchronous archive
# at tag time) and the verifier (sweep + seal + verify on a daily tick).
# ---------------------------------------------------------------------------
VERIFIER_INSTALLED=0
if [[ "$OS" == "Darwin" ]]; then
  USER_DOMAIN="gui/$(id -u)"
  LAUNCHAGENTS_DIR="$HOME/Library/LaunchAgents"
  OLD_MOVER_PLIST="$LAUNCHAGENTS_DIR/$MOVER_PLIST_LABEL.plist"

  if launchctl print "$USER_DOMAIN/$MOVER_PLIST_LABEL" >/dev/null 2>&1; then
    launchctl bootout "$USER_DOMAIN/$MOVER_PLIST_LABEL" 2>/dev/null || true
    info "Booted out the obsolete snapshot_mover LaunchAgent."
  fi
  if [[ -f "$OLD_MOVER_PLIST" ]]; then
    rm -f "$OLD_MOVER_PLIST"
    info "Removed obsolete plist $OLD_MOVER_PLIST"
  fi

  # ---------------------------------------------------------------------
  # Step 8: Install the verifier LaunchAgent.
  #
  # ProgramArguments points at the verifier bundle's executable, not at
  # python3 directly, so launchd-spawned ticks attribute to the bundle's
  # TCC identity.
  # ---------------------------------------------------------------------
  VERIFIER_PLIST="$LAUNCHAGENTS_DIR/$VERIFIER_PLIST_LABEL.plist"
  mkdir -p "$LAUNCHAGENTS_DIR"

  python3 - "$VERIFIER_PLIST_TEMPLATE" "$VERIFIER_PLIST" "$VERIFIER_BUNDLE_EXEC" "$HOME" <<'PYEOF'
import sys
template_path, out_path, exec_path, home = sys.argv[1:5]
with open(template_path) as f:
    text = f.read()
text = text.replace('{{VERIFIER_EXEC}}', exec_path).replace('{{HOME}}', home)
with open(out_path, 'w') as f:
    f.write(text)
PYEOF
  info "Wrote LaunchAgent plist to $VERIFIER_PLIST"

  launchctl bootout "$USER_DOMAIN/$VERIFIER_PLIST_LABEL" 2>/dev/null || true
  if launchctl bootstrap "$USER_DOMAIN" "$VERIFIER_PLIST" 2>/dev/null; then
    info "Verifier LaunchAgent loaded — runs every 86400s (1 day) by default."
    VERIFIER_INSTALLED=1
  else
    info "WARNING: failed to bootstrap verifier LaunchAgent; you may need to log out/in or run 'launchctl bootstrap $USER_DOMAIN $VERIFIER_PLIST' manually."
  fi

  # -------------------------------------------------------------------
  # Step 9: Kick the verifier once interactively so the first
  # ~/Downloads access happens while the user is at the keyboard and
  # macOS can pop the Files & Folders consent dialog.  Without this,
  # launchd may silently drop the prompt if the next scheduled tick
  # fires when no user session is active.
  # -------------------------------------------------------------------
  if [[ $VERIFIER_INSTALLED -eq 1 ]]; then
    launchctl kickstart -k "$USER_DOMAIN/$VERIFIER_PLIST_LABEL" \
      2>/dev/null || true
    info "Kicked the verifier — accept the Files & Folders prompt if it appears."
  fi
else
  info "Verifier LaunchAgent is macOS-only; skipping on $OS."
fi

# ---------------------------------------------------------------------------
# Step 10: Print next-step instructions
# ---------------------------------------------------------------------------
cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Browser Visit Logger — Installation complete
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Bundles installed (Swift Mach-O entrypoints, ad-hoc-signed):
  $HOST_BUNDLE_EXEC
  $VERIFIER_BUNDLE_EXEC

Extension ID    : $EXTENSION_ID
Extension dir   : $EXTENSION_DIR
Verifier        : $VERIFIER_PLIST_LABEL (LaunchAgent, every 86400s = 1 day)

If this is a fresh install:

  1. Open Chrome → chrome://extensions, enable Developer mode, click
     "Load unpacked" and select:
       $EXTENSION_DIR
     The extension ID shown should match: $EXTENSION_ID

  2. Tag any page (★ / ✓ / ~).  macOS will pop a Files & Folders
     prompt for "$HOST_APP_NAME" the first time — click "Allow".
     The bundle's grant covers both ~/Downloads and ~/Documents.

  3. The verifier's first scheduled tick will pop a similar prompt
     for "$VERIFIER_APP_NAME"; click Allow when it appears.  To
     trigger it now (interactively, while you're at the keyboard):
       launchctl kickstart -k gui/\$(id -u)/$VERIFIER_PLIST_LABEL

If you re-ran install.sh (e.g. after a code change), the bundles'
codesign hashes changed — your existing FDA / Files-and-Folders
grants are bound to the OLD hashes and need to be re-granted:

  System Settings → Privacy & Security → Full Disk Access
    → remove "$HOST_APP_NAME" and "$VERIFIER_APP_NAME"
    → re-add the bundles (drag from Finder or click +):
        $HOST_APP
        $VERIFIER_APP
    → toggle each ON
  Then quit Chrome fully (Cmd-Q) and reopen so it picks up the new
  native-messaging manifest path.

Verify the install:
  tail ~/browser-visits-host.log          # archive lines per tag
  tail ~/browser-visits-verifier.log      # tick output
  sqlite3 ~/browser-visits.db "SELECT * FROM visits ORDER BY timestamp DESC LIMIT 10;"

Change the verifier cadence:
  • Edit StartInterval in $VERIFIER_PLIST_LABEL.plist
  • Reload:
      launchctl bootout   gui/\$(id -u)/$VERIFIER_PLIST_LABEL
      launchctl bootstrap gui/\$(id -u) ~/Library/LaunchAgents/$VERIFIER_PLIST_LABEL.plist

Manual verifier ops (delegated to the Swift binary):
  ./verify_snapshot_directory                 # full tick
  ./verify_snapshot_directory --verify-all    # verify pass only
  ./verify_snapshot_directory --show-errors   # inspect mover_errors
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EOF
