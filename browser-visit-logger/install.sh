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
MANIFEST_TEMPLATE="$EXTENSION_DIR/manifest.json.template"
MANIFEST_JSON="$EXTENSION_DIR/manifest.json"
HOST_MANIFEST_TEMPLATE="$NATIVE_DIR/com.browser.visit.logger.json"
HOST_PY="$NATIVE_DIR/host.py"
VERIFIER_PY="$NATIVE_DIR/snapshot_verifier.py"
VERIFIER_PLIST_TEMPLATE="$NATIVE_DIR/com.browser.visit.logger.snapshot_verifier.plist.template"
VERIFIER_PLIST_LABEL="com.browser.visit.logger.snapshot_verifier"
# Removed (cleaned up below if present from a previous-generation install):
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
# Step 4: Make scripts executable
# ---------------------------------------------------------------------------
chmod +x "$HOST_PY"     "$VERIFIER_PY"
info "Made $HOST_PY and $VERIFIER_PY executable."

# Resolve absolute real paths (no symlinks)
HOST_PY_ABS="$(python3 -c "import os, sys; print(os.path.realpath(sys.argv[1]))" "$HOST_PY")"
VERIFIER_PY_ABS="$(python3 -c "import os, sys; print(os.path.realpath(sys.argv[1]))" "$VERIFIER_PY")"

# ---------------------------------------------------------------------------
# Step 5 (macOS only): Build code-signed .app bundles for host and verifier.
#
# An app bundle gives the wrapped Python script a stable TCC identity so
# the user can grant it Files & Folders or Full Disk Access in
# System Settings → Privacy & Security.  Without the bundle, both host.py
# (Chrome-spawned, no inherited TCC) and the verifier (launchd-spawned,
# no inherited TCC) hit EPERM on every ~/Downloads access.
#
# We re-create both bundles on every install.sh run so the signature
# stays in sync with the current host.py / snapshot_verifier.py contents.
# ---------------------------------------------------------------------------
build_app_bundle() {
  local app_dir="$1"           # e.g. ~/Library/Application Support/.../BrowserVisitLoggerHost.app
  local bundle_id="$2"         # e.g. com.browser.visit.logger.host
  local exec_name="$3"         # e.g. BrowserVisitLoggerHost
  local target_script="$4"     # absolute path to host.py / snapshot_verifier.py

  mkdir -p "$app_dir/Contents/MacOS"
  mkdir -p "$app_dir/Contents/Resources"

  # Info.plist — minimum required for TCC to recognize the bundle as a
  # first-class app.  LSUIElement=true keeps the bundle out of the Dock
  # and Cmd-Tab; it's a background helper, not a user-facing app.
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

  # The bundle's main executable: a tiny shell script that exec's into
  # python3 + the real script outside the bundle.  TCC attribution stays
  # with the bundle across the exec, so the user's grant follows.
  #
  # We use /usr/bin/env to find python3 instead of pinning it, because
  # /usr/bin/python3 may not exist on systems where Python is supplied
  # only via Xcode Command Line Tools or Homebrew.
  cat > "$app_dir/Contents/MacOS/$exec_name" <<EOF
#!/bin/bash
exec /usr/bin/env python3 "$target_script" "\$@"
EOF
  chmod +x "$app_dir/Contents/MacOS/$exec_name"

  # Ad-hoc sign the bundle.  --force overwrites any prior signature;
  # --deep covers any nested binaries (none here, but harmless).
  # Without --sign -, TCC may treat each modification as a different
  # app and revoke any existing grant.
  codesign --force --deep --sign - "$app_dir" 2>/dev/null
  info "Built and signed $app_dir"
}

if [[ "$OS" == "Darwin" ]]; then
  mkdir -p "$APP_PARENT"
  build_app_bundle \
    "$HOST_APP" "$HOST_APP_BUNDLE_ID" "$HOST_APP_NAME" "$HOST_PY_ABS"
  build_app_bundle \
    "$VERIFIER_APP" "$VERIFIER_APP_BUNDLE_ID" "$VERIFIER_APP_NAME" \
    "$VERIFIER_PY_ABS"
  HOST_BUNDLE_EXEC="$HOST_APP/Contents/MacOS/$HOST_APP_NAME"
  VERIFIER_BUNDLE_EXEC="$VERIFIER_APP/Contents/MacOS/$VERIFIER_APP_NAME"
else
  # On Linux there's no TCC; Chrome / launchd-equivalent (cron, systemd
  # user units) can read ~/Downloads without a bundle.  Use the bare
  # script paths.
  HOST_BUNDLE_EXEC="$HOST_PY_ABS"
  VERIFIER_BUNDLE_EXEC="$VERIFIER_PY_ABS"
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

Extension ID    : $EXTENSION_ID
Extension dir   : $EXTENSION_DIR
Native host     : $HOST_BUNDLE_EXEC
Verifier        : $([[ "$OS" == "Darwin" ]] && echo "$VERIFIER_PLIST_LABEL (LaunchAgent, every 86400s = 1 day)" || echo "macOS-only, not installed")

Next steps:
  1. Open Chrome and go to: chrome://extensions
  2. Enable "Developer mode" (top-right toggle).
  3. Click "Load unpacked" and select:
       $EXTENSION_DIR
  4. Verify the extension ID shown in Chrome matches:
       $EXTENSION_ID
  5. Tag any page (★ / ✓ / ~) once.  The first tag will trigger a
     macOS Files & Folders prompt for $HOST_APP_NAME — click "Allow".
     This grants host.py access to ~/Downloads (where Chrome drops
     snapshots) and ~/Documents (where the iCloud archive lives).
  6. If you didn't see the verifier's TCC prompt during install, the
     next daily verifier tick will surface it — or run the verifier
     manually from a terminal:
       ./verify_snapshot_directory --quiet

  7. Verify with:
       tail ~/browser-visits-\$(date -u +%Y-%m-%d).log
       sqlite3 ~/browser-visits.db "SELECT * FROM visits ORDER BY timestamp DESC LIMIT 10;"

To change the verifier interval (default 86400s = 1 day):
  • Edit the StartInterval value in:
       ~/Library/LaunchAgents/$VERIFIER_PLIST_LABEL.plist
  • Reload it:
       launchctl bootout gui/\$(id -u)/$VERIFIER_PLIST_LABEL
       launchctl bootstrap gui/\$(id -u) ~/Library/LaunchAgents/$VERIFIER_PLIST_LABEL.plist
  • Verifier output is logged to ~/browser-visits-verifier.log

To run the verifier on demand:
  ./verify_snapshot_directory                 # full tick
  ./verify_snapshot_directory --verify-all    # verify pass only
  ./verify_snapshot_directory --show-errors   # inspect mover_errors

If Chrome shows a different extension ID, re-run this script to regenerate.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EOF
