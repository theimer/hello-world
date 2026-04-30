#!/usr/bin/env bash
# install.sh — Sets up the Browser Visit Logger native messaging host.
#
# What this script does:
#   1. Generates a 2048-bit RSA key pair (once) so the Chrome extension gets a
#      deterministic, stable ID based on the public key.
#   2. Embeds the base64 public key into extension/manifest.json ("key" field).
#   3. Computes the resulting Extension ID (SHA-256 of DER pubkey, nibble-mapped
#      to letters a-p, first 32 chars).
#   4. Makes native-host/host.py executable.
#   5. Installs the native messaging host manifest (with real path + extension ID)
#      to the correct NativeMessagingHosts directory for the OS and browser.
#   6. Prints instructions for loading the unpacked extension in Chrome.
#
# Usage:
#   bash install.sh
#
# Requirements: python3, openssl

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
MOVER_PY="$NATIVE_DIR/snapshot_mover.py"
MOVER_PLIST_TEMPLATE="$NATIVE_DIR/com.browser.visit.logger.snapshot_mover.plist.template"
MOVER_PLIST_LABEL="com.browser.visit.logger.snapshot_mover"
KEY_PEM="$NATIVE_DIR/generated_key.pem"
HOST_NAME="com.browser.visit.logger"

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
# Step 4: Make host.py executable
# ---------------------------------------------------------------------------
chmod +x "$HOST_PY"
info "Made $HOST_PY executable."

# Resolve the absolute real path to host.py (no symlinks)
HOST_PY_ABS="$(python3 -c "import os, sys; print(os.path.realpath(sys.argv[1]))" "$HOST_PY")"

# ---------------------------------------------------------------------------
# Step 5: Build the filled-in native host manifest and install it
# ---------------------------------------------------------------------------
FILLED_MANIFEST="$(python3 - "$HOST_MANIFEST_TEMPLATE" "$HOST_PY_ABS" "$EXTENSION_ID" <<'PYEOF'
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
OS="$(uname -s)"

if [[ "$OS" == "Darwin" ]]; then
  # macOS — Chrome reads from ~/Library/Application Support/...
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
# Step 5b: Install snapshot mover LaunchAgent (macOS only)
# ---------------------------------------------------------------------------
MOVER_INSTALLED=0
if [[ "$OS" == "Darwin" ]]; then
  chmod +x "$MOVER_PY"
  MOVER_PY_ABS="$(python3 -c "import os, sys; print(os.path.realpath(sys.argv[1]))" "$MOVER_PY")"

  LAUNCHAGENTS_DIR="$HOME/Library/LaunchAgents"
  MOVER_PLIST="$LAUNCHAGENTS_DIR/$MOVER_PLIST_LABEL.plist"
  mkdir -p "$LAUNCHAGENTS_DIR"

  # Substitute placeholders in the plist template.
  python3 - "$MOVER_PLIST_TEMPLATE" "$MOVER_PLIST" "$MOVER_PY_ABS" "$HOME" <<'PYEOF'
import sys
template_path, out_path, mover_path, home = sys.argv[1:5]
with open(template_path) as f:
    text = f.read()
text = text.replace('{{MOVER_PATH}}', mover_path).replace('{{HOME}}', home)
with open(out_path, 'w') as f:
    f.write(text)
PYEOF
  info "Wrote LaunchAgent plist to $MOVER_PLIST"

  # (Re-)load the LaunchAgent.  bootout returns non-zero if not currently
  # loaded; that's fine, hence the leading `||true`.
  USER_DOMAIN="gui/$(id -u)"
  launchctl bootout "$USER_DOMAIN/$MOVER_PLIST_LABEL" 2>/dev/null || true
  if launchctl bootstrap "$USER_DOMAIN" "$MOVER_PLIST" 2>/dev/null; then
    info "LaunchAgent loaded — mover runs every hour by default."
    MOVER_INSTALLED=1
  else
    info "WARNING: failed to bootstrap LaunchAgent; you may need to log out/in or run 'launchctl bootstrap $USER_DOMAIN $MOVER_PLIST' manually."
  fi
else
  info "Snapshot mover LaunchAgent is macOS-only; skipping on $OS."
fi

# ---------------------------------------------------------------------------
# Step 6: Print next-step instructions
# ---------------------------------------------------------------------------
cat <<EOF

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Browser Visit Logger — Installation complete
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Extension ID : $EXTENSION_ID
Extension dir: $EXTENSION_DIR
Native host  : $HOST_PY_ABS
Snapshot mover: $([[ "$OS" == "Darwin" ]] && echo "$MOVER_PLIST_LABEL (LaunchAgent, every 3600s)" || echo "macOS-only, not installed")

Next steps:
  1. Open Chrome and go to: chrome://extensions
  2. Enable "Developer mode" (top-right toggle).
  3. Click "Load unpacked" and select:
       $EXTENSION_DIR
  4. Verify the extension ID shown in Chrome matches:
       $EXTENSION_ID
     (It should — the "key" field in manifest.json pins the ID.)
  5. Navigate to any web page. Then check:
       tail ~/browser-visits.log
       sqlite3 ~/browser-visits.db "SELECT * FROM visits ORDER BY id DESC LIMIT 10;"

To change the snapshot mover interval (default 3600s):
  • Edit the StartInterval value in:
       ~/Library/LaunchAgents/$MOVER_PLIST_LABEL.plist
  • Reload it:
       launchctl bootout gui/\$(id -u)/$MOVER_PLIST_LABEL
       launchctl bootstrap gui/\$(id -u) ~/Library/LaunchAgents/$MOVER_PLIST_LABEL.plist
  • Mover output is logged to ~/browser-visits-mover.log

If Chrome shows a different extension ID, re-run this script to regenerate.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EOF
