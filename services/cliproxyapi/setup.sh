#!/usr/bin/env bash
# CLIProxyAPI setup — install, configure, authenticate, start
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONFIG_DIR="$HOME/.cli-proxy-api"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'
log()  { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
err()  { echo -e "${RED}[✗]${NC} $1"; }
step() { echo -e "\n${CYAN}${BOLD}--- $1 ---${NC}"; }

normalize_gemini_auth_files() {
  local auth_dir="$1"
  python3 - "$auth_dir" <<'EOF'
import json
import sys
from pathlib import Path
base = Path(sys.argv[1]).expanduser()
if not base.exists():
    raise SystemExit(0)
for path in sorted(base.glob('gemini-*.json')):
    try:
        data = json.loads(path.read_text())
    except Exception:
        continue
    email = data.get('email')
    project_id = data.get('project_id')
    if not email or not project_id:
        continue
    target = base / f"{email}-gen-lang-client-{project_id}.json"
    if target.exists():
        continue
    target.write_text(path.read_text())
    target.chmod(0o600)
    print(target.name)
EOF
}

# ─── Step 1: Install ─────────────────────────────────────────────────
step "1/6 Install CLIProxyAPI"
if command -v cliproxyapi &>/dev/null; then
  INSTALLED_VER=$(cliproxyapi --version 2>/dev/null || echo "unknown")
  log "Already installed: $INSTALLED_VER"

  # Check for upgrades
  LATEST=$(brew info cliproxyapi 2>/dev/null | head -1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  CURRENT=$(brew list --versions cliproxyapi 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
  if [ -n "$LATEST" ] && [ -n "$CURRENT" ] && [ "$LATEST" != "$CURRENT" ]; then
    warn "Upgrade available: $CURRENT → $LATEST"
    read -p "  Upgrade now? [y/N]: " upgrade
    if [[ "${upgrade:-n}" =~ ^[Yy] ]]; then
      brew upgrade cliproxyapi
      log "Upgraded to $LATEST"
    fi
  fi
else
  if ! command -v brew &>/dev/null; then
    err "Homebrew not found. Install: /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
    exit 1
  fi
  brew install cliproxyapi
  log "Installed $(cliproxyapi --version 2>/dev/null || echo 'cliproxyapi')"
fi

# ─── Step 2: Config ──────────────────────────────────────────────────
step "2/6 Configuration"
mkdir -p "$CONFIG_DIR"

if [ -f "$CONFIG_DIR/config.yaml" ]; then
  log "Config exists: $CONFIG_DIR/config.yaml"
  warn "Not overwriting. To reset: cp $SCRIPT_DIR/config.yaml.template $CONFIG_DIR/config.yaml"
else
  cp "$SCRIPT_DIR/config.yaml.template" "$CONFIG_DIR/config.yaml"
  log "Created config: $CONFIG_DIR/config.yaml"
  warn "Edit the config to add your OAuth credentials and model aliases"
fi

# ─── Step 3: OAuth Authentication ─────────────────────────────────────
step "3/6 OAuth Authentication"
echo "  CLIProxyAPI uses OAuth to authenticate with Claude, Codex, and Gemini."
echo "  Each provider needs a browser login (one-time per account)."
echo ""

# Normalize Gemini auth filenames into CLIProxy's scanned format
NEW_GEMINI_FILES=$(normalize_gemini_auth_files "$CONFIG_DIR" || true)
if [ -n "${NEW_GEMINI_FILES:-}" ]; then
  log "Normalized Gemini auth files:
$NEW_GEMINI_FILES"
fi

# Check existing auth files
CLAUDE_AUTH=$(ls "$CONFIG_DIR"/claude-*.json 2>/dev/null | wc -l | tr -d ' ')
CODEX_AUTH=$(ls "$CONFIG_DIR"/codex-*.json 2>/dev/null | wc -l | tr -d ' ')
ANTIGRAV_AUTH=$(ls "$CONFIG_DIR"/antigravity-*.json 2>/dev/null | wc -l | tr -d ' ')
GEMINI_AUTH=$(find "$CONFIG_DIR" -maxdepth 1 -type f \( -name '*gen-lang-client-*.json' -o -name 'gemini-*.json' \) 2>/dev/null | wc -l | tr -d ' ')

echo "  Current auth state:"
echo "    Claude accounts:      $CLAUDE_AUTH"
echo "    Codex accounts:       $CODEX_AUTH"
echo "    Antigravity accounts: $ANTIGRAV_AUTH"
echo "    Gemini accounts:      $GEMINI_AUTH"
echo ""

if [ "$CLAUDE_AUTH" -eq 0 ] && [ "$CODEX_AUTH" -eq 0 ] && [ "$GEMINI_AUTH" -eq 0 ]; then
  warn "No OAuth credentials found. You need to authenticate at least one provider."
  echo ""
  echo "  To add accounts (run these after setup completes):"
  echo ""
  echo "    # Claude (opens browser)"
  echo "    cliproxyapi --claude-login"
  echo ""
  echo "    # Codex (opens browser)"
  echo "    cliproxyapi --codex-login"
  echo ""
  echo "    # Gemini via Antigravity (opens browser)"
  echo "    cliproxyapi --antigravity-login"
  echo ""
else
  log "Auth credentials found — $((CLAUDE_AUTH + CODEX_AUTH + ANTIGRAV_AUTH + GEMINI_AUTH)) accounts total"
fi

# ─── Step 4: Usage Guard ─────────────────────────────────────────────
step "4/6 Usage Guard"
if [ -f "$SCRIPT_DIR/usage-guard.py" ]; then
  cp "$SCRIPT_DIR/usage-guard.py" "$HOME/.local/bin/" 2>/dev/null && chmod +x "$HOME/.local/bin/usage-guard.py" || true
  cp "$SCRIPT_DIR/usage-guard.json" "$CONFIG_DIR/" 2>/dev/null || true
  log "Usage guard installed"
else
  warn "Usage guard not found in fullstackOS — skipping"
fi

# ─── Step 5: Start Service ───────────────────────────────────────────
step "5/6 Start Service"
if brew services list 2>/dev/null | grep -q "cliproxyapi.*started"; then
  log "CLIProxyAPI service already running"
else
  brew services start cliproxyapi 2>/dev/null || true
  sleep 2
  if curl -sf http://127.0.0.1:8317/ &>/dev/null; then
    log "Service started and healthy on port 8317"
  else
    warn "Service started but not responding yet — may need a few seconds"
  fi
fi

# ─── Step 6: Verify ──────────────────────────────────────────────────
step "6/6 Verification"
echo ""

# Health check
if curl -sf http://127.0.0.1:8317/ &>/dev/null; then
  log "Health: OK (port 8317)"
else
  err "Health: FAILED — service not responding"
fi

# Model listing
MODELS=$(curl -sf -H "Authorization: Bearer your-proxy-key" http://127.0.0.1:8317/v1/models 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('data',[])))" 2>/dev/null || echo "0")
if [ "$MODELS" -gt 0 ]; then
  log "Models: $MODELS models available"
else
  warn "Models: 0 — authenticate at least one provider (see Step 3)"
fi

# Auth files
TOTAL_AUTH=$((CLAUDE_AUTH + CODEX_AUTH + ANTIGRAV_AUTH + GEMINI_AUTH))
if [ "$TOTAL_AUTH" -gt 0 ]; then
  log "Auth: $TOTAL_AUTH credential files in $CONFIG_DIR/"
else
  warn "Auth: No credentials — run 'cliproxyapi --claude-login' to authenticate"
fi

echo ""
echo -e "${BOLD}CLIProxyAPI setup complete.${NC}"
echo ""
echo "  Config:  $CONFIG_DIR/config.yaml"
echo "  Service: brew services info cliproxyapi"
echo "  Health:  curl http://127.0.0.1:8317/"
echo "  Models:  curl -H 'Authorization: Bearer your-proxy-key' http://127.0.0.1:8317/v1/models"
echo ""
echo "  Add accounts:"
echo "    cliproxyapi --claude-login"
echo "    cliproxyapi --codex-login"
echo "    cliproxyapi --antigravity-login"
echo ""
