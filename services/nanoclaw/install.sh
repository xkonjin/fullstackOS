#!/bin/bash
set -euo pipefail

# nanoclaw install — deploy daemon, retire replaced agents
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PLIST_SRC="$REPO_DIR/launchd/com.nanoclaw.daemon.plist"
PLIST_DST="$HOME/Library/LaunchAgents/com.nanoclaw.daemon.plist"
LOG_DIR="$HOME/.nanoclaw/logs"
UID_NUM=$(id -u)

echo "=== nanoclaw install ==="

# 1. Create dirs
mkdir -p "$LOG_DIR"
echo "✓ Log directory: $LOG_DIR"

# 2. Dry-run test first
echo "Running dry-run test..."
cd "$SCRIPT_DIR"
/usr/bin/python3 nanoclaw.py --once --dry-run
echo "✓ Dry-run passed"

# 3. Install plist (template HOME path for portability)
sed "s|$HOME_DIR|$HOME|g" "$PLIST_SRC" > "$PLIST_DST"
echo "✓ Plist installed: $PLIST_DST (HOME=$HOME)"

# 4. Retire replaced agents (unload but don't delete — keep for rollback)
RETIRE_AGENTS=(
    "com.ai.cliproxyapi.monitor"
    "com.cliproxyapi.healthcheck"
    "com.cliproxyapi.tokenrefresh"
    "com.ai.sync-health"
    "com.ai.service-preflight-watch"
    "com.ai.service-preflight-watch-local"
    "com.cliproxyapi.usage-monitor"
)

echo ""
echo "Retiring replaced agents:"
for agent in "${RETIRE_AGENTS[@]}"; do
    plist="$HOME/Library/LaunchAgents/${agent}.plist"
    if [ -f "$plist" ]; then
        if launchctl list "$agent" &>/dev/null; then
            launchctl bootout "gui/$UID_NUM/$agent" 2>/dev/null || true
            echo "  ✓ Unloaded: $agent"
        else
            echo "  - Already unloaded: $agent"
        fi
        # Rename to .disabled for rollback
        mv "$plist" "${plist}.disabled" 2>/dev/null || true
    else
        echo "  - Not found: $agent"
    fi
done

# 5. Remove replaced cron entries
echo ""
echo "Cleaning cron entries..."
# Remove token_refresh.py cron (nanoclaw handles it now)
# Remove log rotation cron (nanoclaw handles it now)
crontab -l 2>/dev/null | grep -v "token_refresh.py" | grep -v "cli-proxy-api/logs.*-delete" | crontab - 2>/dev/null || true
echo "✓ Removed replaced cron entries"

# 6. Load nanoclaw
echo ""
echo "Loading nanoclaw daemon..."
launchctl bootout "gui/$UID_NUM/com.nanoclaw.daemon" 2>/dev/null || true
sleep 1
launchctl bootstrap "gui/$UID_NUM" "$PLIST_DST"
echo "✓ nanoclaw loaded"

# 7. Verify
sleep 3
if launchctl list "com.nanoclaw.daemon" &>/dev/null; then
    PID=$(launchctl list "com.nanoclaw.daemon" 2>/dev/null | awk '{print $1}')
    echo ""
    echo "=== nanoclaw running (PID: $PID) ==="
    echo "State: $HOME/.nanoclaw/state.json"
    echo "Logs:  $LOG_DIR/nanoclaw.log"
    echo "Status: python3 $SCRIPT_DIR/nanoclaw.py --status"
else
    echo ""
    echo "ERROR: nanoclaw failed to start"
    echo "Check: $LOG_DIR/stderr.log"
    exit 1
fi
