#!/usr/bin/env bash
# bus-watch.sh — file-watcher doorbell for Codex/Copilot (no persistent Monitor)
#
# Usage:
#   bus-watch.sh <team> <alias>
#
# Watches the flag file and runs 'agent-bus poll' when it changes.
# Intended for agents that don't have Claude Code's Monitor tool:
#   - Run this in a separate terminal alongside your Codex/Copilot session
#   - It will print a visible notice when new mail arrives
#
# Requirements (install one):
#   macOS: brew install fswatch
#   Linux: apt install inotify-tools

set -euo pipefail

TEAM="${1:?usage: bus-watch.sh <team> <alias>}"
ALIAS="${2:?usage: bus-watch.sh <team> <alias>}"
BUS_HOME="${AGENT_BUS_HOME:-$HOME/.agent-bus}"
FLAG="$BUS_HOME/inbox/$TEAM/$ALIAS.flag"

echo "[bus-watch] identity: $TEAM/$ALIAS"
echo "[bus-watch] flag:     $FLAG"

# ensure flag dir exists so watchers don't error before first send
mkdir -p "$(dirname "$FLAG")"
touch "$FLAG"

poll() {
    echo ""
    echo "[bus-watch] BUS: new mail for $TEAM/$ALIAS — polling"
    agent-bus poll --team "$TEAM" --as "$ALIAS"
}

if command -v fswatch >/dev/null 2>&1; then
    echo "[bus-watch] using fswatch (macOS)"
    fswatch -o "$FLAG" | while read -r; do poll; done
elif command -v inotifywait >/dev/null 2>&1; then
    echo "[bus-watch] using inotifywait (Linux)"
    while inotifywait -q -e close_write "$FLAG" 2>/dev/null; do poll; done
else
    echo "[bus-watch] no watcher found — falling back to 5s poll loop"
    echo "[bus-watch] install: brew install fswatch (macOS) | apt install inotify-tools (Linux)"
    while true; do
        if [ -f "$FLAG" ]; then
            new_mtime=$( (stat -f %m "$FLAG" || stat -c %Y "$FLAG") 2>/dev/null )
            if [ "${new_mtime:-}" != "${last_mtime:-}" ]; then
                last_mtime="$new_mtime"
                poll
            fi
        fi
        sleep 5
    done
fi
