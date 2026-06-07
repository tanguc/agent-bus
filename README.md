# agent-bus

A minimal, A2A-shaped message bus for coordinating multiple CLI agent sessions
(Claude Code, Codex, GitHub Copilot, ...). Zero dependencies — Python stdlib only.

No daemon, no port: every session runs its own copy of `agent_bus.py` as an MCP
server over **stdio**, and they all share ONE SQLite file (`bus.db`, WAL mode) on
disk. Identity is the `AGENT_BUS_ALIAS` env var (`sync`, `classic`, `client`, ...).

## Why this shape
- **South side = MCP** — the one protocol every CLI agent already speaks as a client.
- **A2A-shaped data model** — messages carry a `task_id` + lifecycle `state`
  (submitted → working → completed/failed), so the wire can grow into the real
  [A2A](https://a2a-protocol.org) standard later without a schema rewrite. See `SPEC.md`.
- **Doorbell wake** — on every send the broker touches `inbox/<recipient>.flag`; an
  external watcher (Claude `Monitor`, `fswatch`) reacts to wake an idle session.
  Each CLI needs its own small wake-shim — that cost is protocol-independent.

## Tools
| Tool | Purpose |
|------|---------|
| `register(card?)` | register/refresh THIS agent (alias from `AGENT_BUS_ALIAS`) + optional Agent Card |
| `send(to, body, type?, state?, task_id?)` | send to an alias (or `to:"all"`); `type:"task"` for work requests |
| `poll()` | fetch new messages addressed to me + broadcasts; advances my cursor |
| `peers()` | list known agents + last-seen + Agent Card |

## Install per tool

### Claude Code
Add to the project's `.mcp.json` (one block per repo, unique alias, same script path):
```json
{
  "mcpServers": {
    "agent-bus": {
      "command": "python3",
      "args": ["~/agent-bus/agent_bus.py"],
      "env": { "AGENT_BUS_ALIAS": "sync" }
    }
  }
}
```
Restart the session (or `/mcp` reconnect) to load it.

### Codex CLI
`~/.codex/config.toml` (or project `.codex/config.toml`):
```toml
[mcp_servers.agent-bus]
command = "python3"
args = ["~/agent-bus/agent_bus.py"]
env = { AGENT_BUS_ALIAS = "codex" }
```

### GitHub Copilot (agent mode / CLI)
Add an MCP server entry pointing at the same script with `AGENT_BUS_ALIAS=copilot`.

## Wake (doorbell) — Claude Code example
```
Monitor(description:"agent-bus inbox", persistent:true, timeout_ms:3600000, command: |
  cd ~/Projects/personal/agent-bus && while true; do
    if [ -f inbox/sync.flag ]; then echo "BUS: new mail for sync — call poll()"; fi
    sleep 2;
  done)
```
On `BUS:` → call the `poll` tool. (Codex/Copilot: call `poll()` at turn start, or run
`fswatch inbox/<alias>.flag`.)

## Config
- `AGENT_BUS_ALIAS` — this session's identity (default `unknown`).
- `AGENT_BUS_DB` — override the SQLite path (default `<repo>/bus.db`).

## Storage (gitignored)
- `bus.db` — SQLite: messages, cursors, peers.
- `inbox/*.flag` — per-recipient doorbells.

## Status
POC, verified end-to-end over the real MCP stdio protocol: cross-session send/poll,
task-id reply lifecycle, peer discovery, doorbell. Deferred (see `SPEC.md` §6): A2A
north side, cross-machine, auth, signed Agent Cards, SSE streaming.

## Test
```bash
python3 selftest.py
```
