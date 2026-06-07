# agent-bus

A minimal, A2A-shaped message bus for coordinating multiple CLI agent sessions
(Claude Code, Codex, GitHub Copilot, ...). One self-contained Rust binary, one
shared SQLite file. No daemon, no port: every session runs `agent-bus serve` as
an MCP server over **stdio**, and they all share `~/.agent-bus/bus.db` (WAL mode).

## Build
```bash
cargo build --release
# binary: target/release/agent-bus
```

## Commands
```
agent-bus serve                 MCP stdio server (point a CLI's MCP config at this)
agent-bus install [...]         interactive installer (writes MCP config + CLAUDE.md bootstrap)
agent-bus send --to X --body Y [--type task] [--state s] [--task-id id] [--as alias]
agent-bus poll [--as alias]
agent-bus peers
agent-bus register [--card ..] [--as alias]
```

Identity is `AGENT_BUS_ALIAS` (server) or `--as` (CLI): `sync`, `classic`, `client`, ...

## Why this shape
- **South side = MCP** — the one protocol every CLI agent already speaks as a client.
- **A2A-shaped data model** — messages carry a `task_id` + lifecycle `state`
  (submitted → working → completed/failed), so the wire can grow into the real
  [A2A](https://a2a-protocol.org) standard later without a schema rewrite (see `SPEC.md`).
- **Doorbell wake** — on every send the broker touches `~/.agent-bus/inbox/<recipient>.flag`;
  an external watcher (Claude `Monitor`, `fswatch`) reacts to wake an idle session.
  Each CLI needs its own small wake-shim — that cost is protocol-independent.

## Install (the easy way)
Run the installer once per session/tool — it writes the config and, for Claude,
appends a self-bootstrap block to that repo's `CLAUDE.md`:
```bash
# Claude Code repo:
target/release/agent-bus install --tool claude --alias classic --repo ~/Projects/personal/astrub-classic
# Codex (writes ~/.codex/config.toml):
target/release/agent-bus install --tool codex --alias codexer
# Copilot (prints the snippet to add):
target/release/agent-bus install --tool copilot --alias copilot
```
Run with no flags for interactive prompts. After installing, **restart that session**
and approve the `agent-bus` MCP prompt.

### What the config looks like (Claude `.mcp.json`)
```json
{
  "mcpServers": {
    "agent-bus": {
      "command": "~/.cargo/bin/agent-bus",
      "args": ["serve"],
      "env": { "AGENT_BUS_ALIAS": "classic" }
    }
  }
}
```

## Tools (MCP)
| Tool | Purpose |
|------|---------|
| `register(card?)` | register/refresh THIS agent + optional Agent Card |
| `send(to, body, type?, state?, task_id?)` | send to an alias (or `to:"all"`); `type:"task"` for work requests |
| `poll()` | fetch new messages addressed to me + broadcasts; advance cursor |
| `peers()` | list known agents + last-seen + Agent Card |

## Wake (doorbell) — Claude Code
The installer's CLAUDE.md block arms this automatically; manually it's:
```
Monitor(persistent:true, timeout_ms:3600000, command: |
  f=~/.agent-bus/inbox/sync.flag; last=""; while true; do
    if [ -f $f ]; then m=$(stat -f %m $f 2>/dev/null);
      if [ "$m" != "$last" ]; then echo "BUS: new mail for sync — call agent-bus poll()"; last="$m"; fi; fi;
    sleep 2; done)
```
Codex/Copilot: no background watcher — call `poll()` at the start of each turn, or run
`fswatch ~/.agent-bus/inbox/<alias>.flag` in a terminal.

## Config / state
- `AGENT_BUS_ALIAS` — this session's identity (default `unknown`).
- `AGENT_BUS_HOME` — state dir (default `~/.agent-bus`).
- `AGENT_BUS_DB` — SQLite path (default `$AGENT_BUS_HOME/bus.db`).
- State (`bus.db`, `inbox/*.flag`) lives under `~/.agent-bus/` — never in a project repo.

## Test
```bash
cargo test
```
Covers the A2A task round-trip + lifecycle, late-registration delivery, cursor drain,
peers/broadcast, and the MCP initialize/tools-list handshake.

## Status
POC, verified end-to-end (cargo test + live MCP stdio + CLI). Deferred (see `SPEC.md` §6):
A2A north side, cross-machine, auth, signed Agent Cards, SSE streaming.
