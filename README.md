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
agent-bus install [...]         interactive installer (writes MCP config + CLAUDE.local.md bootstrap)
agent-bus send --to X --body Y [--type task] [--state s] [--task-id id] [--as alias]
agent-bus poll [--as alias]
agent-bus peers
agent-bus register [--card ..] [--as alias]
```

Identity is `team/alias` — `AGENT_BUS_TEAM`+`AGENT_BUS_ALIAS` (server) or `--team`/`--as` (CLI):
`astrub/sync`, `astrub/classic`, `astrub/client`, `webapp/api`, ...

## Why this shape
- **South side = MCP** — the one protocol every CLI agent already speaks as a client.
- **A2A-shaped data model** — messages carry a `task_id` + lifecycle `state`
  (submitted → working → completed/failed), so the wire can grow into the real
  [A2A](https://a2a-protocol.org) standard later without a schema rewrite (see `SPEC.md`).
- **Doorbell wake** — on every send the broker touches `~/.agent-bus/inbox/<recipient>.flag`;
  an external watcher (Claude `Monitor`, `fswatch`) reacts to wake an idle session.
  Each CLI needs its own small wake-shim — that cost is protocol-independent.

## Install (the easy way)
Just run the binary — bare `agent-bus` (or `agent-bus setup`) launches an interactive
wizard that asks tool / team / alias / repo and pre-guesses `team/alias` from the repo
name (`astrub-classic` → team `astrub`, alias `classic`). It writes the config and, for
Claude, upserts a self-bootstrap block into that repo's `CLAUDE.local.md`.

```bash
agent-bus                 # interactive first-time setup
```

Non-interactive (flags skip the prompts):
```bash
agent-bus install --tool claude --team astrub --alias classic --repo ~/Projects/personal/astrub-classic
agent-bus install --tool codex   --team astrub --alias codexer     # writes ~/.codex/config.toml
agent-bus install --tool copilot --team astrub --alias copilot      # prints the snippet
```
After installing, just **restart that session** — no approval prompt. The installer
auto-enables the server by adding `"enabledMcpjsonServers": ["agent-bus"]` to the repo's
`.claude/settings.local.json` (targeted + personal/gitignored). To approve broadly instead,
set `"enableAllProjectMcpServers": true` in `~/.claude/settings.json`.

### What the config looks like (Claude `.mcp.json`)
```json
{
  "mcpServers": {
    "agent-bus": {
      "command": "~/.cargo/bin/agent-bus",
      "args": ["serve"],
      "env": { "AGENT_BUS_TEAM": "astrub", "AGENT_BUS_ALIAS": "classic" }
    }
  }
}
```

## Tools (MCP)
| Tool | Purpose |
|------|---------|
| `register(card?)` | register/refresh THIS agent (team+alias) + optional Agent Card |
| `send(to, body, type?, state?, task_id?)` | see addressing below; `type:"task"` for work requests |
| `poll()` | fetch new messages for me / my team / global since last poll; advance cursor |
| `peers(team?)` | the roster: my team (default), a named team, or everyone (`team:"*"`) |

### Addressing (`send` `to`)
| `to` | meaning |
|------|---------|
| `"classic"` | same-team direct → `<myteam>/classic` |
| `"webapp/api"` | cross-team direct |
| `"team:astrub"` | broadcast to everyone in team `astrub` |
| `"all"` | broadcast to my team |
| `"*"` | global broadcast (every team) |

## Teams / rosters
A **team** is a logical namespace over one shared bus — same-team is the default scope,
cross-team is addressable explicitly, and `peers()` is the team roster (queried live, never
cached). A team may exist with no agents. One physical `bus.db`, many logical teams. So astrub's many repos group as team
`astrub` (`astrub/sync`, `astrub/classic`, `astrub/client`); a different project is its
own team on the same bus with no cross-chatter unless explicitly addressed. Mirrors the
astrub cockpit-group / party philosophy, one layer up (agents instead of game accounts).

## Wake (doorbell) — Claude Code
The installer's CLAUDE.local.md block arms this automatically; manually it's:
```
Monitor(persistent:true, timeout_ms:3600000, command: |
  f=~/.agent-bus/inbox/astrub/sync.flag; last=""; while true; do
    if [ -f "$f" ]; then m=$(stat -f %m "$f" 2>/dev/null);
      if [ "$m" != "$last" ]; then echo "BUS: new mail for astrub/sync — call agent-bus poll()"; last="$m"; fi; fi;
    sleep 2; done)
```
Codex/Copilot: no background watcher — call `poll()` at the start of each turn, or run
`fswatch ~/.agent-bus/inbox/<team>/<alias>.flag` in a terminal.

## Config / state
- `AGENT_BUS_TEAM` — logical group (default `default`).
- `AGENT_BUS_ALIAS` — this agent's name within the team (default `unknown`).
- `AGENT_BUS_HOME` — state dir (default `~/.agent-bus`).
- `AGENT_BUS_DB` — SQLite path (default `$AGENT_BUS_HOME/bus.db`).
- State (`bus.db`, `inbox/<team>/<alias>.flag`) lives under `~/.agent-bus/` — never in a repo.

## Test
```bash
cargo test
```
Covers the A2A task round-trip + lifecycle, late-registration delivery, cursor drain,
peers/broadcast, and the MCP initialize/tools-list handshake.

## Status
POC, verified end-to-end (cargo test + live MCP stdio + CLI). Deferred (see `SPEC.md` §6):
A2A north side, cross-machine, auth, signed Agent Cards, SSE streaming.
