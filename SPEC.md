# Agent-to-Agent Comms — Design Spec

**Status:** draft / decision needed
**Author:** astrub-sync (planning) session
**Date:** 2026-06-07
**Scope:** how the 3 Claude Code sessions (astrub-sync, astrub-classic, astrub-client) — separate terminals, separate repos, one Mac — exchange messages.

---

## 1. Problem

We run three independent Claude Code sessions that must coordinate (execution requests, protocol ASKs, ACKs, design handoffs). Claude Code sessions are **fully isolated** — there is no native peer channel between two top-level sessions. We bootstrapped a **file-based message bus** (markdown outboxes + `.watermark` files + `Monitor` poll-loops). It works, but it is fragile.

### Current bus (what we have today)

5 markdown files, each an append-only "outbox", newest-on-top, watched by `Monitor` sleep-loops that compare `wc -l` against a per-reader `.watermark`:

| File | Writer | Readers |
|------|--------|---------|
| `client.md` | astrub-client | astrub-classic |
| `server.md` | astrub-classic | astrub-client, astrub-sync |
| `sync.md` | astrub-sync | astrub-classic |
| `sync-client.md` | astrub-sync | astrub-client |
| `client-sync.md` | astrub-client | astrub-sync |

### Weaknesses (why this needs replacing)

1. **Polling, not events.** `Monitor` runs `while true; sleep 1` per channel. Laggy, wasteful, and every session needs N monitors (we already run 2).
2. **Line-count watermarks are brittle.** "Newest-on-top" means a line-count delta does NOT identify the new content (it shifted down). Any edit/rewrite/squash desyncs the cursor. We have already been bitten ("handle lines wm+1..cur" pointed at stale content).
3. **No addressing or delivery guarantee.** Routing is a `### ASK -> sync:` text convention. Nothing enforces it; a misfiled or untagged message is silently missed. No ack/read receipts.
4. **Unstructured payloads.** Freeform markdown — no schema, no message id, no reply-to, no type. Can't be validated or queried.
5. **Manual bootstrap + file explosion.** Each new session needs a hand-pasted prompt to arm monitors, and the pairwise model trends toward N×(N−1) files.

---

## 2. What's actually available (researched 2026-06-07)

### A. MCP "channels" — official server→session push (research preview)

Claude Code v2.1.80+ supports **channels**: an MCP server that **pushes events into a running session** (two-way — Claude can reply through the same channel). This is the missing primitive — real push, no polling.

- Opt in per session: `claude --channels plugin:<name>@<marketplace>`.
- Events arrive as `<channel source="...">` blocks while the session is open.
- **Constraints:**
  - Requires **Anthropic auth via claude.ai or Console API key**. NOT available on Bedrock / Vertex / Foundry.
  - **Research preview** — flag syntax + protocol may change; there are open bugs about notifications not always surfacing (CC issues #36665, #3174, #41733).
  - During preview, `--channels` only accepts plugins on an Anthropic allowlist; a **custom** channel you build needs `--dangerously-load-development-channels`.
  - Plugins are **Bun** scripts.
  - Events only arrive while the session is open → for "always-on" you keep a persistent terminal (we already do).

### B. `claude-peers-mcp` — community broker, our exact use case

An MCP server purpose-built for **multiple Claude Code instances on the same machine**:

- Local broker daemon: **SQLite + HTTP on `localhost:7899`**, auto-starts on first use.
- Per-session stdio MCP process registers with the broker; scope-filtered discovery (machine / directory / repo).
- Tools: `list_peers`, `send_message`, `set_summary`, `check_messages`.
- **Delivery:** push via the channels protocol when launched with dev-channels enabled; **poll fallback** via `check_messages` when push is unavailable.
- Requires: claude.ai login (API-key auth can't do channels), localhost-only, Bun, Claude Code v2.1.80+.

### C. Other prior art (for reference)

- **session-bridge** plugin — filesystem-based P2P between sessions (same idea as our bus, more structured).
- **claude-code-chat** — WebSocket broker, cross-machine.
- **AgentDM** — hosted cross-model agent DMs over MCP (external service — fails our "no external services" lean).

### D. Non-starters we ruled out

- `PushNotification` — desktop/phone alert to the human, not an inter-agent transport.
- `RemoteTrigger` / scheduled routines — spawn fresh cloud sessions on a cron, not live peer messaging.
- Agent-team `SendMessage` / subagents — only within ONE session's spawned-agent tree, not across independent terminals. Would require collapsing to one orchestrator session that backgrounds the others — a different working model than 3 interactive terminals.

---

## 3. Options

| Option | Transport | Push? | Structured | Effort | New deps / constraints |
|--------|-----------|-------|------------|--------|------------------------|
| **A. Harden the file-bus** | markdown + fswatch | event (fswatch) | no | low | `fswatch`; still convention-addressed |
| **B. Adopt `claude-peers-mcp`** | SQLite broker @ :7899 | yes (preview) + poll | yes (msg objects) | low–med | Bun, claude.ai auth, v2.1.80+, research-preview bugs |
| **C. Build a custom broker + channel** | our SQLite broker + custom channel plugin | yes (preview) + poll | yes (typed) | high | Bun, claude.ai auth, maintain it ourselves |
| **D. Status quo** | markdown + Monitor poll | no | no | none | the weaknesses in §1 |

### Notes per option

- **A** is the cheapest real win: swap `while true; sleep 1` for `fswatch -0 <file>` (event-driven, no CPU spin) and replace the line-count watermark with a **monotonic message-id cursor** (newest-on-BOTTOM append with `id:` front-matter per entry, cursor = last-id-seen). Removes weaknesses #1 and #2 entirely; #3/#4 remain.
- **B** removes #1–#5 in one move and is **already written and battle-tested for our exact topology**. The markdown files survive only as an optional human-readable audit log. Risk = research-preview instability; mitigated because the broker degrades to **poll mode** (`check_messages` fired by a single `Monitor`/`fswatch`), which is still strictly better than line-count watermarks (messages have ids + structured bodies).
- **C** only justified if `claude-peers-mcp` proves unfit after evaluation (e.g. we need the `-> recipient` CONTRACT semantics baked in, or richer typed payloads). Don't build until B is disproven.

---

## 4. Recommendation

**Evaluate `claude-peers-mcp` (Option B); adopt if it fits. Keep the file-bus as a fallback + audit log during a trial period.**

Rationale: it's the with-the-grain answer (MCP is Claude's native extension surface), it already targets "N Claude Code sessions on one machine", it gives structured + addressed messages with real push, and it fails safe to poll mode. Building our own (C) duplicates it; the file-bus (A/D) can't give delivery guarantees or structured payloads no matter how much we polish it.

**Fallback ladder if B is blocked** (e.g. auth is Bedrock/Vertex, or preview bugs make push unreliable):
1. Run `claude-peers-mcp` in **poll mode** (no channels) — still structured/addressed.
2. If MCP itself is off the table → **Option A** (fswatch + id-cursor file-bus).

> **This recommendation assumes an all-Claude fleet.** If Codex or Copilot must participate,
> jump to §6 — `claude-peers-mcp` and `channels` are Claude-only and the answer changes.

---

## 5. Migration plan (if B is chosen)

1. **Preconditions check** (all 3 sessions): `claude --version` ≥ 2.1.80; `bun --version` present; auth is claude.ai or Console API key (NOT Bedrock/Vertex); for Team/Enterprise, `channelsEnabled` set by admin.
2. **Install** `claude-peers-mcp` in each session's MCP config (`.mcp.json` or user scope). One broker auto-starts on :7899.
3. **Relaunch ritual** updates: each session starts with the channels flag (or dev-channels for custom). Document the new launch command in each repo's CLAUDE.md, replacing the "arm Monitor" instruction.
4. **Addressing convention:** map our roles to peer aliases (`sync`, `classic`, `client`); keep the `topic`/`type` field for ASK / ACK / EXEC-REQUEST / DESIGN-HANDOFF.
5. **Trial period:** run BOTH buses in parallel for one phase (Phase 8 handoff is a good test — it exercises sync→client). Mirror each broker message into the markdown file as an audit log.
6. **Cutover or rollback:** if the broker is reliable across the trial, demote the markdown bus to audit-only (or retire it). If not, fall back per §4.
7. **Update** CONTRACT.md / CLAUDE.md in all three repos with the final protocol; delete the watermark + Monitor instructions.

---

## 6. Multi-tool fleet (Codex + Copilot + Claude in the mix)

If the fleet is not all-Claude, the Claude-specific transports are out:

- **`channels` push** requires claude.ai/Console auth and Claude Code — Codex and Copilot cannot receive it.
- **`claude-peers-mcp`** uses the channel protocol + claude.ai login — Claude-only.

### What all three DO share: MCP client support

| Tool | MCP client | Transports | Config | Push (channels-style)? |
|------|-----------|------------|--------|------------------------|
| Claude Code | yes | stdio, HTTP | `.mcp.json` | **yes** (channels, preview, claude.ai auth) |
| Codex CLI | yes | stdio, streamable HTTP | `~/.codex/config.toml` or `.codex/config.toml` | no — **poll only** |
| Copilot (agent mode / CLI) | yes | stdio, remote | JSON (repo/VS Code) | no — **poll only** |

So the only cross-tool substrate is **a shared MCP broker that every tool registers as a client**. Push is a Claude-only optimization layered on top; **poll is the universal floor.**

### Design — tool-agnostic broker (this is Option C, made concrete)

- **One broker process** exposing MCP over **streamable HTTP** at `localhost:<port>` (HTTP, not per-client stdio, so a single daemon serves all tools). SQLite backing store.
- **Schema:** `messages(id, from, to, topic, type, body, created_at)`, `cursors(agent, last_id)`, `peers(alias, scope, summary, last_seen)`.
- **MCP tools (identical surface for all three tools):** `register(alias, scope)`, `send(to, topic, type, body)`, `poll(since?)` → returns messages for me, advances my cursor, `peers()`, `set_summary(text)`.
- **Optionally also implement Claude's `channels` protocol** in the same broker → Claude sessions get push for free; Codex/Copilot ignore it and poll. One broker, best delivery each tool supports.
- **Addressing:** alias-based (`sync` / `classic` / `client`) + broadcast — enforced by the broker, not by text convention. Messages carry ids + `reply_to`, so nothing is silently missed.

### The real wart: how does a poll-only tool know WHEN to poll?

Claude has `Monitor` (a background process that re-invokes the model on an event). Codex and Copilot have **no built-in background watcher**, so they can't be woken mid-idle. Practical options, best to worst:

1. **Universal doorbell file.** The broker `touch`es `.inbox/<alias>.flag` (with a count) whenever mail lands for an alias. Any tool that can run a file watcher in its harness reacts to it; the structured content still comes from the MCP `poll` tool. Claude's `Monitor` consumes this uniformly; a thin `fswatch` wrapper can do the same for a Codex/Copilot terminal.
2. **Poll-at-turn-start convention.** Codex/Copilot call `poll()` at the start of every turn. Cadence = human pace (you're driving those turns anyway). Simple, no background machinery, but no autonomous wake between turns.
3. **Codex `notify` hook** (outbound only) — can fire a program on Codex events; useful to signal OTHERS that Codex did something, not to wake Codex itself.

Honest limitation: **truly autonomous, between-turns wake is a Claude-only capability today** (via `Monitor` + optionally `channels`). For Codex/Copilot, inbound coordination is turn-paced or doorbell-file-paced, not instant-while-idle.

### Recommendation for a mixed fleet

Build/adopt the **tool-agnostic HTTP MCP broker (Option C)** with the optional Claude-channels layer. Aliases per role. Poll floor for everyone; push for Claude; doorbell file as the universal wake bridge. Keep the markdown bus as the human audit log. (Hosted alternative: **AgentDM** is explicitly cross-model agent DMs over MCP — but it's an external service, which conflicts with the project's local-only lean. Use only if a local broker proves insufficient.)

### Where A2A (Agent2Agent) fits — and where it doesn't

A2A is THE standard for agent↔agent (MCP is agent↔tool). As of 2026 it's **v1.2, Linux-Foundation governed, 150+ orgs**, native in agent FRAMEWORKS (Google ADK, LangGraph, CrewAI, LlamaIndex, Semantic Kernel, AutoGen) and **Copilot Studio (GA)** / **Google Antigravity**. Semantics: task lifecycle (submitted→working→completed/failed), **Agent Cards** for capability discovery, SSE streaming, webhook push.

**It does NOT by itself solve our setup, because:**

- Our three tools — **Claude Code, Codex CLI, Copilot coding agent** — are MCP clients, **not A2A servers**. None exposes an Agent Card / A2A endpoint. A2A's "remote agent" is an always-on HTTP service; an interactive human-driven REPL is not.
- To make a session A2A-reachable you wrap it in an **A2A server shim**, which hits the **same wake problem** (§6) — injecting an inbound task into an idle interactive session. A2A standardizes the envelope + task states, **not** the injection/wake. That wart is transport-agnostic.

**Where it genuinely helps — upgrade the broker's wire, don't replace the plumbing:**

```
Claude Code ─┐
Codex CLI    ├─ MCP (native client) ─► BROKER ─ A2A v1.2 ─► other A2A agents
Copilot CLI ─┘                          │                    (Copilot Studio, ADK/
                                        └─ SQLite + doorbell    LangGraph, cross-machine)
                                           (wake bridge)
```

- **South side = MCP** (what the CLIs natively speak → they reach the broker as clients).
- **North side = A2A** (broker speaks A2A → cross-vendor interop, real task lifecycle that maps perfectly onto "sync asks classic to *execute Phase 7*", Agent-Card discovery instead of a hand-rolled `peers()`). There is an off-the-shelf **`a2a-mcp` bridge** (MCP server that is an A2A client) for exactly this.
- The **wake bridge stays** — A2A's SSE/webhook push needs a listening endpoint the REPLs don't have.

**Net:** A2A swaps the bespoke message schema for a standard and future-proofs cross-vendor/cross-machine, but the broker + wake-bridge still have to exist.

### Verdict depends on the design goal — small static fleet vs open-ended growing fleet

- **Small static fleet (3 CLIs, one Mac, not growing):** building the A2A north side now is over-engineering. A plain MCP broker (south side only) is enough.
- **Open-ended fleet (any/all CLIs, growing, eventually cross-machine — the owner's stated goal 2026-06-07):** targeting A2A is **correct, not over-engineering** — but only if you separate two things:
  - **Adopt the A2A *contract* now (cheap):** model messages as A2A tasks + Agent Cards even while the implementation is a localhost SQLite hub with no auth. This is just picking interfaces you won't have to rip out.
  - **Build the full A2A *stack* later (premature now):** cross-machine transport, signed Agent Cards, auth federation, SSE streaming — add each when a real remote/non-CLI agent needs it.

**Why the calculus flips at scale:** a bespoke schema costs custom glue *per tool* (combinatorial N×M — fine at 3, a compounding tax at 10). A2A makes a new tool join by publishing an Agent Card = ~0 new broker code. The market is converging on this exact standard to kill that combinatorial cost, so at scale A2A is *less* total engineering.

**What A2A still won't do at any scale:** inject an inbound message into an idle interactive CLI and wake it. A2A standardizes the envelope, not the injection. So A2A collapses message-format glue (N×M → ~0) but you still write **one wake-shim per CLI** (linear, unavoidable, protocol-independent). (If vendors ship "CLI-as-A2A-server" mode, the central broker could even become optional — agents discover via Agent Cards directly — but the wake-shims remain.)

### Recommended build for the open-ended goal (two layers, both cheap this week)

1. **Thin local broker** — SQLite + `send`/`poll`/`peers` + doorbell file; serves the current CLIs over MCP (south side).
2. **Behind an A2A-shaped contract** — tasks + Agent Cards as the data model, so the A2A north side and any future tool slot in without a rewrite.

Defer cross-machine / signed cards / auth / SSE until a non-CLI or remote agent actually shows up.

---

## 7. Open decisions (for the owner)

1. **Fleet composition?** All-Claude → Option B (`claude-peers-mcp`, push works). Mixed with Codex/Copilot → Option C (tool-agnostic HTTP broker, §6). This is the first fork — it decides everything below.
2. **Auth mode?** Are the Claude sessions on claude.ai / Console API key? (If any Claude session is on Bedrock/Vertex, channels-push is out for it → poll mode.)
3. **Adopt vs build?** All-Claude: trial `claude-peers-mcp` first. Mixed: build the project-owned broker (no off-the-shelf cross-tool local broker that also speaks Claude channels — AgentDM is the only turnkey option and it's hosted).
4. **Keep markdown as audit log** after cutover, or retire entirely?
5. **Trust posture:** broker is localhost-only; acceptable, or namespaced/scoped per-repo?

---

## 8. Sources

- Claude Code — Channels (push events into a session): https://code.claude.com/docs/en/channels
- Channels reference (build your own): https://code.claude.com/docs/en/channels-reference
- Claude Code — MCP: https://code.claude.com/docs/en/mcp
- `claude-peers-mcp`: https://explainx.ai/skills/aradotso/trending-skills/claude-peers-mcp
- CC issue #28300 — Multi-agent A2A across machines: https://github.com/anthropics/claude-code/issues/28300
- CC issue #37213 — Inter-session communication between instances: https://github.com/anthropics/claude-code/issues/37213
- CC issue #36665 — MCP server push notifications (unsolicited): https://github.com/anthropics/claude-code/issues/36665
- session-bridge plugin: https://agent-wars.com/news/2026-03-15-session-bridge-claude-code-plugin
- claude-code-chat / distributed agents: https://vikrantjain.hashnode.dev/distributed-claude-code-agents-across-machines
- Codex — MCP: https://developers.openai.com/codex/mcp
- Codex — config reference (config.toml, mcp_servers): https://developers.openai.com/codex/config-reference
- GitHub Copilot agent mode + MCP: https://docs.github.com/en/copilot/tutorials/enhance-agent-mode-with-mcp
- AgentDM (cross-model agent DMs over MCP): https://mcpservers.org/servers/agentdm-ai
- A2A protocol — spec: https://a2a-protocol.org/latest/specification/
- A2A and MCP (how they relate): https://a2a-protocol.org/latest/topics/a2a-and-mcp/
- a2a-mcp bridge (MCP server that is an A2A client): https://github.com/a2anet/a2a-mcp
- Copilot Studio — connect over A2A: https://learn.microsoft.com/en-us/microsoft-copilot-studio/add-agent-agent-to-agent
