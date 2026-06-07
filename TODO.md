# agent-bus — backlog

POC is working (serve/install/send/poll/peers/teams, wizard, version). Below is the
QoL + hardening backlog, roughly priority-ordered. Nothing here blocks current use.

## Prompting / wizard QoL
- [ ] Wizard: optional `--card` / "describe this agent" step (capability card at setup).
- [ ] Wizard: final confirm screen ("write .mcp.json + CLAUDE.md + register? y/N") before applying.
- [ ] Team Select already fuzzy-filters (inquire) — document it; consider showing agent counts inline.
- [ ] `--yes` / non-interactive guard so CI never blocks on a prompt.

## New commands
- [ ] `whoami` — print this shell's resolved team/alias + db path + config source.
- [ ] `doctor` — checks: bus.db reachable, binary on PATH, .mcp.json present + points here,
      .claude/settings.local.json has enabledMcpjsonServers, stale-last_seen warnings.
- [ ] `roster` — alias for `peers --team <mine>` (friendlier name).
- [ ] `unregister` / `prune` — drop a peer; prune peers with last_seen older than N.

## Bugs / warts
- [ ] Team/global broadcast (`to:"team:X"` / `"all"` / `"*"`) is delivered back to the SENDER
      on poll (recipient_alias='*' matches the sender too). Harmless self-echo, but noisy —
      exclude `sender_team/sender_alias` from a broadcast recipient's poll. (found 2026-06-07)
- [ ] `version` shows `-dirty` from build.rs `git status` — fine, but ensure release installs
      are built from a clean tree (task install after commit).

## Wake / delivery
- [ ] Ship an `fswatch ~/.agent-bus/inbox/<team>/<alias>.flag` wrapper for Codex/Copilot
      (they have no Monitor). Document the poll-at-turn fallback.
- [ ] Message TTL / retention: prune delivered messages older than N days to keep bus.db small.

## A2A north side (deferred — see SPEC.md §6)
- [ ] Speak A2A on the wire (tasks + Agent Cards) when a non-CLI / cross-machine / cross-vendor
      agent needs to join. Adopt the contract; defer auth/signed-cards/SSE until needed.

## Cutover (parallel-trial -> agent-bus primary)
Owner decision, applied to ALL THREE sessions at once. Preconditions:
- [ ] all three (sync/classic/client) restarted + actively polling (fresh `last_seen`)
- [ ] a real client -> classic -> client round-trip succeeds over the bus (same task_id)
- [ ] only then: decommission the `.md` auto-monitoring in each repo's CLAUDE.md together
      (classic already removed its .md monitor early — re-add until cutover).
