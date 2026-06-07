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
- [x] all three (sync/classic/client) restarted + actively polling (fresh `last_seen`)
      — classic + client confirmed live + polling; sync active via CLI (2026-06-07)
- [x] a real client -> classic -> client round-trip succeeds over the bus (same task_id)
      — LANDED 2026-06-07: client tasks 7ba47430 + b0cbe5c0 -> classic working/completed
        on same task_ids -> client confirmed, referenced fix commits 42201e7, 06a31a2.
        Bus-only, no .md. Two real server bugs fixed over the bus.
- [ ] OWNER DECISION (the only remaining gate): formalize bus-primary across all three.
      Current drift to reconcile: client is already BUS-ONLY (owner-directed); classic +
      sync are still dual. Decide: (a) decommission `.md` auto-monitoring/docs in sync +
      classic too -> bus is the one channel; or (b) re-align client to dual.
      Recommendation: (a) — the bus is proven load-bearing and client already moved.
