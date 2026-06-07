# agent-bus — backlog

v0.2.1 shipped (2026-06-07): unregister, roster, wizard card+confirm, fswatch wrapper.
v0.2.0 shipped (2026-06-07): peek, tasks, prune, whoami, doctor, self-echo fix, receipts, --unread.

## Prompting / wizard QoL
- [x] Wizard: `--card` step — asks capability card before writing config. (v0.2.1)
- [x] Wizard: confirm screen (tool/repo/id/card summary + Apply/Cancel). (v0.2.1)
- [ ] Team Select already fuzzy-filters (inquire) — document it; consider showing agent counts inline.
- [ ] `--yes` / non-interactive guard so CI never blocks on a prompt.

## New commands
- [x] `whoami` — team/alias, db path, config source. (v0.2.0)
- [x] `doctor` — bus.db reachable, binary on PATH, .mcp.json, enabledMcpjsonServers, stale peers, open tasks. (v0.2.0)
- [x] `roster` — alias for `peers` scoped to my team. (v0.2.1)
- [x] `unregister [--as a] [--team t]` — drop a peer from the registry. (v0.2.1)

## Bugs / warts
- [x] Team/global broadcast self-echo excluded from poll results. (v0.2.0)
- [x] Bootstrap block: poll immediately on session start to drain restart backlog. (v0.2.0)
- [x] Bootstrap block: cross-platform stat (macOS -f %m || Linux -c %Y). (v0.2.0)
- [ ] `version` shows `-dirty` from build.rs — build from clean tree before releasing.

## Wake / delivery
- [x] Receipts: poll writes read receipts; peek shows read_by list; peers --unread. (v0.2.0)
- [x] Message TTL / retention: `prune --days N`. (v0.2.0)
- [x] `scripts/bus-watch.sh` — fswatch/inotifywait doorbell; falls back to poll loop. (v0.2.1)

## Read-only / task visibility
- [x] `peek [--limit N] [--task-id id] [--since-id N]` — read-only, no cursor advance. (v0.2.0)
- [x] `tasks [--filter all|open|mine|for-me]` — task rollup by task_id. (v0.2.0)

## A2A north side (deferred — see SPEC.md §6)
- [ ] Speak A2A on the wire (tasks + Agent Cards) when a non-CLI / cross-machine / cross-vendor
      agent needs to join. Adopt the contract; defer auth/signed-cards/SSE until needed.

## Cutover (parallel-trial -> agent-bus primary)
Owner decision, applied to ALL THREE sessions at once. Preconditions:
- [x] all three (sync/classic/client) restarted + actively polling (fresh `last_seen`)
- [x] real client -> classic -> client round-trip over the bus (2026-06-07)
- [ ] OWNER DECISION: classic still dual (.md + bus). Recommend: cut classic to bus-only
      (bus is proven, client already bus-only, sync bus-only). Or keep dual indefinitely.
