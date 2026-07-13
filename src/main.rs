// agent-bus — minimal A2A-shaped message bus for coordinating CLI agent sessions.
//
//   agent-bus                           first-time interactive setup wizard
//   agent-bus serve                     MCP stdio server
//   agent-bus install [--tool ..]       non-interactive installer / wizard
//   agent-bus send --to X --body Y      send a message
//   agent-bus poll  [--as a] [--team t] fetch new messages, advance cursor
//   agent-bus peek  [--limit N] [--task-id id] [--since-id N]  read-only; no cursor advance
//   agent-bus tasks [--filter all|open|mine|for-me]
//   agent-bus prune [--days N]          delete messages older than N days
//   agent-bus peers [--team t|*] [--unread]
//   agent-bus whoami                    print identity + config source
//   agent-bus doctor                    check bus health
//   agent-bus register [--card ..]
//   agent-bus teams
//   agent-bus version
//
// Identity = AGENT_BUS_TEAM/AGENT_BUS_ALIAS (MCP serve) or --team/--as (CLI).
// A2A-shaped: task_id + lifecycle state (submitted→working→completed|failed).

use inquire::{InquireError, Select, Text};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

fn version_string() -> String {
    format!(
        "agent-bus {} (git {}, built {})",
        env!("CARGO_PKG_VERSION"),
        env!("AGENT_BUS_GIT"),
        env!("AGENT_BUS_BUILD")
    )
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT,
    sender_team     TEXT,
    sender_alias    TEXT,
    recipient_team  TEXT,
    recipient_alias TEXT,
    type            TEXT,
    state           TEXT,
    body            TEXT,
    created_at      INTEGER
);
CREATE TABLE IF NOT EXISTS cursors (
    team    TEXT,
    alias   TEXT,
    last_id INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(team, alias)
);
CREATE TABLE IF NOT EXISTS peers (
    team      TEXT,
    alias     TEXT,
    card      TEXT,
    last_seen INTEGER,
    PRIMARY KEY(team, alias)
);
CREATE TABLE IF NOT EXISTS receipts (
    message_id   INTEGER,
    reader_team  TEXT,
    reader_alias TEXT,
    read_at      INTEGER,
    PRIMARY KEY(message_id, reader_team, reader_alias)
);
CREATE TABLE IF NOT EXISTS teams (
    name       TEXT PRIMARY KEY,
    created_at INTEGER
);
-- teams used to be derived from peers; backfill so existing ones survive the migration
INSERT OR IGNORE INTO teams(name, created_at) SELECT DISTINCT team, 0 FROM peers;
";

// ---------------------------------------------------------------- paths/util
fn home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into()))
}
fn bus_home() -> PathBuf {
    std::env::var("AGENT_BUS_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".agent-bus"))
}
fn db_path() -> PathBuf {
    std::env::var("AGENT_BUS_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|_| bus_home().join("bus.db"))
}
fn inbox_dir() -> PathBuf {
    bus_home().join("inbox")
}
fn flag_path(team: &str, alias: &str) -> PathBuf {
    inbox_dir().join(team).join(format!("{}.flag", alias))
}
fn team_env() -> String {
    std::env::var("AGENT_BUS_TEAM").unwrap_or_else(|_| "default".into())
}
fn alias_env() -> String {
    std::env::var("AGENT_BUS_ALIAS").unwrap_or_else(|_| "unknown".into())
}
fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
fn short_id() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    format!("{:08x}", n.wrapping_mul(2654435761) as u32)
}

// ---------------------------------------------------------------- storage
fn init_schema(conn: &Connection) {
    conn.execute_batch(SCHEMA).expect("init schema");
}
fn open_db() -> Connection {
    fs::create_dir_all(bus_home()).ok();
    fs::create_dir_all(inbox_dir()).ok();
    let conn = Connection::open(db_path()).expect("open bus.db");
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;").ok();
    init_schema(&conn);
    conn
}

// touch presence only — never create the peer. inserting here conjured card-less ghost
// peers from a single poll, which made uninstalled identities look registered and
// defeated the recipient check in send()
fn mark_seen(conn: &Connection, team: &str, alias: &str) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE peers SET last_seen=?3 WHERE team=?1 AND alias=?2",
        params![team, alias, now_ms()],
    )?;
    Ok(())
}

// recipient addressing: `to` -> (recipient_team, recipient_alias)
//   "all"          -> (my_team, "*")   team broadcast
//   "*"|"everyone" -> ("*", "*")       global broadcast
//   "team:NAME"    -> (NAME, "*")      named-team broadcast
//   "team/alias"   -> (team, alias)    cross-team direct
//   "alias"        -> (my_team, alias) same-team direct
fn resolve_to(to: &str, my_team: &str) -> (String, String) {
    if to == "all" {
        (my_team.to_string(), "*".into())
    } else if to == "*" || to == "everyone" {
        ("*".into(), "*".into())
    } else if let Some(t) = to.strip_prefix("team:") {
        (t.to_string(), "*".into())
    } else if let Some((t, a)) = to.split_once('/') {
        (t.to_string(), a.to_string())
    } else {
        (my_team.to_string(), to.to_string())
    }
}

fn touch_doorbell(conn: &Connection, rt: &str, ra: &str, my_team: &str, my_alias: &str) {
    let targets: Vec<(String, String)> = if ra == "*" {
        let (sql, p): (&str, Vec<String>) = if rt == "*" {
            ("SELECT team, alias FROM peers", vec![])
        } else {
            ("SELECT team, alias FROM peers WHERE team=?1", vec![rt.to_string()])
        };
        let mut v = vec![];
        if let Ok(mut s) = conn.prepare(sql) {
            let rows = s.query_map(rusqlite::params_from_iter(p.iter()), |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            });
            if let Ok(rows) = rows {
                for t in rows.flatten() {
                    v.push(t);
                }
            }
        }
        v
    } else {
        vec![(rt.to_string(), ra.to_string())]
    };
    for (t, a) in targets {
        if t == my_team && a == my_alias {
            continue;
        }
        let fp = flag_path(&t, &a);
        if let Some(parent) = fp.parent() {
            fs::create_dir_all(parent).ok();
        }
        let _ = fs::write(fp, now_ms().to_string());
    }
}

// ---------------------------------------------------------------- row helpers
fn row_to_msg(r: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    let st = r.get::<_, String>(2)?;
    let sa = r.get::<_, String>(3)?;
    let rt = r.get::<_, String>(4)?;
    let ra = r.get::<_, String>(5)?;
    Ok(json!({
        "id":      r.get::<_, i64>(0)?,
        "task_id": r.get::<_, String>(1)?,
        "from":    format!("{}/{}", st, sa),
        "to":      format!("{}/{}", rt, ra),
        "type":    r.get::<_, String>(6)?,
        "state":   r.get::<_, String>(7)?,
        "body":    r.get::<_, String>(8)?,
        "at":      r.get::<_, i64>(9)?,
    }))
}

fn task_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Value> {
    // columns: task_id(0), type(1), state(2), sender_team(3), sender_alias(4),
    //          recipient_team(5), recipient_alias(6), body(7), created_at(8)
    let ft = r.get::<_, String>(3)?;
    let fa = r.get::<_, String>(4)?;
    let tt = r.get::<_, String>(5)?;
    let ta = r.get::<_, String>(6)?;
    Ok(json!({
        "task_id": r.get::<_, String>(0)?,
        "type":    r.get::<_, String>(1)?,
        "state":   r.get::<_, String>(2)?,
        "from":    format!("{}/{}", ft, fa),
        "to":      format!("{}/{}", tt, ta),
        "body":    r.get::<_, String>(7)?,
        "at":      r.get::<_, i64>(8)?,
    }))
}

// ---------------------------------------------------------------- tools
fn tool_register(conn: &Connection, team: &str, alias: &str, args: &Value) -> rusqlite::Result<Value> {
    let card = args["card"].as_str();
    ensure_team(conn, team)?; // registering into a new team creates it
    conn.execute(
        "INSERT INTO peers(team, alias, card, last_seen) VALUES(?1, ?2, ?3, ?4) \
         ON CONFLICT(team, alias) DO UPDATE SET card=COALESCE(?3, peers.card), last_seen=?4",
        params![team, alias, card, now_ms()],
    )?;
    Ok(json!({"ok": true, "team": team, "alias": alias, "id": format!("{}/{}", team, alias)}))
}

fn tool_send(conn: &Connection, team: &str, alias: &str, args: &Value) -> rusqlite::Result<Value> {
    let to = match args["to"].as_str() {
        Some(s) => s,
        None => return Ok(json!({"ok": false, "error": "missing 'to'"})),
    };
    let (rt, ra) = resolve_to(to, team);
    let body  = args["body"].as_str().unwrap_or("");
    let mtype = args["type"].as_str().unwrap_or("message");
    let state = args["state"].as_str().map(|s| s.to_string()).unwrap_or_else(|| {
        if mtype == "task" { "submitted".into() } else { "info".into() }
    });
    let task_id = args["task_id"].as_str().map(|s| s.to_string()).unwrap_or_else(short_id);
    conn.execute(
        "INSERT INTO messages(task_id, sender_team, sender_alias, recipient_team, recipient_alias, type, state, body, created_at) \
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![task_id, team, alias, rt, ra, mtype, state, body, now_ms()],
    )?;
    let id = conn.last_insert_rowid();
    mark_seen(conn, team, alias)?;
    touch_doorbell(conn, &rt, &ra, team, alias);

    let mut out = json!({"ok": true, "id": id, "task_id": task_id, "to": format!("{}/{}", rt, ra)});
    // a direct send to an unregistered identity is still delivered if that agent later
    // registers under it (poll matches on recipient, cursor starts at 0) — but a typo'd
    // alias is a silent black hole, so surface the ambiguity instead of hiding it
    if ra != "*" && !peer_exists(conn, &rt, &ra) {
        out["recipient_registered"] = json!(false);
        out["warning"] = json!(format!(
            "no peer '{}/{}' is registered — delivered only if an agent later registers under that exact identity",
            rt, ra
        ));
        out["known_peers"] = json!(all_peer_ids(conn));
    }
    Ok(out)
}

fn tool_poll(conn: &Connection, team: &str, alias: &str) -> rusqlite::Result<Value> {
    mark_seen(conn, team, alias)?;
    let last: i64 = conn
        .query_row(
            "SELECT last_id FROM cursors WHERE team=?1 AND alias=?2",
            params![team, alias],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0);

    // self-echo exclusion: broadcast messages sent by ME are excluded from MY poll
    let mut stmt = conn.prepare(
        "SELECT id, task_id, sender_team, sender_alias, recipient_team, recipient_alias, type, state, body, created_at \
         FROM messages WHERE id > ?1 AND ( \
           (recipient_team=?2 AND recipient_alias=?3) \
           OR (recipient_alias='*' AND (recipient_team=?2 OR recipient_team='*') \
               AND NOT (sender_team=?2 AND sender_alias=?3)) \
         ) ORDER BY id",
    )?;
    let msgs: Vec<Value> = stmt
        .query_map(params![last, team, alias], row_to_msg)?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    if let Some(lastmsg) = msgs.last() {
        let newlast = lastmsg["id"].as_i64().unwrap();
        conn.execute(
            "INSERT INTO cursors(team, alias, last_id) VALUES(?1, ?2, ?3) \
             ON CONFLICT(team, alias) DO UPDATE SET last_id=?3",
            params![team, alias, newlast],
        )?;
        let _ = fs::remove_file(flag_path(team, alias));
    }

    // write receipts for all delivered messages
    drop(stmt);
    for msg in &msgs {
        let mid = msg["id"].as_i64().unwrap_or(0);
        conn.execute(
            "INSERT OR IGNORE INTO receipts(message_id, reader_team, reader_alias, read_at) \
             VALUES(?1, ?2, ?3, ?4)",
            params![mid, team, alias, now_ms()],
        )?;
    }

    Ok(json!({"ok": true, "count": msgs.len(), "messages": msgs}))
}

// read-only view — does NOT advance cursor or write receipts
fn tool_peek(conn: &Connection, args: &Value) -> rusqlite::Result<Value> {
    let limit   = args["limit"].as_i64().unwrap_or(50).max(1).min(1000);
    let task_id = args["task_id"].as_str();
    let since_id = args["since_id"].as_i64();

    let mut msgs: Vec<Value> = if let Some(tid) = task_id {
        let mut stmt = conn.prepare(
            "SELECT id, task_id, sender_team, sender_alias, recipient_team, recipient_alias, \
             type, state, body, created_at FROM messages WHERE task_id=?1 ORDER BY id",
        )?;
        let x = stmt.query_map(params![tid], row_to_msg)?.collect::<rusqlite::Result<Vec<_>>>()?; x
    } else if let Some(sid) = since_id {
        let mut stmt = conn.prepare(
            "SELECT id, task_id, sender_team, sender_alias, recipient_team, recipient_alias, \
             type, state, body, created_at FROM messages WHERE id > ?1 ORDER BY id ASC LIMIT ?2",
        )?;
        let x = stmt.query_map(params![sid, limit], row_to_msg)?.collect::<rusqlite::Result<Vec<_>>>()?; x
    } else {
        let mut stmt = conn.prepare(
            "SELECT id, task_id, sender_team, sender_alias, recipient_team, recipient_alias, \
             type, state, body, created_at FROM messages ORDER BY id DESC LIMIT ?1",
        )?;
        let mut v: Vec<Value> = stmt.query_map(params![limit], row_to_msg)?.collect::<rusqlite::Result<Vec<_>>>()?;
        v.reverse();
        v
    };

    // annotate with read_by (bulk fetch to avoid N+1)
    if !msgs.is_empty() {
        let ids: Vec<i64> = msgs.iter().filter_map(|m| m["id"].as_i64()).collect();
        let placeholders = (1..=ids.len())
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT message_id, reader_team || '/' || reader_alias FROM receipts \
             WHERE message_id IN ({}) ORDER BY message_id, read_at",
            placeholders
        );
        if let Ok(mut stmt) = conn.prepare(&sql) {
            if let Ok(rows) = stmt.query_map(rusqlite::params_from_iter(ids.iter()), |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
            }) {
                let receipts: Vec<(i64, String)> = rows.flatten().collect();
                let mut by_id: HashMap<i64, Vec<String>> = HashMap::new();
                for (mid, reader) in receipts {
                    by_id.entry(mid).or_default().push(reader);
                }
                for msg in &mut msgs {
                    let mid = msg["id"].as_i64().unwrap_or(0);
                    if let Some(readers) = by_id.get(&mid) {
                        msg["read_by"] = json!(readers);
                    }
                }
            }
        }
    }

    Ok(json!({"ok": true, "count": msgs.len(), "messages": msgs}))
}

// task rollup — latest state per task_id, using first message for from/to/type
fn tool_tasks(conn: &Connection, team: &str, alias: &str, args: &Value) -> rusqlite::Result<Value> {
    let filter = args["filter"].as_str().unwrap_or("all");

    let base = "SELECT fm.task_id, fm.type, lm.state, \
                fm.sender_team, fm.sender_alias, fm.recipient_team, fm.recipient_alias, \
                lm.body, lm.created_at \
                FROM messages fm \
                INNER JOIN (SELECT task_id, MIN(id) first_id, MAX(id) last_id \
                            FROM messages GROUP BY task_id) agg ON fm.id = agg.first_id \
                INNER JOIN messages lm ON lm.id = agg.last_id";

    let tasks: Vec<Value> = match filter {
        "open" => {
            // 'info' is a status state, not open work — must match doctor's open_tasks count
            let sql = format!("{} WHERE lm.state NOT IN ('completed','failed','info') ORDER BY lm.created_at DESC", base);
            let mut stmt = conn.prepare(&sql)?;
            let x = stmt.query_map([], task_row)?.collect::<rusqlite::Result<Vec<_>>>()?; x
        }
        "mine" => {
            let sql = format!("{} WHERE fm.sender_team=?1 AND fm.sender_alias=?2 ORDER BY lm.created_at DESC", base);
            let mut stmt = conn.prepare(&sql)?;
            let x = stmt.query_map(params![team, alias], task_row)?.collect::<rusqlite::Result<Vec<_>>>()?; x
        }
        "for-me" | "for_me" => {
            let sql = format!(
                "{} WHERE (fm.recipient_team=?1 AND (fm.recipient_alias=?2 OR fm.recipient_alias='*')) \
                 OR fm.recipient_team='*' ORDER BY lm.created_at DESC",
                base
            );
            let mut stmt = conn.prepare(&sql)?;
            let x = stmt.query_map(params![team, alias], task_row)?.collect::<rusqlite::Result<Vec<_>>>()?; x
        }
        _ => {
            let sql = format!("{} ORDER BY lm.created_at DESC", base);
            let mut stmt = conn.prepare(&sql)?;
            let x = stmt.query_map([], task_row)?.collect::<rusqlite::Result<Vec<_>>>()?; x
        }
    };

    Ok(json!({"ok": true, "count": tasks.len(), "filter": filter, "tasks": tasks}))
}

fn tool_prune(conn: &Connection, args: &Value) -> rusqlite::Result<Value> {
    let days   = args["days"].as_i64().unwrap_or(30).max(1);
    let cutoff = now_ms() - days * 86_400_000;
    let n = conn.execute("DELETE FROM messages WHERE created_at < ?1", params![cutoff])?;
    conn.execute(
        "DELETE FROM receipts WHERE message_id NOT IN (SELECT id FROM messages)",
        [],
    )?;
    Ok(json!({"ok": true, "deleted": n, "days": days}))
}

// the identity a repo's .mcp.json configures the agent-bus server to run as
fn mcp_identity(mcp_path: &Path) -> Option<(String, String)> {
    let v: Value = serde_json::from_str(&fs::read_to_string(mcp_path).ok()?).ok()?;
    let env = &v["mcpServers"]["agent-bus"]["env"];
    Some((env["AGENT_BUS_TEAM"].as_str()?.to_string(),
          env["AGENT_BUS_ALIAS"].as_str()?.to_string()))
}

fn value_after(tokens: &[&str], flag: &str) -> Option<String> {
    tokens.iter().position(|t| *t == flag)
        .and_then(|i| tokens.get(i + 1))
        .map(|v| v.trim_matches(|c| c == '"' || c == '\'' || c == ';').to_string())
}

// the EXPECTED identity, recovered from durable per-repo evidence when .mcp.json is
// gone: the SessionStart hook's --team/--as, else the CLAUDE.local.md bootstrap line
fn discover_identity(cwd: &Path) -> Option<(String, String)> {
    if let Ok(s) = fs::read_to_string(cwd.join(".claude/settings.local.json")) {
        let toks: Vec<&str> = s.split_whitespace().collect();
        if let (Some(t), Some(a)) = (value_after(&toks, "--team"), value_after(&toks, "--as")) {
            return Some((t, a));
        }
    }
    if let Ok(s) = fs::read_to_string(cwd.join("CLAUDE.local.md")) {
        // ...identity `team/alias`...
        if let Some(i) = s.find("identity `") {
            let rest = &s[i + "identity `".len()..];
            if let Some(end) = rest.find('`') {
                if let Some((t, a)) = rest[..end].split_once('/') {
                    return Some((t.to_string(), a.to_string()));
                }
            }
        }
    }
    None
}

// any local marker that this dir is an agent-bus repo (so a global hook can run
// `doctor --quiet` everywhere and stay silent in unrelated projects)
fn is_agentbus_repo(cwd: &Path) -> bool {
    let has = |rel: &str, needle: &str| {
        fs::read_to_string(cwd.join(rel)).map(|s| s.contains(needle)).unwrap_or(false)
    };
    has(".mcp.json", "agent-bus")
        || has("CLAUDE.local.md", BLOCK_START)
        || has(".claude/settings.local.json", "agent-bus")
}

fn global_settings_path() -> PathBuf { home_dir().join(".claude").join("settings.json") }

// merge a "agent-bus doctor --quiet" SessionStart hook into a global settings file,
// preserving every other setting and hook. returns false if it was already there.
fn add_global_hook(path: &Path) -> bool {
    let cmd = "agent-bus doctor --quiet";
    let mut root: Value = fs::read_to_string(path).ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_else(|| json!({}));
    if !root.is_object() { root = json!({}); }
    let present = root["hooks"]["SessionStart"].as_array().map(|arr| arr.iter().any(|e| {
        e["hooks"].as_array().map(|h| h.iter().any(|hk|
            hk["command"].as_str().map(|c| c.contains("agent-bus doctor")).unwrap_or(false)
        )).unwrap_or(false)
    })).unwrap_or(false);
    if present { return false; }

    if !root["hooks"].is_object() { root["hooks"] = json!({}); }
    let mut starts = root["hooks"]["SessionStart"].as_array().cloned().unwrap_or_default();
    starts.push(json!({"hooks": [{"type": "command", "command": cmd}]}));
    root["hooks"]["SessionStart"] = json!(starts);
    if path.exists() { let _ = fs::copy(path, path.with_extension("json.bak")); }
    if let Some(p) = path.parent() { let _ = fs::create_dir_all(p); }
    fs::write(path, serde_json::to_string_pretty(&root).unwrap() + "\n").expect("write global settings.json");
    true
}

fn global_hook_present() -> bool {
    let Ok(s) = fs::read_to_string(global_settings_path()) else { return false };
    let Ok(v) = serde_json::from_str::<Value>(&s) else { return false };
    v["hooks"]["SessionStart"].as_array().map(|arr| arr.iter().any(|e| {
        e["hooks"].as_array().map(|h| h.iter().any(|hk|
            hk["command"].as_str().map(|c| c.contains("agent-bus doctor")).unwrap_or(false)
        )).unwrap_or(false)
    })).unwrap_or(false)
}

// detect (and with fix, reconcile) bus state left dangling by deletions/renames:
//   - open tasks addressed to a recipient that is no longer a registered peer
//   - cursors belonging to a peer that no longer exists
fn tool_sync(conn: &Connection, fix: bool) -> rusqlite::Result<Value> {
    let mut orphans: Vec<Value> = vec![];
    {
        let mut stmt = conn.prepare(
            "SELECT a.task_id, fm.recipient_team, fm.recipient_alias, \
                    fm.sender_team, fm.sender_alias, lm.state \
             FROM messages fm \
             JOIN (SELECT task_id, MIN(id) fid, MAX(id) lid FROM messages GROUP BY task_id) a ON fm.id=a.fid \
             JOIN messages lm ON lm.id=a.lid \
             WHERE lm.state IN ('submitted','working','accepted') AND fm.recipient_alias<>'*'",
        )?;
        let rows = stmt.query_map([], |r| Ok((
            r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?,
            r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?,
        )))?;
        for (task_id, rt, ra, st, sa, state) in rows.flatten() {
            if !peer_exists(conn, &rt, &ra) {
                orphans.push(json!({
                    "task_id": task_id, "to": format!("{}/{}", rt, ra),
                    "from": format!("{}/{}", st, sa), "state": state,
                    "_st": st, "_sa": sa,
                }));
            }
        }
    }

    let mut dead: Vec<Value> = vec![];
    {
        let mut stmt = conn.prepare("SELECT team, alias FROM cursors")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        for (t, a) in rows.flatten() {
            if !peer_exists(conn, &t, &a) {
                dead.push(json!({"team": t, "alias": a}));
            }
        }
    }

    let (mut failed, mut pruned) = (0u64, 0u64);
    if fix {
        for o in &orphans {
            // append a terminal 'failed' back to the sender so they stop waiting
            conn.execute(
                "INSERT INTO messages(task_id, sender_team, sender_alias, recipient_team, recipient_alias, type, state, body, created_at) \
                 VALUES(?1,'bus','sync',?2,?3,'task','failed',?4,?5)",
                params![
                    o["task_id"].as_str().unwrap_or(""),
                    o["_st"].as_str().unwrap_or(""), o["_sa"].as_str().unwrap_or(""),
                    format!("auto-failed by sync: recipient {} is no longer registered", o["to"].as_str().unwrap_or("?")),
                    now_ms()
                ],
            )?;
            touch_doorbell(conn, o["_st"].as_str().unwrap_or(""), o["_sa"].as_str().unwrap_or(""), "bus", "sync");
            failed += 1;
        }
        for d in &dead {
            pruned += conn.execute(
                "DELETE FROM cursors WHERE team=?1 AND alias=?2",
                params![d["team"].as_str().unwrap_or(""), d["alias"].as_str().unwrap_or("")],
            )? as u64;
        }
    }

    // strip internal fields from the reported orphans
    let orphans_out: Vec<Value> = orphans.iter().map(|o| json!({
        "task_id": o["task_id"], "to": o["to"], "from": o["from"], "state": o["state"]
    })).collect();

    Ok(json!({
        "ok": true, "fixed": fix,
        "orphaned_tasks": orphans_out, "dead_cursors": dead,
        "failed_tasks": failed, "pruned_cursors": pruned,
    }))
}

fn tool_unregister(conn: &Connection, team: &str, alias: &str, args: &Value) -> rusqlite::Result<Value> {
    let target_team  = args["team"].as_str().unwrap_or(team);
    let target_alias = args["alias"].as_str()
        .or_else(|| args["as"].as_str())
        .unwrap_or(alias);
    let n = conn.execute(
        "DELETE FROM peers WHERE team=?1 AND alias=?2",
        params![target_team, target_alias],
    )?;
    Ok(json!({"ok": true, "removed": n > 0, "id": format!("{}/{}", target_team, target_alias)}))
}

fn tool_peers(conn: &Connection, my_team: &str, args: &Value) -> rusqlite::Result<Value> {
    let filter     = args["team"].as_str().unwrap_or(my_team);
    let show_unread = args["unread"].as_bool().unwrap_or(false);

    let (sql, p): (&str, Vec<String>) = if filter == "*" {
        ("SELECT team, alias, card, last_seen FROM peers ORDER BY team, alias", vec![])
    } else {
        (
            "SELECT team, alias, card, last_seen FROM peers WHERE team=?1 ORDER BY alias",
            vec![filter.to_string()],
        )
    };
    let mut stmt = conn.prepare(sql)?;
    let peer_data: Vec<(String, String, Option<String>, i64)> = stmt
        .query_map(rusqlite::params_from_iter(p.iter()), |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt); // release conn borrow before using conn in closure

    let peers: Vec<Value> = peer_data
        .into_iter()
        .map(|(t, a, card, ls)| {
            let mut obj = json!({
                "id":        format!("{}/{}", t, a),
                "team":      t.clone(),
                "alias":     a.clone(),
                "card":      card,
                "last_seen": ls,
            });
            if show_unread {
                // messages addressed to this peer with no receipt yet
                let unread: i64 = conn
                    .query_row(
                        "SELECT COUNT(*) FROM messages m \
                         WHERE ((m.recipient_team=?1 AND (m.recipient_alias=?2 OR m.recipient_alias='*')) \
                                OR (m.recipient_team='*' AND m.recipient_alias='*')) \
                         AND NOT (m.sender_team=?1 AND m.sender_alias=?2) \
                         AND NOT EXISTS (\
                           SELECT 1 FROM receipts r \
                           WHERE r.message_id=m.id AND r.reader_team=?1 AND r.reader_alias=?2)",
                        params![t, a],
                        |r| r.get(0),
                    )
                    .unwrap_or(0);
                obj["unread"] = json!(unread);
            }
            obj
        })
        .collect();

    Ok(json!({"ok": true, "scope": filter, "peers": peers}))
}

// ---------------------------------------------------------------- registry helpers
// teams live in their own table, so a team with zero agents still lists (count 0)
fn list_teams(conn: &Connection) -> Vec<(String, i64)> {
    let mut stmt = match conn.prepare(
        "SELECT t.name, COUNT(p.alias) FROM teams t \
         LEFT JOIN peers p ON p.team = t.name \
         GROUP BY t.name ORDER BY t.name",
    ) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
}

fn peer_exists(conn: &Connection, team: &str, alias: &str) -> bool {
    conn.query_row(
        "SELECT 1 FROM peers WHERE team=?1 AND alias=?2",
        params![team, alias],
        |_| Ok(()),
    )
    .optional()
    .map(|o| o.is_some())
    .unwrap_or(false)
}

fn all_peer_ids(conn: &Connection) -> Vec<String> {
    let mut stmt = match conn.prepare("SELECT team || '/' || alias FROM peers ORDER BY team, alias") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    stmt.query_map([], |r| r.get::<_, String>(0))
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
}

fn ensure_team(conn: &Connection, name: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO teams(name, created_at) VALUES(?1, ?2)",
        params![name, now_ms()],
    )?;
    Ok(())
}

fn team_row_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row("SELECT 1 FROM teams WHERE name=?1", params![name], |_| Ok(()))
        .optional().map(|o| o.is_some()).unwrap_or(false)
}

// team CRUD: create/read(=peers,teams)/update(rename)/delete
fn tool_create_team(conn: &Connection, name: &str) -> rusqlite::Result<Value> {
    if !valid_name(name) {
        return Ok(json!({"ok": false, "error": "team name must be non-empty, no spaces or '/'"}));
    }
    if team_row_exists(conn, name) {
        return Ok(json!({"ok": false, "error": format!("team '{}' already exists", name)}));
    }
    ensure_team(conn, name)?;
    Ok(json!({"ok": true, "created": true, "team": name}))
}

fn tool_delete_team(conn: &Connection, name: &str, force: bool) -> rusqlite::Result<Value> {
    if !team_row_exists(conn, name) {
        return Ok(json!({"ok": false, "error": format!("no such team '{}'", name)}));
    }
    let members = list_aliases(conn, name);
    if !members.is_empty() && !force {
        return Ok(json!({
            "ok": false, "needs_force": true, "team": name, "agents": members,
            "error": format!("team '{}' has {} agent(s) — re-run with --force to remove them too", name, members.len())
        }));
    }
    // message history is an append-only log; leave it, but drop live state
    conn.execute("DELETE FROM peers   WHERE team=?1", params![name])?;
    conn.execute("DELETE FROM cursors WHERE team=?1", params![name])?;
    conn.execute("DELETE FROM teams   WHERE name=?1", params![name])?;
    Ok(json!({"ok": true, "deleted": true, "team": name, "removed_agents": members.len()}))
}

fn tool_rename_team(conn: &Connection, from: &str, to: &str) -> rusqlite::Result<Value> {
    if !team_row_exists(conn, from) {
        return Ok(json!({"ok": false, "error": format!("no such team '{}'", from)}));
    }
    if !valid_name(to) {
        return Ok(json!({"ok": false, "error": "new team name must be non-empty, no spaces or '/'"}));
    }
    if team_row_exists(conn, to) {
        return Ok(json!({"ok": false, "error": format!("team '{}' already exists", to)}));
    }
    // cascade the rename across every table that stores a team, atomically. exact
    // matches only — '*' broadcasts and other teams are untouched
    let tx = conn.unchecked_transaction()?;
    tx.execute("UPDATE teams    SET name=?2           WHERE name=?1",           params![from, to])?;
    let n = tx.execute("UPDATE peers SET team=?2      WHERE team=?1",           params![from, to])?;
    tx.execute("UPDATE cursors  SET team=?2           WHERE team=?1",           params![from, to])?;
    tx.execute("UPDATE messages SET sender_team=?2    WHERE sender_team=?1",    params![from, to])?;
    tx.execute("UPDATE messages SET recipient_team=?2 WHERE recipient_team=?1", params![from, to])?;
    tx.execute("UPDATE receipts SET reader_team=?2    WHERE reader_team=?1",    params![from, to])?;
    tx.commit()?;
    Ok(json!({"ok": true, "renamed": true, "from": from, "to": to, "agents": n}))
}
fn list_aliases(conn: &Connection, team: &str) -> Vec<String> {
    let mut stmt = match conn.prepare("SELECT alias FROM peers WHERE team=?1 ORDER BY alias") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    stmt.query_map(params![team], |r| r.get::<_, String>(0))
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
}

fn call_tool(conn: &Connection, team: &str, alias: &str, name: &str, args: &Value) -> Value {
    let r: rusqlite::Result<Value> = match name {
        "register"          => tool_register(conn, team, alias, args),
        "send"              => tool_send(conn, team, alias, args),
        "poll"              => tool_poll(conn, team, alias),
        "peek" | "history"  => tool_peek(conn, args),
        "tasks"             => tool_tasks(conn, team, alias, args),
        "peers"             => tool_peers(conn, team, args),
        "prune"             => tool_prune(conn, args),
        "unregister"        => tool_unregister(conn, team, alias, args),
        "sync"              => tool_sync(conn, args["fix"].as_bool().unwrap_or(false)),
        "create-team"       => tool_create_team(conn, args["name"].as_str().unwrap_or("")),
        "delete-team"       => tool_delete_team(conn, args["name"].as_str().unwrap_or(""), args["force"].as_bool().unwrap_or(false)),
        "rename-team"       => tool_rename_team(conn, args["from"].as_str().unwrap_or(""), args["to"].as_str().unwrap_or("")),
        _ => return json!({"ok": false, "error": format!("unknown tool: {}", name)}),
    };
    match r {
        Ok(v)  => v,
        Err(e) => json!({"ok": false, "error": e.to_string()}),
    }
}

// ---------------------------------------------------------------- pretty CLI output
// every cli command renders human-readable by default; --json restores the raw object
const PAD: &str = "         "; // aligns continuation lines under "  #1234  "

fn ago(at_ms: i64) -> String {
    let d = (now_ms() - at_ms).max(0) / 1000;
    match d {
        0..=4        => "just now".to_string(),
        5..=59       => format!("{}s ago", d),
        60..=3599    => format!("{}m ago", d / 60),
        3600..=86399 => format!("{}h ago", d / 3600),
        _            => format!("{}d ago", d / 86400),
    }
}

fn ellipsis(s: &str, max: usize) -> String {
    let flat = s.replace('\n', " ");
    if flat.chars().count() <= max { return flat; }
    let head: String = flat.chars().take(max.saturating_sub(1)).collect();
    format!("{}…", head.trim_end())
}

fn body_block(body: &str, max_lines: usize) -> String {
    let lines: Vec<&str> = body.lines().collect();
    let mut out: Vec<String> = lines.iter().take(max_lines).map(|l| format!("{}{}", PAD, l)).collect();
    if lines.len() > max_lines {
        out.push(format!("{}… (+{} more lines)", PAD, lines.len() - max_lines));
    }
    out.join("\n")
}

fn plural(n: usize) -> &'static str { if n == 1 { "" } else { "s" } }

fn print_msgs(msgs: &[Value]) {
    for m in msgs {
        println!(
            "  #{:<6} {} → {}",
            m["id"].as_i64().unwrap_or(0),
            m["from"].as_str().unwrap_or("?"),
            m["to"].as_str().unwrap_or("?")
        );
        let mut meta = vec![m["type"].as_str().unwrap_or("message").to_string()];
        if let Some(s) = m["state"].as_str()   { if !s.is_empty() { meta.push(s.into()); } }
        if let Some(t) = m["task_id"].as_str() { if !t.is_empty() { meta.push(format!("task {}", t)); } }
        meta.push(ago(m["at"].as_i64().unwrap_or(0)));
        if let Some(rb) = m["read_by"].as_array() {
            let readers: Vec<&str> = rb.iter().filter_map(|v| v.as_str()).collect();
            if !readers.is_empty() { meta.push(format!("read by {}", readers.join(", "))); }
        }
        println!("{}{}", PAD, meta.join(" · "));
        let b = m["body"].as_str().unwrap_or("");
        if !b.trim().is_empty() { println!("{}", body_block(b, 6)); }
        println!();
    }
}

fn emit(cmd: &str, v: &Value, flags: &HashMap<String, String>) {
    if flags.contains_key("json") { println!("{}", v); return; }
    if !v["ok"].as_bool().unwrap_or(true) {
        eprintln!("✗ {}", v["error"].as_str().unwrap_or("unknown error"));
        // delete-team refusal lists the agents that block it
        if let Some(a) = v["agents"].as_array() {
            let ids: Vec<&str> = a.iter().filter_map(|x| x.as_str()).collect();
            if !ids.is_empty() { eprintln!("  agents: {}", ids.join(", ")); }
        }
        std::process::exit(1);
    }
    match cmd {
        "poll" | "peek" => {
            let msgs = v["messages"].as_array().cloned().unwrap_or_default();
            if msgs.is_empty() {
                println!("○ no {}messages", if cmd == "poll" { "new " } else { "" });
                return;
            }
            let kind = if cmd == "poll" { "new message" } else { "message" };
            println!("● {} {}{}\n", msgs.len(), kind, plural(msgs.len()));
            print_msgs(&msgs);
        }
        "send" => {
            println!("● sent #{} → {}", v["id"].as_i64().unwrap_or(0), v["to"].as_str().unwrap_or("?"));
            if let Some(t) = v["task_id"].as_str() { println!("  task {}", t); }
            if let Some(w) = v["warning"].as_str() {
                println!("! {}", w);
                if let Some(k) = v["known_peers"].as_array() {
                    let ids: Vec<&str> = k.iter().filter_map(|x| x.as_str()).collect();
                    if !ids.is_empty() { println!("  registered peers: {}", ids.join(", ")); }
                }
            }
        }
        "sync" => {
            let ot = v["orphaned_tasks"].as_array().cloned().unwrap_or_default();
            let dc = v["dead_cursors"].as_array().cloned().unwrap_or_default();
            let fixed = v["fixed"].as_bool().unwrap_or(false);
            if ot.is_empty() && dc.is_empty() { println!("● bus in sync — no orphans"); return; }
            let mark = if fixed { "●" } else { "!" };
            if !ot.is_empty() {
                println!("{} {} orphaned open task{} (recipient no longer registered):", mark, ot.len(), plural(ot.len()));
                for t in &ot {
                    println!("  {}  {} → {}  [{}]",
                        t["task_id"].as_str().unwrap_or("?"), t["from"].as_str().unwrap_or("?"),
                        t["to"].as_str().unwrap_or("?"), t["state"].as_str().unwrap_or("?"));
                }
            }
            if !dc.is_empty() {
                println!("{} {} dead cursor{} (peer gone):", mark, dc.len(), plural(dc.len()));
                for d in &dc { println!("  {}/{}", d["team"].as_str().unwrap_or("?"), d["alias"].as_str().unwrap_or("?")); }
            }
            if fixed {
                println!("\n● reconciled: {} task{} failed, {} cursor{} pruned",
                    v["failed_tasks"].as_i64().unwrap_or(0), plural(v["failed_tasks"].as_i64().unwrap_or(0) as usize),
                    v["pruned_cursors"].as_i64().unwrap_or(0), plural(v["pruned_cursors"].as_i64().unwrap_or(0) as usize));
            } else {
                println!("\n  run `agent-bus sync --fix` to fail these tasks and prune the cursors");
            }
        }
        "create-team" => println!("● created team '{}'", v["team"].as_str().unwrap_or("?")),
        "delete-team" => {
            let n = v["removed_agents"].as_i64().unwrap_or(0);
            let tail = if n > 0 { format!(" and {} agent{}", n, plural(n as usize)) } else { String::new() };
            println!("● deleted team '{}'{}", v["team"].as_str().unwrap_or("?"), tail);
        }
        "rename-team" => {
            let n = v["agents"].as_i64().unwrap_or(0);
            let tail = if n > 0 { format!("  ({} agent{} moved)", n, plural(n as usize)) } else { String::new() };
            println!("● renamed team '{}' → '{}'{}", v["from"].as_str().unwrap_or("?"), v["to"].as_str().unwrap_or("?"), tail);
        }
        "register" => println!("● registered {}", v["id"].as_str().unwrap_or("?")),
        "unregister" => {
            let id = v["id"].as_str().unwrap_or("?");
            if v["removed"].as_bool().unwrap_or(false) { println!("● removed {}", id); }
            else                                       { println!("○ {} was not registered", id); }
        }
        "prune" => println!(
            "● pruned {} message{} older than {}d",
            v["deleted"].as_i64().unwrap_or(0),
            plural(v["deleted"].as_i64().unwrap_or(0) as usize),
            v["days"].as_i64().unwrap_or(0)
        ),
        "peers" => {
            let peers = v["peers"].as_array().cloned().unwrap_or_default();
            if peers.is_empty() { println!("○ no peers registered"); return; }
            let scope = v["scope"].as_str().unwrap_or("");
            let suffix = if scope.is_empty() { String::new() } else { format!("  (scope: {})", scope) };
            println!("● {} peer{}{}\n", peers.len(), plural(peers.len()), suffix);
            let w = peers.iter().filter_map(|p| p["id"].as_str()).map(|s| s.chars().count()).max().unwrap_or(0);
            for p in &peers {
                let mut line = format!(
                    "  {:<w$}  seen {}",
                    p["id"].as_str().unwrap_or("?"),
                    ago(p["last_seen"].as_i64().unwrap_or(0)),
                    w = w
                );
                if let Some(u) = p["unread"].as_i64() {
                    if u > 0 { line.push_str(&format!("  ·  {} unread", u)); }
                }
                println!("{}", line);
                if let Some(c) = p["card"].as_str() {
                    if !c.trim().is_empty() { println!("    {}", ellipsis(c, 96)); }
                }
            }
        }
        "tasks" => {
            let tasks = v["tasks"].as_array().cloned().unwrap_or_default();
            let filter = v["filter"].as_str().unwrap_or("all");
            if tasks.is_empty() { println!("○ no tasks  (filter: {})", filter); return; }
            println!("● {} task{}  (filter: {})\n", tasks.len(), plural(tasks.len()), filter);
            for t in &tasks {
                println!("  {}  [{}]", t["task_id"].as_str().unwrap_or("?"), t["state"].as_str().unwrap_or("?"));
                println!(
                    "    {} → {}  ·  {}",
                    t["from"].as_str().unwrap_or("?"),
                    t["to"].as_str().unwrap_or("?"),
                    ago(t["at"].as_i64().unwrap_or(0))
                );
                let b = t["body"].as_str().unwrap_or("");
                if !b.trim().is_empty() { println!("    {}", ellipsis(b, 96)); }
                println!();
            }
        }
        _ => println!("{}", serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())),
    }
}

fn tools_list() -> Value {
    json!([
        {
            "name": "register",
            "description": "Register/refresh THIS agent in the bus. Call once at session start, then immediately call poll to drain backlog.",
            "inputSchema": {"type":"object","properties":{"card":{"type":"string","description":"capability blurb (Agent Card)"}}}
        },
        {
            "name": "send",
            "description": "Send a message. to: alias (same team), 'team/alias' (cross-team), 'team:NAME' (broadcast), 'all' (my team), '*' (global). type='task' for work requests with lifecycle tracking. If the recipient is not a registered peer the reply carries recipient_registered=false plus known_peers — re-check peers before assuming an agent is absent, since agents join dynamically.",
            "inputSchema": {"type":"object","required":["to","body"],"properties":{
                "to":      {"type":"string"},
                "body":    {"type":"string"},
                "type":    {"type":"string","enum":["task","status","message"]},
                "state":   {"type":"string","enum":["submitted","working","completed","failed","info"]},
                "task_id": {"type":"string","description":"reuse to update an existing task"}
            }}
        },
        {
            "name": "poll",
            "description": "Fetch new messages since last poll; advances cursor. Excludes broadcast self-echo. Writes receipts.",
            "inputSchema": {"type":"object","properties":{
                "ack": {"type":"string","description":"optional, ignored — reserved; lets clients emit a well-formed arg"}
            }}
        },
        {
            "name": "peek",
            "description": "Read-only view of recent messages or a task thread. Does NOT advance cursor or write receipts. Shows read_by list per message.",
            "inputSchema": {"type":"object","properties":{
                "limit":    {"type":"integer","description":"max messages (default 50, max 1000)"},
                "task_id":  {"type":"string","description":"return full thread for this task_id"},
                "since_id": {"type":"integer","description":"return messages with id > since_id"}
            }}
        },
        {
            "name": "tasks",
            "description": "Task rollup: one row per task_id showing latest state. filter: all|open|mine|for-me",
            "inputSchema": {"type":"object","properties":{
                "filter": {"type":"string","enum":["all","open","mine","for-me"],"description":"default: all"}
            }}
        },
        {
            "name": "peers",
            "description": "List agents in my team (default), a named team, or everyone (team='*'). unread=true adds unread count per peer.",
            "inputSchema": {"type":"object","properties":{
                "team":   {"type":"string","description":"team name or '*' for all"},
                "unread": {"type":"boolean","description":"include unread message count per peer"}
            }}
        },
        {
            "name": "prune",
            "description": "Delete messages older than N days (default 30). Returns count deleted.",
            "inputSchema": {"type":"object","properties":{
                "days": {"type":"integer","description":"messages older than this many days are deleted"}
            }}
        },
        {
            "name": "unregister",
            "description": "Remove a peer from the registry. Defaults to self. Specify team/alias to remove another agent.",
            "inputSchema": {"type":"object","properties":{
                "team":  {"type":"string","description":"team of the peer to remove (default: my team)"},
                "alias": {"type":"string","description":"alias of the peer to remove (default: me)"}
            }}
        }
    ])
}

// ---------------------------------------------------------------- MCP stdio server
fn handle(conn: &Connection, team: &str, alias: &str, req: &Value) -> Option<Value> {
    let method = req["method"].as_str().unwrap_or("");
    let id     = req.get("id").cloned();
    let params = &req["params"];
    match method {
        "initialize" => {
            let proto = params["protocolVersion"].as_str().unwrap_or("2025-06-18");
            Some(json!({"jsonrpc":"2.0","id":id,"result":{
                "protocolVersion": proto,
                "capabilities":    {"tools":{}},
                "serverInfo":      {"name":"agent-bus","version":env!("CARGO_PKG_VERSION")}
            }}))
        }
        "notifications/initialized" | "initialized" => None,
        "ping" => Some(json!({"jsonrpc":"2.0","id":id,"result":{}})),
        "tools/list" => Some(json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools_list()}})),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            let out  = call_tool(conn, team, alias, name, &args);
            let is_err = !out["ok"].as_bool().unwrap_or(true);
            Some(json!({"jsonrpc":"2.0","id":id,"result":{
                "content": [{"type":"text","text":out.to_string()}],
                "isError": is_err
            }}))
        }
        _ => {
            if id.is_some() {
                Some(json!({"jsonrpc":"2.0","id":id,"error":{"code":-32601,"message":format!("method not found: {}",method)}}))
            } else {
                None
            }
        }
    }
}

fn serve() {
    let (team, alias) = (team_env(), alias_env());
    let conn = open_db();
    eprintln!("[agent-bus] serve {}/{} db={}", team, alias, db_path().display());
    let stdin = io::stdin();
    let mut out = io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l)  => l,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() { continue; }
        let req: Value = match serde_json::from_str(line) {
            Ok(v)  => v,
            Err(_) => continue,
        };
        if let Some(resp) = handle(&conn, &team, &alias, &req) {
            let _ = writeln!(out, "{}", resp);
            let _ = out.flush();
        }
    }
}

// ---------------------------------------------------------------- CLI client
fn cli_team(flags: &HashMap<String, String>) -> String {
    flags.get("team").cloned().unwrap_or_else(team_env)
}
fn cli_alias(flags: &HashMap<String, String>) -> String {
    flags
        .get("as")
        .or_else(|| flags.get("alias"))
        .cloned()
        .unwrap_or_else(|| std::env::var("AGENT_BUS_ALIAS").unwrap_or_else(|_| "cli".into()))
}

fn cli_send(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    for (k, j) in [("to","to"),("body","body"),("type","type"),("state","state"),("task-id","task_id")] {
        if let Some(v) = flags.get(k) { args[j] = json!(v); }
    }
    emit("send", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "send", &args), flags);
}
fn cli_poll(flags: &HashMap<String, String>) {
    let conn = open_db();
    emit("poll", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "poll", &json!({})), flags);
}
fn cli_peek(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("limit") {
        if let Ok(n) = v.parse::<i64>() { args["limit"] = json!(n); }
    }
    if let Some(v) = flags.get("task-id")  { args["task_id"]  = json!(v); }
    if let Some(v) = flags.get("since-id") {
        if let Ok(n) = v.parse::<i64>() { args["since_id"] = json!(n); }
    }
    emit("peek", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "peek", &args), flags);
}
fn cli_tasks(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("filter") { args["filter"] = json!(v); }
    emit("tasks", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "tasks", &args), flags);
}
fn cli_peers(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(t) = flags.get("team")  { args["team"]   = json!(t); }
    if flags.get("unread").is_some()     { args["unread"] = json!(true); }
    emit("peers", &call_tool(&conn, &cli_team(flags), "cli", "peers", &args), flags);
}
fn cli_register(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("card") { args["card"] = json!(v); }
    emit("register", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "register", &args), flags);
}

fn print_teams(conn: &Connection, teams: &[(String, i64)]) {
    if teams.is_empty() { println!("○ no teams yet — create one below"); return; }
    println!("● {} team{}\n", teams.len(), plural(teams.len()));
    let w = teams.iter().map(|(t, _)| t.chars().count()).max().unwrap_or(0);
    for (t, n) in teams {
        let members = if *n == 0 { "(empty)".to_string() } else { ellipsis(&list_aliases(conn, t).join(", "), 72) };
        println!("  {:<w$}  {:>2} agent{}   {}", t, n, if *n == 1 { " " } else { "s" }, members, w = w);
    }
    println!();
}

fn valid_name(s: &str) -> bool { !s.is_empty() && !s.contains(char::is_whitespace) && !s.contains('/') }

fn create_team_interactive(conn: &Connection, existing: &[(String, i64)]) {
    let cwd_base = std::env::current_dir()
        .map(|p| basename(&p.to_string_lossy()))
        .unwrap_or_else(|_| "agent".into());
    let (guess_team, guess_alias) = guess_identity(&cwd_base);

    // prefill from the cwd, unless that team already exists
    let team_default = if existing.iter().any(|(t, _)| t == &guess_team) { String::new() } else { guess_team };

    let name = loop {
        let n = ask_text("New team name", &team_default, Some("lowercase, no spaces (e.g. astrub)")).trim().to_string();
        if !valid_name(&n)                       { eprintln!("  team name must be non-empty, no spaces or '/'"); continue; }
        if existing.iter().any(|(t, _)| t == &n) { eprintln!("  team '{}' already exists", n); continue; }
        break n;
    };

    // an empty team is legal — it just has no agents yet
    let add_now = "add its first agent now".to_string();
    let empty    = "create it empty".to_string();
    let with_agent = ask_select(
        "Add an agent?",
        &[add_now.clone(), empty],
        &add_now,
        Some("a team can stay empty and have agents join later"),
    ) == add_now;

    if !with_agent {
        println!("\n  team:  {}  (empty)\n", name);
        if ask_select("Create?", &["Yes".into(), "No — cancel".into()], "Yes", None).starts_with("No") {
            println!("canceled — nothing written.");
            return;
        }
        match ensure_team(conn, &name) {
            Ok(())  => println!("● created empty team '{}' — agents can join with --team {}", name, name),
            Err(e)  => { eprintln!("✗ failed to create team: {}", e); std::process::exit(1); }
        }
        return;
    }

    // "astrub-classic" under team "astrub" -> alias "classic"; else fall back to the folder name
    let alias_default = if guess_identity(&cwd_base).0 == name { guess_alias } else { cwd_base.clone() };

    let alias = loop {
        let a = ask_text("First agent alias", &alias_default, Some("this agent's name inside the team")).trim().to_string();
        if !valid_name(&a) { eprintln!("  alias must be non-empty, no spaces or '/'"); continue; }
        break a;
    };

    let card = ask_text("Capability card (optional)", "", Some("shown in the peers roster"));
    let mut args = json!({});
    if !card.trim().is_empty() { args["card"] = json!(card.trim()); }

    println!("\n  team:   {}\n  agent:  {}/{}\n", name, name, alias);
    if ask_select("Create?", &["Yes".into(), "No — cancel".into()], "Yes", None).starts_with("No") {
        println!("canceled — nothing written.");
        return;
    }

    match tool_register(conn, &name, &alias, &args) {
        Ok(v)  => println!("● created team '{}' with agent {}", name, v["id"].as_str().unwrap_or("?")),
        Err(e) => { eprintln!("✗ failed to create team: {}", e); std::process::exit(1); }
    }
}

fn cli_teams(flags: &HashMap<String, String>) {
    let conn  = open_db();
    let teams = list_teams(&conn);

    if flags.contains_key("json") {
        let t: Vec<Value> = teams.iter().map(|(t, n)| json!({"team":t,"agents":n})).collect();
        println!("{}", json!({"ok":true,"teams":t}));
        return;
    }

    print_teams(&conn, &teams);

    // interactive menu only on a real terminal; piped/scripted use stays a plain listing
    if !is_tty() || flags.contains_key("no-interactive") { return; }

    let create = "+ create a new team".to_string();
    if ask_select("What next?", &[create.clone(), "exit".into()], "exit", None) == create {
        create_team_interactive(&conn, &teams);
    }
}
fn cli_prune(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("days") {
        if let Ok(n) = v.parse::<i64>() { args["days"] = json!(n); }
    }
    emit("prune", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "prune", &args), flags);
}
fn need_pos(cmd: &str, pos: &[String], usage: &str) -> String {
    match pos.first() {
        Some(p) => p.clone(),
        None => { eprintln!("error: `agent-bus {}` needs {}", cmd, usage); std::process::exit(2); }
    }
}

fn cli_install_hook(_flags: &HashMap<String, String>) {
    let path = global_settings_path();
    let existed = path.exists();
    if add_global_hook(&path) {
        println!("● installed global SessionStart hook → {}", path.display());
        println!("  runs `agent-bus doctor --quiet` at every session start — silent unless it finds config drift");
        if existed { println!("  backup: {}", path.with_extension("json.bak").display()); }
    } else {
        println!("• global drift hook already present in {}", path.display());
    }
}

fn cli_sync(flags: &HashMap<String, String>) {
    let conn = open_db();
    let args = json!({"fix": flags.contains_key("fix")});
    emit("sync", &call_tool(&conn, "cli", "cli", "sync", &args), flags);
}

fn cli_create_team(flags: &HashMap<String, String>, pos: &[String]) {
    let name = need_pos("create-team", pos, "a team name");
    let conn = open_db();
    emit("create-team", &call_tool(&conn, "cli", "cli", "create-team", &json!({"name": name})), flags);
}

fn cli_delete_team(flags: &HashMap<String, String>, pos: &[String]) {
    let name = need_pos("delete-team", pos, "a team name");
    let conn = open_db();
    let args = json!({"name": name, "force": flags.contains_key("force")});
    emit("delete-team", &call_tool(&conn, "cli", "cli", "delete-team", &args), flags);
}

fn cli_rename_team(flags: &HashMap<String, String>, pos: &[String]) {
    let from = need_pos("rename-team", pos, "<old> <new>");
    let to   = match pos.get(1) {
        Some(t) => t.clone(),
        None => { eprintln!("error: `agent-bus rename-team` needs <old> <new>"); std::process::exit(2); }
    };
    let conn = open_db();
    emit("rename-team", &call_tool(&conn, "cli", "cli", "rename-team", &json!({"from": from, "to": to})), flags);
}

fn cli_unregister(flags: &HashMap<String, String>, pos: &[String]) {
    let conn = open_db();
    let mut args = json!({});
    // positional target: `alias` or `team/alias`; explicit flags win over it
    if let Some(p) = pos.first() {
        match p.split_once('/') {
            Some((t, a)) => { args["team"] = json!(t); args["alias"] = json!(a); }
            None         => { args["alias"] = json!(p); }
        }
    }
    if let Some(t) = flags.get("team")  { args["team"]  = json!(t); }
    if let Some(a) = flags.get("as").or_else(|| flags.get("alias")) { args["alias"] = json!(a); }
    emit("unregister", &call_tool(&conn, &cli_team(flags), &cli_alias(flags), "unregister", &args), flags);
}
fn cli_whoami(flags: &HashMap<String, String>) {
    let team  = cli_team(flags);
    let alias = cli_alias(flags);
    let team_src  = if std::env::var("AGENT_BUS_TEAM").is_ok() { "env" } else { "default" };
    let alias_src = if std::env::var("AGENT_BUS_ALIAS").is_ok() { "env" }
                    else if flags.contains_key("as") || flags.contains_key("alias") { "flag" }
                    else { "default" };
    let v = json!({
        "ok":       true,
        "identity": format!("{}/{}", team, alias),
        "team":     {"value": team,  "source": team_src},
        "alias":    {"value": alias, "source": alias_src},
        "db":       db_path().display().to_string(),
        "bus_home": bus_home().display().to_string(),
    });
    if flags.contains_key("json") { println!("{}", v); return; }

    println!("● {}/{}\n", team, alias);
    println!("  team      {}  ({})", team, team_src);
    println!("  alias     {}  ({})", alias, alias_src);
    println!("  db        {}", db_path().display());
    println!("  bus home  {}", bus_home().display());
}
fn cli_doctor(flags: &HashMap<String, String>) {
    let cwd = std::env::current_dir().unwrap_or_default();
    // a global SessionStart hook runs `doctor --quiet` in EVERY repo; stay silent and
    // do nothing in projects that aren't agent-bus repos
    if flags.contains_key("quiet") && !is_agentbus_repo(&cwd) { return; }

    let mut checks: Vec<Value> = vec![];
    let mut all_ok = true;

    // 1. bus.db reachable
    let db = db_path();
    if db.exists() {
        checks.push(json!({"check":"bus.db","status":"ok","path":db.display().to_string()}));
    } else {
        checks.push(json!({"check":"bus.db","status":"warn","note":"not yet created — run 'agent-bus register'"}));
    }

    // 2. binary on PATH
    let on_path = std::process::Command::new("which")
        .arg("agent-bus")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if on_path {
        checks.push(json!({"check":"binary_on_path","status":"ok"}));
    } else {
        checks.push(json!({"check":"binary_on_path","status":"fail","note":"not on PATH — run 'cargo install --path .'"}));
        all_ok = false;
    }

    // 3. .mcp.json in CWD
    let mcp = cwd.join(".mcp.json");
    let mcp_has_bus = mcp.exists()
        && fs::read_to_string(&mcp).unwrap_or_default().contains("agent-bus");
    if mcp_has_bus {
        checks.push(json!({"check":".mcp.json","status":"ok","path":mcp.display().to_string()}));
    } else if mcp.exists() {
        checks.push(json!({"check":".mcp.json","status":"warn","note":"exists but agent-bus not configured — run 'agent-bus install'"}));
    } else {
        checks.push(json!({"check":".mcp.json","status":"warn","note":"not found in CWD — run 'agent-bus install'"}));
    }

    // 3b. config drift — the SessionStart hook passes the EXPECTED identity via
    // --team/--as. If the local .mcp.json is missing or pins a different identity,
    // the MCP server silently falls back to a parent-scope config (this is exactly
    // how a session mis-registered as the parent identity after .mcp.json vanished).
    // expected identity: explicit flags win, else recovered from durable per-repo
    // evidence (so a bare `doctor --quiet` from the global hook still detects drift)
    let expected = flags.get("team").cloned().zip(flags.get("as").cloned())
        .or_else(|| discover_identity(&cwd));
    if let Some((et, ea)) = expected.as_ref() {
        let actual = mcp_identity(&mcp);
        let matches = actual.as_ref().map(|(t, a)| t == et && a == ea).unwrap_or(false);
        if matches {
            checks.push(json!({"check":"config","status":"ok","note":format!("{}/{}", et, ea)}));
        } else if flags.contains_key("repair") {
            let exe = std::env::current_exe().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|_| "agent-bus".into());
            write_mcp_config(&cwd.to_string_lossy(), et, ea, &exe);
            if let Ok(canon) = cwd.canonicalize() { add_git_excludes(&canon); }
            checks.push(json!({"check":"config","status":"warn",
                "note":format!("recreated .mcp.json -> {}/{} — restart the session to bind it (current MCP connection still on the old identity)", et, ea)}));
        } else {
            let was = actual.map(|(t, a)| format!("{}/{}", t, a)).unwrap_or_else(|| "missing".into());
            checks.push(json!({"check":"config","status":"warn",
                "note":format!("local .mcp.json is {} but should be {}/{} — MCP falls back to a parent identity. run 'agent-bus doctor --team {} --as {} --repair' then restart", was, et, ea, et, ea)}));
        }
    }

    // 3c. is the wipe-proof global drift hook installed? (info-level, hidden by --quiet)
    if is_agentbus_repo(&cwd) {
        if global_hook_present() {
            checks.push(json!({"check":"global_hook","status":"ok"}));
        } else {
            checks.push(json!({"check":"global_hook","status":"info",
                "note":"no global drift hook — run 'agent-bus install-hook' to catch config drift even if this repo's .claude/ is wiped"}));
        }
    }

    // 4. MCP auto-approved
    let settings = cwd.join(".claude").join("settings.local.json");
    let approved = settings.exists()
        && fs::read_to_string(&settings).unwrap_or_default().contains("agent-bus");
    checks.push(json!({
        "check":  "mcp_auto_approved",
        "status": if approved { "ok" } else { "warn" },
        "note":   if approved { "" } else { "run 'agent-bus install' to auto-approve" }
    }));

    // 4b. agent-bus artifacts must stay local — never pushed to a remote
    let mut leaks: Vec<String> = vec![];
    for art in ["CLAUDE.local.md", ".mcp.json"] {
        if !cwd.join(art).exists() { continue; }
        if is_git_tracked(&cwd, art) {
            leaks.push(format!("{} is tracked by git (will be pushed) — should be local-only", art));
        } else if !is_git_ignored(&cwd, art) {
            leaks.push(format!("{} is neither tracked nor excluded — add it to .git/info/exclude", art));
        }
    }
    // the bootstrap block must not sit in a tracked CLAUDE.md
    let cm = cwd.join("CLAUDE.md");
    if cm.exists() && is_git_tracked(&cwd, "CLAUDE.md")
        && fs::read_to_string(&cm).unwrap_or_default().contains(BLOCK_START) {
        leaks.push("CLAUDE.md is tracked and still holds the agent-bus bootstrap block — run 'agent-bus install' to migrate it".into());
    }
    if leaks.is_empty() {
        checks.push(json!({"check":"artifacts_local","status":"ok"}));
    } else {
        checks.push(json!({"check":"artifacts_local","status":"warn","note":leaks.join("; ")}));
    }

    // 5. peer staleness + open tasks (only if db exists)
    if db.exists() {
        let conn = open_db();
        let stale_threshold = now_ms() - 3_600_000;
        let mut stmt = conn
            .prepare("SELECT team || '/' || alias FROM peers WHERE last_seen < ?1")
            .unwrap();
        let stale: Vec<String> = stmt
            .query_map(params![stale_threshold], |r| r.get::<_, String>(0))
            .unwrap()
            .flatten()
            .collect();
        if stale.is_empty() {
            checks.push(json!({"check":"peers_active","status":"ok"}));
        } else {
            checks.push(json!({"check":"peers_active","status":"warn","stale_1h":stale,"note":"not seen in 1h — may be offline"}));
        }

        drop(stmt);
        // fm.task_id must be qualified — bare `task_id` is ambiguous against agg and
        // the resulting SQL error used to be swallowed, always reporting 0 open
        let open: rusqlite::Result<i64> = conn.query_row(
            "SELECT COUNT(DISTINCT fm.task_id) FROM messages fm \
             INNER JOIN (SELECT task_id, MAX(id) last_id FROM messages GROUP BY task_id) agg ON fm.id=agg.last_id \
             WHERE fm.state NOT IN ('completed','failed','info')",
            [],
            |r| r.get(0),
        );
        match open {
            Ok(n)  => checks.push(json!({"check":"open_tasks","status":"ok","count":n})),
            Err(e) => {
                checks.push(json!({"check":"open_tasks","status":"fail","note":e.to_string()}));
                all_ok = false;
            }
        }

        // 6. is this repo's configured identity still in sync with the bus?
        // env wins (running as the MCP server); else read cwd/.mcp.json
        let identity = std::env::var("AGENT_BUS_TEAM").ok()
            .zip(std::env::var("AGENT_BUS_ALIAS").ok())
            .or_else(|| mcp_identity(&mcp));
        if let Some((t, a)) = identity {
            if !team_row_exists(&conn, &t) {
                checks.push(json!({"check":"identity","status":"warn",
                    "note":format!("{}/{} — team '{}' is not on the bus; a restart recreates it. finish the migration or update this repo's config", t, a, t)}));
            } else if !peer_exists(&conn, &t, &a) {
                checks.push(json!({"check":"identity","status":"warn",
                    "note":format!("{}/{} — configured but not registered yet (registers on next session start)", t, a)}));
            } else {
                checks.push(json!({"check":"identity","status":"ok","note":format!("{}/{}", t, a)}));
            }
        }
    }

    let v = json!({"ok": all_ok, "checks": checks});
    if flags.contains_key("json") { println!("{}", v); return; }

    // --quiet: print only non-ok checks; stay completely silent when healthy
    // (used by the SessionStart hook so a clean start adds no noise)
    let quiet = flags.contains_key("quiet");
    let shown: Vec<&Value> = if quiet {
        // only actionable problems in the session-start context; hide ok + info
        checks.iter().filter(|c| matches!(c["status"].as_str(), Some("warn") | Some("fail"))).collect()
    } else {
        checks.iter().collect()
    };
    if quiet && shown.is_empty() {
        if !all_ok { std::process::exit(1); }
        return;
    }

    let warns = checks.iter().filter(|c| c["status"] == "warn").count();
    let headline = if !all_ok        { "issues found".to_string() }
                   else if warns > 0 { format!("{} warning{}", warns, plural(warns)) }
                   else              { "all checks passed".to_string() };
    if quiet { println!("● agent-bus doctor — {}\n", headline); }
    else     { println!("● doctor — {}\n", headline); }
    let w = shown.iter().filter_map(|c| c["check"].as_str()).map(|s| s.chars().count()).max().unwrap_or(0);
    for c in &shown {
        let sym = match c["status"].as_str().unwrap_or("") {
            "ok"   => "✓",
            "warn" => "!",
            "info" => "·",
            _      => "✗",
        };
        // surface whichever detail this check carries
        let detail = c["note"].as_str().map(String::from)
            .filter(|s| !s.is_empty())
            .or_else(|| c["path"].as_str().map(String::from))
            .or_else(|| c["count"].as_i64().map(|n| format!("{} open", n)))
            .unwrap_or_default();
        let stale = c["stale_1h"].as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
            .unwrap_or_default();

        let mut line = format!("  {} {:<w$}", sym, c["check"].as_str().unwrap_or("?"), w = w);
        if !detail.is_empty() { line.push_str(&format!("  {}", detail)); }
        if !stale.is_empty()  { line.push_str(&format!(": {}", stale)); }
        println!("{}", line.trim_end());
    }
    if !all_ok { std::process::exit(1); }
}

// ---------------------------------------------------------------- installer / wizard
fn prompt(label: &str, default: &str) -> String {
    print!("{} [{}]: ", label, default);
    io::stdout().flush().ok();
    let mut s = String::new();
    io::stdin().read_line(&mut s).ok();
    let s = s.trim();
    if s.is_empty() { default.to_string() } else { s.to_string() }
}
fn choose(label: &str, options: &[&str], default: &str) -> String {
    println!("{}:", label);
    for (i, o) in options.iter().enumerate() {
        println!("  {}. {}{}", i + 1, o, if *o == default { " (default)" } else { "" });
    }
    let pick = prompt("  choose", default);
    if let Ok(n) = pick.parse::<usize>() {
        if n >= 1 && n <= options.len() {
            return options[n - 1].to_string();
        }
    }
    pick
}

fn is_tty() -> bool {
    io::stdin().is_terminal() && io::stdout().is_terminal()
}
// ctrl-c / esc must leave the wizard, not silently accept the default
fn abort_prompt(err: &InquireError) -> ! {
    match err {
        InquireError::OperationInterrupted => eprintln!("\naborted (ctrl-c) — nothing written."),
        _                                  => eprintln!("\ncanceled — nothing written."),
    }
    std::process::exit(130);
}
fn is_abort(e: &InquireError) -> bool {
    matches!(e, InquireError::OperationInterrupted | InquireError::OperationCanceled)
}

fn ask_select(label: &str, options: &[String], default: &str, help: Option<&str>) -> String {
    if is_tty() {
        let start = options.iter().position(|o| o == default).unwrap_or(0);
        let mut s = Select::new(label, options.to_vec()).with_starting_cursor(start);
        if let Some(h) = help { s = s.with_help_message(h); }
        match s.prompt() {
            Ok(v)                    => v,
            Err(e) if is_abort(&e)   => abort_prompt(&e),
            Err(_)                   => default.to_string(),
        }
    } else {
        let refs: Vec<&str> = options.iter().map(|s| s.as_str()).collect();
        choose(label, &refs, default)
    }
}
fn ask_text(label: &str, default: &str, help: Option<&str>) -> String {
    if is_tty() {
        let mut t = Text::new(label).with_default(default);
        if let Some(h) = help { t = t.with_help_message(h); }
        match t.prompt() {
            Ok(s) if !s.trim().is_empty() => s,
            Ok(_)                         => default.to_string(),
            Err(e) if is_abort(&e)        => abort_prompt(&e),
            Err(_)                        => default.to_string(),
        }
    } else {
        prompt(label, default)
    }
}

fn guess_identity(name: &str) -> (String, String) {
    match name.split_once('-') {
        Some((t, a)) if !t.is_empty() && !a.is_empty() => (t.to_string(), a.to_string()),
        _ => ("default".to_string(), name.to_string()),
    }
}
fn basename(path: &str) -> String {
    PathBuf::from(path)
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "agent".into())
}

fn choose_team(conn: &Connection, default: &str) -> String {
    let teams = list_teams(conn);
    if teams.is_empty() {
        return ask_text("Team (logical group)", default, Some("first team — pick a name (e.g. astrub)"));
    }
    let counts = teams.iter().map(|(t, n)| format!("{}:{}", t, n)).collect::<Vec<_>>().join("  ");
    let create = "+ create a new team".to_string();
    let mut opts: Vec<String> = teams.iter().map(|(t, _)| t.clone()).collect();
    opts.push(create.clone());
    let start = if teams.iter().any(|(t, _)| t == default) { default } else { &create };
    let chosen = ask_select("Team", &opts, start, Some(&format!("existing: {}", counts)));
    if chosen == create {
        ask_text("New team name", default, None)
    } else {
        chosen
    }
}
fn choose_alias(conn: &Connection, team: &str, default: &str) -> String {
    let al = list_aliases(conn, team);
    let help = if al.is_empty() {
        format!("first agent in '{}'", team)
    } else {
        format!("already in '{}': {}", team, al.join(", "))
    };
    let a = ask_text("Alias (this agent's name)", default, Some(&help));
    if al.iter().any(|x| x == &a) {
        println!("note: {}/{} already exists — installing reuses that identity", team, a);
    }
    a
}

fn wizard() {
    println!("◆ agent-bus setup — {}\n", version_string());
    let tool = ask_select(
        "Which tool is this for?",
        &["claude".into(), "codex".into(), "copilot".into()],
        "claude",
        Some("the agent CLI that will run this bus"),
    );
    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| ".".into());
    let repo = if tool == "claude" {
        ask_text("Target repo path", &cwd, Some("where to write .mcp.json + CLAUDE.md"))
    } else {
        cwd.clone()
    };
    let (gt, ga) = guess_identity(&basename(&repo));
    let conn = open_db();
    let team  = choose_team(&conn, &gt);
    let alias = choose_alias(&conn, &team, &ga);
    let card_raw = ask_text(
        "Capability card (optional)",
        "",
        Some("describe what this agent does — shown in 'peers' roster"),
    );
    let card: Option<String> = if card_raw.trim().is_empty() { None } else { Some(card_raw) };

    // confirm screen
    println!("\n  tool:  {}", tool);
    println!("  repo:  {}", repo);
    println!("  id:    {}/{}", team, alias);
    if let Some(ref c) = card { println!("  card:  {}", c); }
    println!();
    let confirm = ask_select(
        "Apply?",
        &["Yes — write config files".into(), "No — cancel".into()],
        "Yes — write config files",
        None,
    );
    if confirm.starts_with("No") {
        println!("aborted — no files written.");
        return;
    }

    apply_install(&tool, &team, &alias, &repo, card.as_deref());
}

fn install(flags: &HashMap<String, String>) {
    if flags.is_empty() { return wizard(); }
    let cwd = std::env::current_dir()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| ".".into());
    let tool = flags.get("tool").cloned().unwrap_or_else(|| {
        ask_select("Which tool is this for?", &["claude".into(), "codex".into(), "copilot".into()], "claude", None)
    });
    let repo = flags.get("repo").cloned().unwrap_or_else(|| {
        if tool == "claude" { ask_text("Target repo path", &cwd, None) } else { cwd.clone() }
    });
    let (gt, ga) = guess_identity(&basename(&repo));
    let conn  = open_db();
    let team  = flags.get("team").cloned().unwrap_or_else(|| choose_team(&conn, &gt));
    let alias = flags.get("alias").cloned().unwrap_or_else(|| choose_alias(&conn, &team, &ga));
    let card  = flags.get("card").map(|s| s.as_str());
    apply_install(&tool, &team, &alias, &repo, card);
}

// .git is a directory in a normal clone, but a file in a worktree or submodule
fn find_git_root(start: &Path) -> Option<PathBuf> {
    let mut p = start.canonicalize().ok()?;
    loop {
        if p.join(".git").exists() { return Some(p); }
        if !p.pop() { return None; }
    }
}

// config may live in any directory inside a worktree, not just the repo root —
// nothing install writes depends on .git being a sibling
fn validate_install_target(repo: &str) {
    let canon = PathBuf::from(repo).canonicalize().unwrap_or_else(|_| {
        eprintln!("error: {} does not exist", repo);
        std::process::exit(1);
    });

    if let Ok(home) = std::env::var("HOME") {
        if let Ok(home_canon) = PathBuf::from(home).canonicalize() {
            if canon == home_canon {
                eprintln!("error: refusing to write config files into your home directory");
                eprintln!("       target a specific project repo instead");
                std::process::exit(1);
            }
        }
    }

    match find_git_root(&canon) {
        None => {
            eprintln!("error: {} is not inside a git repository", canon.display());
            eprintln!("       (.git not found here or in any parent directory)");
            std::process::exit(1);
        }
        Some(root) if root != canon => {
            println!("note: installing into a subdirectory of the repo at {}", root.display());
            println!("      launch Claude Code from {} for this identity to apply", canon.display());
        }
        Some(_) => {}
    }
}

const EXCLUDE_MARK: &str = "# agent-bus (local-only, not committed)";

// resolves through worktrees/submodules, where .git is a file rather than a directory
fn git_exclude_path(root: &Path) -> Option<PathBuf> {
    let out = std::process::Command::new("git")
        .arg("-C").arg(root)
        .args(["rev-parse", "--git-path", "info/exclude"])
        .output()
        .ok()?;
    if !out.status.success() { return None; }
    let raw = String::from_utf8(out.stdout).ok()?;
    let p = PathBuf::from(raw.trim());
    Some(if p.is_absolute() { p } else { root.join(p) })
}

fn is_git_tracked(dir: &Path, file: &str) -> bool {
    std::process::Command::new("git")
        .arg("-C").arg(dir)
        .args(["ls-files", "--error-unmatch", file])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// true if git would ignore this path — honours .gitignore AND .git/info/exclude
fn is_git_ignored(dir: &Path, file: &str) -> bool {
    std::process::Command::new("git")
        .arg("-C").arg(dir)
        .args(["check-ignore", "-q", file])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// keep agent config out of the enclosing repo without touching the tracked .gitignore
fn add_git_excludes(target: &Path) {
    let Some(root) = find_git_root(target) else { return };
    let Some(excl) = git_exclude_path(&root) else { return };

    // patterns are anchored to the repo root, so a subfolder install needs its prefix
    let prefix = target.strip_prefix(&root)
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_default();
    let pfx = if prefix.is_empty() { String::new() } else { format!("{}/", prefix) };
    // CLAUDE.md is the user's own file — only our generated artifacts get excluded
    let entries = [
        format!("/{}.mcp.json", pfx),
        format!("/{}CLAUDE.local.md", pfx),
        format!("/{}.claude/", pfx),
    ];

    let existing = fs::read_to_string(&excl).unwrap_or_default();

    // older versions excluded CLAUDE.md; it is the user's file now, so drop that
    // line if we are the ones who added it (i.e. it sits below our marker)
    let legacy = format!("/{}CLAUDE.md", pfx);
    let mut past_mark = false;
    let mut pruned_any = false;
    let pruned: String = existing
        .lines()
        .filter(|l| {
            if l.trim() == EXCLUDE_MARK { past_mark = true; return true; }
            if past_mark && l.trim() == legacy { pruned_any = true; return false; }
            true
        })
        .map(|l| format!("{}\n", l))
        .collect();

    let missing: Vec<&String> = entries
        .iter()
        .filter(|e| !pruned.lines().any(|l| l.trim() == e.as_str()))
        .collect();
    if missing.is_empty() && !pruned_any { return; }

    if let Some(parent) = excl.parent() { let _ = fs::create_dir_all(parent); }
    let mut out = pruned;
    if !out.is_empty() && !out.ends_with('\n') { out.push('\n'); }
    if !out.contains(EXCLUDE_MARK) { out.push_str(&format!("\n{}\n", EXCLUDE_MARK)); }
    for e in &missing { out.push_str(e); out.push('\n'); }

    match fs::write(&excl, out) {
        Ok(()) => {
            println!("excluded agent config via {}", excl.display());
            if pruned_any { println!("      dropped the stale {} exclude — that file is yours", legacy); }
        }
        Err(e) => eprintln!("warning: could not write {}: {}", excl.display(), e),
    }
}

fn apply_install(tool: &str, team: &str, alias: &str, repo: &str, card: Option<&str>) {
    validate_install_target(repo);
    let exe = std::env::current_exe()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "agent-bus".into());
    match tool {
        "claude"  => install_claude(repo, team, alias, &exe),
        "codex"   => install_codex(team, alias, &exe),
        "copilot" => print_copilot(team, alias, &exe),
        other => {
            eprintln!("unknown tool: {} (use claude|codex|copilot)", other);
            return;
        }
    }
    let card_val = card.map(|s| s.to_string())
        .unwrap_or_else(|| format!("installed for {}", tool));
    let conn = open_db();
    let _ = tool_register(&conn, team, alias, &json!({"card": card_val}));
    println!("registered {}/{} on the bus", team, alias);
}

const BLOCK_START: &str = "<!-- agent-bus-bootstrap -->";
const BLOCK_END:   &str = "<!-- /agent-bus-bootstrap -->";

fn has_block(s: &str) -> bool { s.contains(BLOCK_START) && s.contains(BLOCK_END) }

// returns the text with our block removed; used to migrate a legacy CLAUDE.md
fn strip_block(existing: &str) -> String {
    match (existing.find(BLOCK_START), existing.find(BLOCK_END)) {
        (Some(s), Some(e)) if e > s => {
            let mut out = String::new();
            out.push_str(&existing[..s]);
            out.push_str(&existing[e + BLOCK_END.len()..]);
            out
        }
        _ => existing.to_string(),
    }
}

fn upsert_block(existing: &str, block: &str) -> String {
    let start = BLOCK_START;
    let end   = BLOCK_END;
    if let (Some(s), Some(e)) = (existing.find(start), existing.find(end)) {
        let mut out = String::new();
        out.push_str(&existing[..s]);
        out.push_str(block.trim_end());
        out.push_str(&existing[e + end.len()..]);
        out
    } else {
        format!("{}\n{}\n", existing.trim_end(), block)
    }
}

fn wire_session_hook(repo: &str, team: &str, alias: &str) {
    let dir  = PathBuf::from(repo).join(".claude");
    fs::create_dir_all(&dir).ok();
    let path = dir.join("settings.local.json");
    let mut root: Value = if path.exists() {
        serde_json::from_str(&fs::read_to_string(&path).unwrap_or_default()).unwrap_or(json!({}))
    } else {
        json!({})
    };
    if !root.is_object() { root = json!({}); }

    // doctor --quiet first: it surfaces config/identity drift (e.g. a missing
    // .mcp.json making the session fall back to a parent identity) in the session's
    // startup context, before poll runs. --team/--as pin the *expected* identity.
    let hook_cmd = format!(
        "agent-bus doctor --team {t} --as {a} --quiet; agent-bus poll --team {t} --as {a}",
        t = team, a = alias
    );

    // check if our hook already exists
    let already = root["hooks"]["SessionStart"]
        .as_array()
        .map(|arr| arr.iter().any(|entry| {
            entry["hooks"].as_array().map(|h| h.iter().any(|hk| {
                hk["command"].as_str() == Some(&hook_cmd)
            })).unwrap_or(false)
        }))
        .unwrap_or(false);

    if already {
        println!("• SessionStart hook already present in {}", path.display());
        return;
    }

    if !root["hooks"].is_object() { root["hooks"] = json!({}); }
    let mut starts = root["hooks"]["SessionStart"].as_array().cloned().unwrap_or_default();
    starts.push(json!({"hooks": [{"type": "command", "command": hook_cmd}]}));
    root["hooks"]["SessionStart"] = json!(starts);

    fs::write(&path, serde_json::to_string_pretty(&root).unwrap() + "\n").ok();
    println!("wired SessionStart hook in {}", path.display());
}

// older installs put the block in CLAUDE.md, which for a tracked file leaks
// machine-specific paths into git history. pull it back out.
fn migrate_legacy_block(repo: &Path) {
    let cm = repo.join("CLAUDE.md");
    let Ok(existing) = fs::read_to_string(&cm) else { return };
    if !has_block(&existing) { return; }

    let stripped = strip_block(&existing);
    if stripped.trim().is_empty() {
        // agent-bus was the file's only content — it created it, so remove it
        match fs::remove_file(&cm) {
            Ok(())  => println!("removed {} (contained only the agent-bus block)", cm.display()),
            Err(e)  => eprintln!("warning: could not remove {}: {}", cm.display(), e),
        }
        return;
    }

    let cleaned = format!("{}\n", stripped.trim_end());
    match fs::write(&cm, cleaned) {
        Ok(()) => {
            println!("migrated the agent-bus block out of {}", cm.display());
            if is_git_tracked(repo, "CLAUDE.md") {
                println!("      CLAUDE.md is tracked — check `git diff` and commit the removal");
            }
        }
        Err(e) => eprintln!("warning: could not rewrite {}: {}", cm.display(), e),
    }
}

// write/refresh the agent-bus entry in a repo's .mcp.json, preserving other servers
fn write_mcp_config(repo: &str, team: &str, alias: &str, exe: &str) -> PathBuf {
    let mcp_path = PathBuf::from(repo).join(".mcp.json");
    let mut root: Value = if mcp_path.exists() {
        serde_json::from_str(&fs::read_to_string(&mcp_path).unwrap_or_default()).unwrap_or(json!({}))
    } else {
        json!({})
    };
    if !root.is_object()               { root = json!({}); }
    if !root["mcpServers"].is_object() { root["mcpServers"] = json!({}); }
    root["mcpServers"]["agent-bus"] = json!({
        "command": exe, "args": ["serve"],
        "env": {"AGENT_BUS_TEAM": team, "AGENT_BUS_ALIAS": alias}
    });
    fs::write(&mcp_path, serde_json::to_string_pretty(&root).unwrap() + "\n").expect("write .mcp.json");
    mcp_path
}

fn install_claude(repo: &str, team: &str, alias: &str, exe: &str) {
    let mcp_path = write_mcp_config(repo, team, alias, exe);
    println!("wrote {}", mcp_path.display());

    // the bootstrap hardcodes machine-specific inbox paths, so it belongs in
    // CLAUDE.local.md — loaded right after CLAUDE.md, and never committed
    let local = PathBuf::from(repo).join("CLAUDE.local.md");
    let existing = fs::read_to_string(&local).unwrap_or_default();
    fs::write(&local, upsert_block(&existing, &bootstrap_block(team, alias)))
        .expect("write CLAUDE.local.md");
    println!("wrote agent-bus bootstrap into {}", local.display());

    migrate_legacy_block(Path::new(repo));

    enable_mcp_setting(repo);
    wire_session_hook(repo, team, alias);
    if std::env::var("AGENT_BUS_NO_GIT_EXCLUDE").is_err() {
        if let Ok(canon) = PathBuf::from(repo).canonicalize() { add_git_excludes(&canon); }
    }
    println!("restart the Claude Code session in {} (no approval prompt — auto-enabled)", repo);
}

fn enable_mcp_setting(repo: &str) {
    let dir  = PathBuf::from(repo).join(".claude");
    fs::create_dir_all(&dir).ok();
    let path = dir.join("settings.local.json");
    let mut root: Value = if path.exists() {
        serde_json::from_str(&fs::read_to_string(&path).unwrap_or_default()).unwrap_or(json!({}))
    } else {
        json!({})
    };
    if !root.is_object() { root = json!({}); }
    let mut names = root["enabledMcpjsonServers"].as_array().cloned().unwrap_or_default();
    if !names.iter().any(|v| v == "agent-bus") {
        names.push(json!("agent-bus"));
    }
    root["enabledMcpjsonServers"] = json!(names);
    fs::write(&path, serde_json::to_string_pretty(&root).unwrap() + "\n").ok();
    println!("auto-approved agent-bus in {}", path.display());
}

fn install_codex(team: &str, alias: &str, exe: &str) {
    let path = home_dir().join(".codex").join("config.toml");
    fs::create_dir_all(path.parent().unwrap()).ok();
    let existing = fs::read_to_string(&path).unwrap_or_default();
    if existing.contains("[mcp_servers.agent-bus]") {
        println!("• {} already has [mcp_servers.agent-bus] (edit by hand to change team/alias)", path.display());
        return;
    }
    let block = format!(
        "\n[mcp_servers.agent-bus]\ncommand = \"{}\"\nargs = [\"serve\"]\nenv = {{ AGENT_BUS_TEAM = \"{}\", AGENT_BUS_ALIAS = \"{}\" }}\n",
        exe, team, alias
    );
    let mut f = fs::OpenOptions::new().create(true).append(true).open(&path).expect("open config.toml");
    write!(f, "{}", block).expect("append config.toml");
    println!("added [mcp_servers.agent-bus] to {}", path.display());
    println!("restart Codex to load it");
}

fn print_copilot(team: &str, alias: &str, exe: &str) {
    println!("Add this MCP server to Copilot's config:");
    println!("  command: {}", exe);
    println!("  args:    [\"serve\"]");
    println!("  env:     AGENT_BUS_TEAM={}  AGENT_BUS_ALIAS={}", team, alias);
    println!("Then call poll() at the start of each turn (Copilot has no background watcher).");
}

fn bootstrap_block(team: &str, alias: &str) -> String {
    let fp = flag_path(team, alias);
    format!(
        "<!-- agent-bus-bootstrap -->\n\
## agent-bus (cross-session coordination)\n\
This repo has the `agent-bus` MCP server, identity `{team}/{alias}`. At session start:\n\
1. Call `mcp__agent-bus__register` with a short capability card.\n\
2. Call `mcp__agent-bus__poll` immediately to drain any backlog accumulated during the restart gap.\n\
3. Arm a doorbell so inbound mail wakes you:\n\
```\n\
Monitor(persistent:true, timeout_ms:3600000, command: |\n\
  f={flag}; last=\"\"; while true; do\n\
    if [ -f \"$f\" ]; then\n\
      m=$( (stat -f %m \"$f\" || stat -c %Y \"$f\") 2>/dev/null);\n\
      if [ \"$m\" != \"$last\" ]; then echo \"BUS: new mail for {team}/{alias} — call agent-bus poll()\"; last=\"$m\"; fi;\n\
    fi; sleep 2; done)\n\
```\n\
4. On a `BUS:` ping, call `mcp__agent-bus__poll` and act on the messages.\n\
5. Reply with `mcp__agent-bus__send` — to=\"alias\" (same team), \"team/alias\" (cross-team), or \"team:{team}\" (broadcast).\n\
6. `mcp__agent-bus__peers` lists your team roster (team:\"*\" = everyone).\n\
7. `mcp__agent-bus__peek` for read-only history; `mcp__agent-bus__tasks` for task rollup.\n\
8. Agents join at any time, so your roster is always stale. Before concluding a peer\n\
   does not exist, re-call `peers` with team:\"*\" — never answer from an earlier roster.\n\
   If the user names a peer you have not seen, re-check first, then send anyway: mail to a\n\
   not-yet-registered identity is delivered once that agent registers under it. A `send`\n\
   reply carrying `recipient_registered: false` means the alias may be a typo — compare it\n\
   against `known_peers` in the same reply before retrying.\n\
<!-- /agent-bus-bootstrap -->",
        team = team, alias = alias, flag = fp.display()
    )
}

// ---------------------------------------------------------------- arg parsing
// positionals used to be silently discarded, so `unregister foo --team t` dropped
// `foo` and fell back to the caller's own alias — deleting the wrong peer
// valueless flags must not swallow the next token, or `--unread foo` eats `foo`
const BOOL_FLAGS: &[&str] = &["json", "help", "h", "unread", "no-interactive", "force", "fix", "quiet", "repair"];

fn parse_args(args: &[String]) -> (HashMap<String, String>, Vec<String>) {
    let mut m = HashMap::new();
    let mut pos = Vec::new();
    let mut i = 0;
    while i < args.len() {
        if let Some(key) = args[i].strip_prefix("--") {
            let takes_value = !BOOL_FLAGS.contains(&key)
                && i + 1 < args.len()
                && !args[i + 1].starts_with("--");
            let val = if takes_value { i += 1; args[i].clone() } else { "true".into() };
            m.insert(key.to_string(), val);
        } else {
            pos.push(args[i].clone());
        }
        i += 1;
    }
    (m, pos)
}

#[cfg(test)]
fn parse_flags(args: &[String]) -> HashMap<String, String> { parse_args(args).0 }

// reject typo'd flags and stray arguments rather than acting on a wrong target
fn guard(cmd: &str, flags: &HashMap<String, String>, pos: &[String], allowed: &[&str], max_pos: usize) {
    for k in flags.keys() {
        if k == "json" { continue; }
        if !allowed.contains(&k.as_str()) {
            eprintln!("error: unknown flag `--{}` for `agent-bus {}`", k, cmd);
            eprintln!("       try `agent-bus {} --help`", cmd);
            std::process::exit(2);
        }
    }
    if pos.len() > max_pos {
        eprintln!("error: unexpected argument `{}` for `agent-bus {}`", pos[max_pos], cmd);
        eprintln!("       try `agent-bus {} --help`", cmd);
        std::process::exit(2);
    }
}

// (allowed flags, max positional args) per command
fn spec(cmd: &str) -> (&'static [&'static str], usize) {
    match cmd {
        "serve"                => (&[], 0),
        "install"              => (&["tool", "team", "alias", "repo", "card"], 0),
        "send"                 => (&["to", "body", "type", "state", "task-id", "team", "as"], 0),
        "poll"                 => (&["team", "as"], 0),
        "peek" | "history"     => (&["limit", "task-id", "since-id", "team", "as"], 0),
        "tasks"                => (&["filter", "team", "as"], 0),
        "prune"                => (&["days", "team", "as"], 0),
        "peers" | "roster"     => (&["team", "unread"], 0),
        "teams"                => (&["no-interactive"], 0),
        "create-team"          => (&[], 1),          // positional: name
        "delete-team"          => (&["force"], 1),   // positional: name
        "rename-team"          => (&[], 2),          // positionals: old new
        "register"             => (&["card", "team", "as"], 0),
        "unregister"           => (&["team", "as", "alias"], 1), // positional: alias or team/alias
        "whoami"               => (&["team", "as"], 0),
        "doctor"               => (&["team", "as", "quiet", "repair"], 0),
        "install-hook"         => (&[], 0),
        "sync"                 => (&["fix"], 0),
        _                      => (&[], 0),
    }
}

fn usage() -> &'static str {
    "agent-bus <command>\n\
     \n\
     setup / (no args)             interactive first-time setup\n\
     serve                         MCP stdio server\n\
     install [--tool --team --alias --repo [--card ..]]\n\
     send --to X --body Y [--type task] [--state s] [--task-id id] [--team t] [--as a]\n\
     poll [--team t] [--as a]\n\
     peek [--limit N] [--task-id id] [--since-id N]   read-only; no cursor advance\n\
     tasks [--filter all|open|mine|for-me]\n\
     prune [--days N]\n\
     peers [--team t|*] [--unread]\n\
     roster                        alias for peers (my team)\n\
     teams [--no-interactive]      list teams; on a tty, offers to create one\n\
     create-team <name>            create an empty team\n\
     rename-team <old> <new>       rename a team (cascades across the bus)\n\
     delete-team <name> [--force]  delete a team; --force also removes its agents\n\
     register [--card ..] [--team t] [--as a]\n\
     unregister [alias | team/alias]    defaults to self if no target given\n\
     whoami\n\
     doctor [--team t --as a] [--quiet] [--repair]   per-agent health + config drift;\n\
     \x20                            --repair recreates a missing/mismatched .mcp.json\n\
     install-hook                  add a global SessionStart drift-check to ~/.claude/settings.json\n\
     sync [--fix]                  find bus-wide orphans (dead recipients/cursors); --fix reconciles\n\
     version\n\
     \n\
     --help    show this message           --json    print the raw JSON object\n\
     \n\
     install writes the bootstrap to CLAUDE.local.md and adds\n\
     .mcp.json/CLAUDE.local.md/.claude to the repo's .git/info/exclude;\n\
     set AGENT_BUS_NO_GIT_EXCLUDE=1 to skip that"
}

fn main() {
    // rust ignores SIGPIPE, so `agent-bus peek | head` would panic on EPIPE mid-print.
    // restore the default handler so we exit quietly like any other unix tool.
    #[cfg(unix)]
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL); }

    let argv: Vec<String> = std::env::args().collect();
    let cmd = argv.get(1).map(|s| s.as_str()).unwrap_or("");
    let (flags, pos) = parse_args(&argv[2.min(argv.len())..]);

    // --help anywhere wins, before any command runs or any prompt opens
    if matches!(cmd, "help" | "--help" | "-h") || flags.contains_key("help") || flags.contains_key("h") {
        println!("{}", usage());
        return;
    }

    // reject an unknown command before validating its flags, or the error misleads
    const COMMANDS: &[&str] = &[
        "", "setup", "serve", "install", "send", "poll", "peek", "history", "tasks",
        "prune", "peers", "roster", "teams", "create-team", "delete-team",
        "rename-team", "register", "unregister", "whoami",
        "doctor", "install-hook", "sync", "version", "--version", "-V",
    ];
    if !COMMANDS.contains(&cmd) {
        eprintln!("error: unknown command `{}`\n", cmd);
        eprintln!("{}", usage());
        std::process::exit(2);
    }

    if !matches!(cmd, "" | "setup" | "version" | "--version" | "-V") {
        let (allowed, max_pos) = spec(cmd);
        guard(cmd, &flags, &pos, allowed, max_pos);
    }

    match cmd {
        "serve"               => serve(),
        "install"             => install(&flags),
        "send"                => cli_send(&flags),
        "poll"                => cli_poll(&flags),
        "peek" | "history"    => cli_peek(&flags),
        "tasks"               => cli_tasks(&flags),
        "prune"               => cli_prune(&flags),
        "peers"               => cli_peers(&flags),
        "roster"              => cli_peers(&flags),  // alias: peers scoped to my team (default)
        "teams"               => cli_teams(&flags),
        "create-team"         => cli_create_team(&flags, &pos),
        "delete-team"         => cli_delete_team(&flags, &pos),
        "rename-team"         => cli_rename_team(&flags, &pos),
        "register"            => cli_register(&flags),
        "unregister"          => cli_unregister(&flags, &pos),
        "whoami"              => cli_whoami(&flags),
        "doctor"              => cli_doctor(&flags),
        "install-hook"        => cli_install_hook(&flags),
        "sync"                => cli_sync(&flags),
        "version" | "--version" | "-V" => println!("{}", version_string()),
        "" | "setup"          => wizard(),
        other => {
            eprintln!("error: unknown command `{}`\n", other);
            eprintln!("{}", usage());
            std::process::exit(2);
        }
    }
}

// ---------------------------------------------------------------- tests
#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Connection {
        use std::sync::Once;
        static O: Once = Once::new();
        O.call_once(|| {
            let d = std::env::temp_dir().join(format!("agent-bus-test-{}", std::process::id()));
            std::env::set_var("AGENT_BUS_HOME", d);
        });
        let c = Connection::open_in_memory().unwrap();
        init_schema(&c);
        c
    }

    #[test]
    fn same_team_task_roundtrip() {
        let c = mem();
        let r = tool_send(&c, "astrub", "sync", &json!({"to":"classic","type":"task","body":"execute Phase 7"})).unwrap();
        assert!(r["ok"].as_bool().unwrap());
        assert_eq!(r["to"].as_str().unwrap(), "astrub/classic");
        let task_id = r["task_id"].as_str().unwrap().to_string();

        let r = tool_poll(&c, "astrub", "classic").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["task_id"].as_str().unwrap(), task_id);
        assert_eq!(r["messages"][0]["from"].as_str().unwrap(), "astrub/sync");

        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 0);

        tool_send(&c, "astrub", "classic", &json!({
            "to":"sync","type":"status","state":"completed","task_id":task_id,"body":"255 green"
        })).unwrap();
        let r = tool_poll(&c, "astrub", "sync").unwrap();
        assert_eq!(r["messages"][0]["state"].as_str().unwrap(), "completed");
    }

    #[test]
    fn team_isolation_and_cross_team() {
        let c = mem();
        tool_send(&c, "astrub", "sync", &json!({"to":"all","body":"team note"})).unwrap();
        assert_eq!(tool_poll(&c, "webapp", "api").unwrap()["count"], 0);
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 1);

        tool_send(&c, "astrub", "sync", &json!({"to":"webapp/api","body":"hi other team"})).unwrap();
        let r = tool_poll(&c, "webapp", "api").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["from"].as_str().unwrap(), "astrub/sync");
    }

    #[test]
    fn team_broadcast_and_global() {
        let c = mem();
        tool_send(&c, "ops", "boss", &json!({"to":"team:astrub","body":"hello astrub"})).unwrap();
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 1);
        assert_eq!(tool_poll(&c, "ops", "boss").unwrap()["count"], 0);

        tool_send(&c, "ops", "boss", &json!({"to":"*","body":"global"})).unwrap();
        let r = tool_poll(&c, "astrub", "client").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 2);
        assert!(r["messages"].as_array().unwrap().iter().any(|m| m["body"] == "global"));
        let r = tool_poll(&c, "webapp", "api").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["body"].as_str().unwrap(), "global");
    }

    #[test]
    fn broadcast_self_echo_excluded() {
        let c = mem();
        tool_send(&c, "astrub", "sync", &json!({"to":"all","body":"team note"})).unwrap();
        // sender must NOT see its own team broadcast
        assert_eq!(tool_poll(&c, "astrub", "sync").unwrap()["count"], 0);
        // another team member does see it
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 1);

        // global broadcast: sender excluded, others included
        tool_send(&c, "ops", "boss", &json!({"to":"*","body":"global"})).unwrap();
        assert_eq!(tool_poll(&c, "ops", "boss").unwrap()["count"], 0);
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 1);
    }

    #[test]
    fn peek_is_readonly() {
        let c = mem();
        let tid = "t-peek-test";
        tool_send(&c, "astrub", "sync", &json!({"to":"classic","type":"task","body":"do work","task_id":tid})).unwrap();
        tool_send(&c, "astrub", "classic", &json!({"to":"sync","type":"status","state":"completed","task_id":tid,"body":"done"})).unwrap();

        // peek at task thread (read-only)
        let r = tool_peek(&c, &json!({"task_id": tid})).unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 2);

        // cursor untouched — poll still sees the messages
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 1);
        assert_eq!(tool_poll(&c, "astrub", "sync").unwrap()["count"], 1);

        // after poll, peek shows read_by
        let r = tool_peek(&c, &json!({"task_id": tid})).unwrap();
        let msgs = r["messages"].as_array().unwrap();
        // first message (to classic) should have classic as reader
        assert!(msgs[0].get("read_by").is_some());
    }

    #[test]
    fn tasks_rollup() {
        let c = mem();
        let tid = "p8";
        tool_send(&c, "astrub", "sync", &json!({"to":"classic","type":"task","body":"phase 8","task_id":tid})).unwrap();
        tool_send(&c, "astrub", "classic", &json!({"to":"sync","type":"status","state":"working","task_id":tid,"body":"on it"})).unwrap();

        let r = tool_tasks(&c, "astrub", "sync", &json!({"filter":"open"})).unwrap();
        let tasks = r["tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["task_id"].as_str().unwrap(), tid);
        assert_eq!(tasks[0]["state"].as_str().unwrap(), "working");

        tool_send(&c, "astrub", "classic", &json!({"to":"sync","type":"status","state":"completed","task_id":tid,"body":"done"})).unwrap();
        assert_eq!(tool_tasks(&c, "astrub", "sync", &json!({"filter":"open"})).unwrap()["count"], 0);
        assert_eq!(tool_tasks(&c, "astrub", "sync", &json!({"filter":"all"})).unwrap()["count"], 1);

        // mine: sync sent the original task
        let r = tool_tasks(&c, "astrub", "sync", &json!({"filter":"mine"})).unwrap();
        assert_eq!(r["count"], 1);
        // for-me: classic received it
        let r = tool_tasks(&c, "astrub", "classic", &json!({"filter":"for-me"})).unwrap();
        assert_eq!(r["count"], 1);
    }

    #[test]
    fn prune_removes_old_messages() {
        let c = mem();
        // insert a message with old created_at directly
        c.execute(
            "INSERT INTO messages(task_id, sender_team, sender_alias, recipient_team, recipient_alias, \
             type, state, body, created_at) VALUES('old','a','b','a','c','message','info','old msg', ?1)",
            params![now_ms() - 40 * 86_400_000i64],
        ).unwrap();
        tool_send(&c, "a", "b", &json!({"to":"c","body":"new"})).unwrap();

        let r = tool_prune(&c, &json!({"days": 30})).unwrap();
        assert_eq!(r["deleted"].as_i64().unwrap(), 1);
        // new message survives
        let r = tool_peek(&c, &json!({"limit": 10})).unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["body"].as_str().unwrap(), "new");
    }

    #[test]
    fn peers_roster_scoping() {
        let c = mem();
        tool_register(&c, "astrub", "sync",    &json!({"card":"planning"})).unwrap();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        tool_register(&c, "webapp", "api",     &json!({})).unwrap();

        let mine = tool_peers(&c, "astrub", &json!({})).unwrap();
        let names: Vec<&str> = mine["peers"].as_array().unwrap().iter()
            .map(|p| p["alias"].as_str().unwrap()).collect();
        assert!(names.contains(&"sync") && names.contains(&"classic") && !names.contains(&"api"));

        let all = tool_peers(&c, "astrub", &json!({"team":"*"})).unwrap();
        assert_eq!(all["peers"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn peers_unread_count() {
        let c = mem();
        tool_register(&c, "astrub", "sync",    &json!({})).unwrap();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();

        tool_send(&c, "astrub", "sync", &json!({"to":"classic","body":"unread msg"})).unwrap();

        let r = tool_peers(&c, "astrub", &json!({"unread": true})).unwrap();
        let classic = r["peers"].as_array().unwrap().iter()
            .find(|p| p["alias"] == "classic").unwrap();
        assert_eq!(classic["unread"].as_i64().unwrap(), 1);

        // after classic polls, unread drops to 0
        tool_poll(&c, "astrub", "classic").unwrap();
        let r = tool_peers(&c, "astrub", &json!({"unread": true})).unwrap();
        let classic = r["peers"].as_array().unwrap().iter()
            .find(|p| p["alias"] == "classic").unwrap();
        assert_eq!(classic["unread"].as_i64().unwrap(), 0);
    }

    fn argv(s: &[&str]) -> Vec<String> { s.iter().map(|x| x.to_string()).collect() }

    #[test]
    fn parse_args_keeps_positionals() {
        let (f, p) = parse_args(&argv(&["infra-repo", "--team", "home-infra"]));
        assert_eq!(p, vec!["infra-repo"]);
        assert_eq!(f.get("team").map(String::as_str), Some("home-infra"));

        // flag-before-positional, and valueless flags
        let (f, p) = parse_args(&argv(&["--team", "t", "--unread", "extra"]));
        assert_eq!(p, vec!["extra"]);
        assert_eq!(f.get("unread").map(String::as_str), Some("true"));
    }

    #[test]
    fn unregister_targets_the_named_peer_not_self() {
        // `unregister infra-repo --team home-infra` must delete home-infra/infra-repo,
        // never the caller's own identity (the positional used to be discarded)
        let c = mem();
        tool_register(&c, "home-infra", "infra-repo", &json!({})).unwrap();
        tool_register(&c, "home-infra", "me", &json!({})).unwrap();

        let (_, pos) = parse_args(&argv(&["infra-repo", "--team", "home-infra"]));
        let mut args = json!({});
        if let Some(p) = pos.first() { args["alias"] = json!(p); }
        args["team"] = json!("home-infra");

        let r = tool_unregister(&c, "home-infra", "me", &args).unwrap();
        assert_eq!(r["id"], "home-infra/infra-repo");
        assert_eq!(r["removed"], true);
        assert!(peer_exists(&c, "home-infra", "me"), "caller must survive");

        // team/alias form
        let (_, pos) = parse_args(&argv(&["home-infra/me"]));
        let (t, a) = pos[0].split_once('/').unwrap();
        let r = tool_unregister(&c, "x", "y", &json!({"team":t,"alias":a})).unwrap();
        assert_eq!(r["id"], "home-infra/me");
        assert_eq!(r["removed"], true);
    }

    #[test]
    fn strip_block_removes_only_our_block() {
        let block = bootstrap_block("astrub", "classic");
        let user_doc = "# My project\n\nSome real notes.\n";

        // block appended to a real doc -> doc survives, block gone
        let combined = upsert_block(user_doc, &block);
        assert!(has_block(&combined));
        let stripped = strip_block(&combined);
        assert!(!has_block(&stripped));
        assert!(stripped.contains("Some real notes."));
        assert!(stripped.contains("# My project"));

        // a file that is nothing but the block strips to empty -> caller deletes it
        assert!(strip_block(&block).trim().is_empty());

        // a doc without our block is untouched
        assert_eq!(strip_block(user_doc), user_doc);
    }

    #[test]
    fn poll_and_send_do_not_conjure_ghost_peers() {
        // a bare poll/send must not create a peer — only register does. otherwise an
        // uninstalled identity shows up on the roster and send()'s recipient check lies
        let c = mem();
        tool_poll(&c, "stonebill", "stonebill").unwrap();
        tool_send(&c, "stonebill", "stonebill", &json!({"to":"nobody","body":"x"})).unwrap();
        let peers = tool_peers(&c, "stonebill", &json!({})).unwrap();
        assert!(peers["peers"].as_array().unwrap().is_empty());
        assert!(!peer_exists(&c, "stonebill", "stonebill"));

        // register creates it; a later poll just refreshes last_seen
        tool_register(&c, "stonebill", "stonebill", &json!({"card":"real"})).unwrap();
        assert!(peer_exists(&c, "stonebill", "stonebill"));
        tool_poll(&c, "stonebill", "stonebill").unwrap();
        let peers = tool_peers(&c, "stonebill", &json!({})).unwrap();
        assert_eq!(peers["peers"].as_array().unwrap().len(), 1);
        assert_eq!(peers["peers"][0]["card"], "real");
    }

    #[test]
    fn find_git_root_walks_up_from_subdir() {
        // installing from a subfolder must resolve to the enclosing worktree
        let root = find_git_root(Path::new("src")).expect("crate root is a git repo");
        assert!(root.join(".git").exists());
        assert!(root.join("Cargo.toml").exists());
        assert!(find_git_root(Path::new("/")).is_none());
    }

    #[test]
    fn send_warns_on_unregistered_recipient() {
        let c = mem();
        tool_register(&c, "mycs", "bff", &json!({})).unwrap();

        // typo'd alias: delivered, but flagged so the sender can catch it
        let r = tool_send(&c, "mycs", "bff", &json!({"to":"claude","body":"hi"})).unwrap();
        assert_eq!(r["ok"], true);
        assert_eq!(r["recipient_registered"], false);
        assert!(r["known_peers"].as_array().unwrap().iter().any(|p| p == "mycs/bff"));

        // a registered recipient carries no warning
        let r = tool_send(&c, "mycs", "bff", &json!({"to":"bff","body":"self"})).unwrap();
        assert!(r["warning"].is_null());

        // broadcasts are never flagged — they have no single recipient
        let r = tool_send(&c, "mycs", "bff", &json!({"to":"team:mycs","body":"all"})).unwrap();
        assert!(r["warning"].is_null());
    }

    #[test]
    fn late_joining_agent_receives_earlier_mail() {
        // the dynamic-join case: mail sent before an agent registers is still delivered,
        // because poll matches on recipient and a new cursor starts at 0
        let c = mem();
        tool_register(&c, "mycs", "bff", &json!({})).unwrap();
        tool_send(&c, "mycs", "bff", &json!({"to":"back","body":"do the changes"})).unwrap();

        // 'back' joins only afterwards
        tool_register(&c, "mycs", "back", &json!({})).unwrap();
        let r = tool_poll(&c, "mycs", "back").unwrap();
        assert_eq!(r["count"], 1);
        assert_eq!(r["messages"][0]["body"], "do the changes");
    }

    #[test]
    fn sync_detects_and_fixes_orphans() {
        let c = mem();
        tool_register(&c, "astrub", "client", &json!({})).unwrap();
        tool_register(&c, "astrub", "ghost",  &json!({})).unwrap();
        tool_register(&c, "home-infra", "infra-repo", &json!({})).unwrap();
        // an open task from client to infra-repo, and infra-repo drains once so it has a cursor
        tool_send(&c, "astrub", "client", &json!({"to":"home-infra/infra-repo","body":"need IP","type":"task"})).unwrap();
        tool_poll(&c, "home-infra", "infra-repo").unwrap();
        // ghost must actually receive something for a cursor row to be written
        tool_send(&c, "astrub", "client", &json!({"to":"ghost","body":"hi"})).unwrap();
        tool_poll(&c, "astrub", "ghost").unwrap();

        // clean bus: nothing orphaned yet
        let r = tool_sync(&c, false).unwrap();
        assert_eq!(r["orphaned_tasks"].as_array().unwrap().len(), 0);
        assert_eq!(r["dead_cursors"].as_array().unwrap().len(), 0);

        // unregister leaves ghost's cursor behind (dead cursor);
        // delete-team removes infra-repo entirely (its recipient task orphans)
        tool_unregister(&c, "astrub", "ghost", &json!({})).unwrap();
        tool_delete_team(&c, "home-infra", true).unwrap();

        let r = tool_sync(&c, false).unwrap();
        assert_eq!(r["orphaned_tasks"].as_array().unwrap().len(), 1);
        assert_eq!(r["orphaned_tasks"][0]["to"], "home-infra/infra-repo");
        assert_eq!(r["dead_cursors"].as_array().unwrap().len(), 1);
        assert_eq!(r["dead_cursors"][0]["alias"], "ghost");
        // detect-only must not mutate
        assert_eq!(tool_tasks(&c, "astrub", "client", &json!({"filter":"open"})).unwrap()["count"], 1);

        // --fix: task goes failed (delivered back to the sender), cursor pruned
        let r = tool_sync(&c, true).unwrap();
        assert_eq!(r["failed_tasks"], 1);
        assert_eq!(r["pruned_cursors"], 1);
        assert_eq!(tool_tasks(&c, "astrub", "client", &json!({"filter":"open"})).unwrap()["count"], 0);
        let got = tool_poll(&c, "astrub", "client").unwrap();
        assert!(got["messages"].as_array().unwrap().iter().any(|m| m["state"] == "failed"));

        // idempotent: a second sync finds nothing
        let r = tool_sync(&c, true).unwrap();
        assert_eq!(r["orphaned_tasks"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn discover_identity_from_hook_and_bootstrap() {
        let dir = std::env::temp_dir().join(format!("ab-disc-{}", short_id()));
        std::fs::create_dir_all(dir.join(".claude")).unwrap();
        // from the SessionStart hook's --team/--as
        std::fs::write(dir.join(".claude/settings.local.json"),
            r#"{"hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"agent-bus doctor --team astrub --as classic --quiet; agent-bus poll --team astrub --as classic"}]}]}}"#).unwrap();
        assert_eq!(discover_identity(&dir), Some(("astrub".into(), "classic".into())));
        assert!(is_agentbus_repo(&dir));

        // from the CLAUDE.local.md bootstrap when no settings hook
        let dir2 = std::env::temp_dir().join(format!("ab-disc2-{}", short_id()));
        std::fs::create_dir_all(&dir2).unwrap();
        std::fs::write(dir2.join("CLAUDE.local.md"),
            format!("{}\nidentity `sergen/infra-home`.\n{}", BLOCK_START, BLOCK_END)).unwrap();
        assert_eq!(discover_identity(&dir2), Some(("sergen".into(), "infra-home".into())));
        assert!(is_agentbus_repo(&dir2));

        // a non-agent-bus dir yields nothing
        let dir3 = std::env::temp_dir().join(format!("ab-disc3-{}", short_id()));
        std::fs::create_dir_all(&dir3).unwrap();
        assert_eq!(discover_identity(&dir3), None);
        assert!(!is_agentbus_repo(&dir3));

        std::fs::remove_dir_all(&dir).ok(); std::fs::remove_dir_all(&dir2).ok(); std::fs::remove_dir_all(&dir3).ok();
    }

    #[test]
    fn add_global_hook_preserves_and_is_idempotent() {
        let dir = std::env::temp_dir().join(format!("ab-gh-{}", short_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("settings.json");
        // pre-existing unrelated SessionStart hook + other settings must survive
        std::fs::write(&p, r#"{"model":"x","hooks":{"SessionStart":[{"hooks":[{"type":"command","command":"bash other.sh"}]}]}}"#).unwrap();

        assert!(add_global_hook(&p));            // added
        assert!(!add_global_hook(&p));           // idempotent — already there

        let v: Value = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        assert_eq!(v["model"], "x");             // other settings intact
        let arr = v["hooks"]["SessionStart"].as_array().unwrap();
        assert_eq!(arr.len(), 2);                // existing + ours
        let cmds: Vec<&str> = arr.iter().flat_map(|e| e["hooks"].as_array().unwrap())
            .filter_map(|h| h["command"].as_str()).collect();
        assert!(cmds.iter().any(|c| c.contains("bash other.sh")));
        assert!(cmds.iter().any(|c| c.contains("agent-bus doctor --quiet")));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_mcp_config_roundtrips_and_preserves_others() {
        let dir = std::env::temp_dir().join(format!("ab-wmc-{}", short_id()));
        std::fs::create_dir_all(&dir).unwrap();
        // pre-existing unrelated server must survive a rewrite
        std::fs::write(dir.join(".mcp.json"),
            r#"{"mcpServers":{"other":{"command":"x"}}}"#).unwrap();
        let p = write_mcp_config(&dir.to_string_lossy(), "sergen", "infra-home", "/bin/agent-bus");
        assert_eq!(mcp_identity(&p), Some(("sergen".into(), "infra-home".into())));
        let v: Value = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
        assert_eq!(v["mcpServers"]["agent-bus"]["command"], "/bin/agent-bus");
        assert_eq!(v["mcpServers"]["other"]["command"], "x"); // untouched
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn mcp_identity_parses_team_alias() {
        let dir = std::env::temp_dir().join(format!("ab-mcp-{}", short_id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join(".mcp.json");
        std::fs::write(&p, r#"{"mcpServers":{"agent-bus":{"env":{"AGENT_BUS_TEAM":"sergen","AGENT_BUS_ALIAS":"infra-home"}}}}"#).unwrap();
        assert_eq!(mcp_identity(&p), Some(("sergen".into(), "infra-home".into())));
        assert_eq!(mcp_identity(&dir.join("nope.json")), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn team_crud_lifecycle() {
        let c = mem();

        // create
        assert_eq!(tool_create_team(&c, "alpha").unwrap()["created"], true);
        assert!(team_row_exists(&c, "alpha"));
        // duplicate create refused
        assert_eq!(tool_create_team(&c, "alpha").unwrap()["ok"], false);
        // invalid name refused
        assert_eq!(tool_create_team(&c, "bad name").unwrap()["ok"], false);

        // rename (empty team)
        assert_eq!(tool_rename_team(&c, "alpha", "beta").unwrap()["renamed"], true);
        assert!(!team_row_exists(&c, "alpha") && team_row_exists(&c, "beta"));

        // delete empty team
        assert_eq!(tool_delete_team(&c, "beta", false).unwrap()["deleted"], true);
        assert!(!team_row_exists(&c, "beta"));
        // delete missing team refused
        assert_eq!(tool_delete_team(&c, "beta", false).unwrap()["ok"], false);
    }

    #[test]
    fn delete_team_guards_populated_teams() {
        let c = mem();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        tool_register(&c, "astrub", "sync",    &json!({})).unwrap();

        // refused without --force, and the team survives
        let r = tool_delete_team(&c, "astrub", false).unwrap();
        assert_eq!(r["ok"], false);
        assert_eq!(r["needs_force"], true);
        assert_eq!(r["agents"].as_array().unwrap().len(), 2);
        assert!(team_row_exists(&c, "astrub"));

        // --force removes team and its agents
        let r = tool_delete_team(&c, "astrub", true).unwrap();
        assert_eq!(r["removed_agents"], 2);
        assert!(!team_row_exists(&c, "astrub"));
        assert!(!peer_exists(&c, "astrub", "classic"));
    }

    #[test]
    fn rename_team_cascades_to_messages_and_peers() {
        let c = mem();
        tool_register(&c, "old", "a", &json!({})).unwrap();
        tool_register(&c, "old", "b", &json!({})).unwrap();
        tool_send(&c, "old", "a", &json!({"to":"b","body":"hi"})).unwrap();

        let r = tool_rename_team(&c, "old", "new").unwrap();
        assert_eq!(r["agents"], 2);

        // peer moved, and its mail is still deliverable under the new team
        assert!(peer_exists(&c, "new", "b") && !peer_exists(&c, "old", "b"));
        let got = tool_poll(&c, "new", "b").unwrap();
        assert_eq!(got["count"], 1);
        assert_eq!(got["messages"][0]["from"], "new/a");

        // target-name collision refused
        tool_create_team(&c, "taken").unwrap();
        assert_eq!(tool_rename_team(&c, "new", "taken").unwrap()["ok"], false);
    }

    #[test]
    fn team_can_exist_without_peers() {
        let c = mem();
        // an empty team stands on its own and lists with zero agents
        ensure_team(&c, "ghost").unwrap();
        let teams = list_teams(&c);
        assert_eq!(teams, vec![("ghost".to_string(), 0)]);

        // registering into a brand-new team creates that team implicitly
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        let teams = list_teams(&c);
        assert_eq!(teams, vec![("astrub".to_string(), 1), ("ghost".to_string(), 0)]);

        // and the empty team survives a peer joining another team
        assert!(list_teams(&c).iter().any(|(t, n)| t == "ghost" && *n == 0));
    }

    #[test]
    fn abort_classifies_ctrl_c_and_esc() {
        // ctrl-c / esc must abort the wizard; anything else falls back to the default
        assert!(is_abort(&InquireError::OperationInterrupted));
        assert!(is_abort(&InquireError::OperationCanceled));
        assert!(!is_abort(&InquireError::NotTTY));
    }

    #[test]
    fn tasks_open_filter_excludes_info() {
        // 'info' is a status state, not open work — doctor's open_tasks count
        // excludes it, so --filter open must too or the two disagree
        let c = mem();
        tool_register(&c, "astrub", "sync",    &json!({})).unwrap();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();

        tool_send(&c, "astrub", "sync", &json!({
            "to":"classic","body":"real work","type":"task","state":"submitted"})).unwrap();
        tool_send(&c, "astrub", "sync", &json!({
            "to":"classic","body":"fyi","type":"status","state":"info"})).unwrap();
        tool_send(&c, "astrub", "sync", &json!({
            "to":"classic","body":"done","type":"task","state":"completed"})).unwrap();

        let all = tool_tasks(&c, "astrub", "sync", &json!({"filter":"all"})).unwrap();
        assert_eq!(all["count"], 3);

        // only the 'submitted' task is open
        let open = tool_tasks(&c, "astrub", "sync", &json!({"filter":"open"})).unwrap();
        assert_eq!(open["count"], 1);
        assert_eq!(open["tasks"][0]["state"], "submitted");
    }

    #[test]
    fn unread_excludes_self_broadcast() {
        // a peer's own broadcasts must not count against its own unread:
        // poll excludes self-echo (never receipts them), so unread would be
        // permanently inflated without the matching exclusion.
        let c = mem();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        tool_register(&c, "astrub", "sync",    &json!({})).unwrap();

        // classic broadcasts to its own team, then drains its inbox
        tool_send(&c, "astrub", "classic", &json!({"to":"team:astrub","body":"hello team"})).unwrap();
        tool_poll(&c, "astrub", "classic").unwrap();

        let r = tool_peers(&c, "astrub", &json!({"unread": true})).unwrap();
        let classic = r["peers"].as_array().unwrap().iter()
            .find(|p| p["alias"] == "classic").unwrap();
        // self-echo excluded from poll -> must also be excluded from unread
        assert_eq!(classic["unread"].as_i64().unwrap(), 0);

        // sanity: sync (a real recipient) still sees it as unread
        let sync = r["peers"].as_array().unwrap().iter()
            .find(|p| p["alias"] == "sync").unwrap();
        assert_eq!(sync["unread"].as_i64().unwrap(), 1);
    }

    #[test]
    fn registry_lists_teams_and_aliases() {
        let c = mem();
        tool_register(&c, "astrub", "sync",    &json!({})).unwrap();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        tool_register(&c, "webapp", "api",     &json!({})).unwrap();
        let teams = list_teams(&c);
        assert!(teams.iter().any(|(t, n)| t == "astrub" && *n == 2));
        assert!(teams.iter().any(|(t, n)| t == "webapp" && *n == 1));
        let mut al = list_aliases(&c, "astrub");
        al.sort();
        assert_eq!(al, vec!["classic".to_string(), "sync".to_string()]);
        assert!(list_aliases(&c, "nope").is_empty());
    }

    #[test]
    fn guess_identity_from_repo_name() {
        assert_eq!(guess_identity("astrub-classic"), ("astrub".into(), "classic".into()));
        assert_eq!(guess_identity("astrub-client"),  ("astrub".into(), "client".into()));
        assert_eq!(guess_identity("standalone"),     ("default".into(), "standalone".into()));
    }

    #[test]
    fn mcp_handshake() {
        let c = mem();
        let resp = handle(&c, "astrub", "sync", &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}})).unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], "agent-bus");
        assert!(handle(&c, "astrub", "sync", &json!({"jsonrpc":"2.0","method":"notifications/initialized"})).is_none());
        let resp = handle(&c, "astrub", "sync", &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).unwrap();
        assert_eq!(resp["result"]["tools"].as_array().unwrap().len(), 8);
    }
}
