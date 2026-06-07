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

use inquire::{Select, Text};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, IsTerminal, Write};
use std::path::PathBuf;
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

fn mark_seen(conn: &Connection, team: &str, alias: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO peers(team, alias, last_seen) VALUES(?1, ?2, ?3) \
         ON CONFLICT(team, alias) DO UPDATE SET last_seen=?3",
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
    Ok(json!({"ok": true, "id": id, "task_id": task_id, "to": format!("{}/{}", rt, ra)}))
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
            let sql = format!("{} WHERE lm.state NOT IN ('completed','failed') ORDER BY lm.created_at DESC", base);
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
fn list_teams(conn: &Connection) -> Vec<(String, i64)> {
    let mut stmt = match conn.prepare("SELECT team, COUNT(*) FROM peers GROUP BY team ORDER BY team") {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))
        .map(|rows| rows.flatten().collect())
        .unwrap_or_default()
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
        _ => return json!({"ok": false, "error": format!("unknown tool: {}", name)}),
    };
    match r {
        Ok(v)  => v,
        Err(e) => json!({"ok": false, "error": e.to_string()}),
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
            "description": "Send a message. to: alias (same team), 'team/alias' (cross-team), 'team:NAME' (broadcast), 'all' (my team), '*' (global). type='task' for work requests with lifecycle tracking.",
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
            "inputSchema": {"type":"object","properties":{}}
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
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "send", &args));
}
fn cli_poll(flags: &HashMap<String, String>) {
    let conn = open_db();
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "poll", &json!({})));
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
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "peek", &args));
}
fn cli_tasks(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("filter") { args["filter"] = json!(v); }
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "tasks", &args));
}
fn cli_peers(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(t) = flags.get("team")  { args["team"]   = json!(t); }
    if flags.get("unread").is_some()     { args["unread"] = json!(true); }
    println!("{}", call_tool(&conn, &cli_team(flags), "cli", "peers", &args));
}
fn cli_register(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("card") { args["card"] = json!(v); }
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "register", &args));
}
fn cli_teams() {
    let conn = open_db();
    let teams: Vec<Value> = list_teams(&conn).iter().map(|(t, n)| json!({"team":t,"agents":n})).collect();
    println!("{}", json!({"ok":true,"teams":teams}));
}
fn cli_prune(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("days") {
        if let Ok(n) = v.parse::<i64>() { args["days"] = json!(n); }
    }
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "prune", &args));
}
fn cli_unregister(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(t) = flags.get("team")  { args["team"]  = json!(t); }
    if let Some(a) = flags.get("as").or_else(|| flags.get("alias")) { args["alias"] = json!(a); }
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "unregister", &args));
}
fn cli_whoami(flags: &HashMap<String, String>) {
    let team  = cli_team(flags);
    let alias = cli_alias(flags);
    let team_src  = if std::env::var("AGENT_BUS_TEAM").is_ok() { "env" } else { "default" };
    let alias_src = if std::env::var("AGENT_BUS_ALIAS").is_ok() { "env" }
                    else if flags.contains_key("as") || flags.contains_key("alias") { "flag" }
                    else { "default" };
    println!("{}", json!({
        "ok":       true,
        "identity": format!("{}/{}", team, alias),
        "team":     {"value": team,  "source": team_src},
        "alias":    {"value": alias, "source": alias_src},
        "db":       db_path().display().to_string(),
        "bus_home": bus_home().display().to_string(),
    }));
}
fn cli_doctor(_flags: &HashMap<String, String>) {
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
    let cwd = std::env::current_dir().unwrap_or_default();
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

    // 4. MCP auto-approved
    let settings = cwd.join(".claude").join("settings.local.json");
    let approved = settings.exists()
        && fs::read_to_string(&settings).unwrap_or_default().contains("agent-bus");
    checks.push(json!({
        "check":  "mcp_auto_approved",
        "status": if approved { "ok" } else { "warn" },
        "note":   if approved { "" } else { "run 'agent-bus install' to auto-approve" }
    }));

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
        let open: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT task_id) FROM messages fm \
                 INNER JOIN (SELECT task_id, MAX(id) last_id FROM messages GROUP BY task_id) agg ON fm.id=agg.last_id \
                 WHERE fm.state NOT IN ('completed','failed','info')",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        checks.push(json!({"check":"open_tasks","status":"ok","count":open}));
    }

    println!("{}", json!({"ok": all_ok, "checks": checks}));
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
fn ask_select(label: &str, options: &[String], default: &str, help: Option<&str>) -> String {
    if is_tty() {
        let start = options.iter().position(|o| o == default).unwrap_or(0);
        let mut s = Select::new(label, options.to_vec()).with_starting_cursor(start);
        if let Some(h) = help { s = s.with_help_message(h); }
        s.prompt().unwrap_or_else(|_| default.to_string())
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
            _ => default.to_string(),
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

fn apply_install(tool: &str, team: &str, alias: &str, repo: &str, card: Option<&str>) {
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

fn upsert_block(existing: &str, block: &str) -> String {
    let start = "<!-- agent-bus-bootstrap -->";
    let end   = "<!-- /agent-bus-bootstrap -->";
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

fn install_claude(repo: &str, team: &str, alias: &str, exe: &str) {
    let mcp_path = PathBuf::from(repo).join(".mcp.json");
    let mut root: Value = if mcp_path.exists() {
        serde_json::from_str(&fs::read_to_string(&mcp_path).unwrap_or_default()).unwrap_or(json!({}))
    } else {
        json!({})
    };
    if !root.is_object()         { root = json!({}); }
    if !root["mcpServers"].is_object() { root["mcpServers"] = json!({}); }
    root["mcpServers"]["agent-bus"] = json!({
        "command": exe, "args": ["serve"],
        "env": {"AGENT_BUS_TEAM": team, "AGENT_BUS_ALIAS": alias}
    });
    fs::write(&mcp_path, serde_json::to_string_pretty(&root).unwrap() + "\n").expect("write .mcp.json");
    println!("wrote {}", mcp_path.display());

    let cm = PathBuf::from(repo).join("CLAUDE.md");
    let existing = fs::read_to_string(&cm).unwrap_or_default();
    let updated  = upsert_block(&existing, &bootstrap_block(team, alias));
    fs::write(&cm, updated).expect("write CLAUDE.md");
    println!("wrote agent-bus bootstrap into {}", cm.display());

    enable_mcp_setting(repo);
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
<!-- /agent-bus-bootstrap -->",
        team = team, alias = alias, flag = fp.display()
    )
}

// ---------------------------------------------------------------- arg parsing
fn parse_flags(args: &[String]) -> HashMap<String, String> {
    let mut m = HashMap::new();
    let mut i = 0;
    while i < args.len() {
        if let Some(key) = args[i].strip_prefix("--") {
            let val = if i + 1 < args.len() && !args[i + 1].starts_with("--") {
                i += 1;
                args[i].clone()
            } else {
                "true".into()
            };
            m.insert(key.to_string(), val);
        }
        i += 1;
    }
    m
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let cmd   = argv.get(1).map(|s| s.as_str()).unwrap_or("");
    let flags = parse_flags(&argv[2.min(argv.len())..]);
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
        "teams"               => cli_teams(),
        "register"            => cli_register(&flags),
        "unregister"          => cli_unregister(&flags),
        "whoami"              => cli_whoami(&flags),
        "doctor"              => cli_doctor(&flags),
        "version" | "--version" | "-V" => println!("{}", version_string()),
        "" | "setup"          => wizard(),
        _ => eprintln!(
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
             teams\n\
             register [--card ..] [--team t] [--as a]\n\
             unregister [--as a] [--team t]\n\
             whoami\n\
             doctor\n\
             version"
        ),
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
