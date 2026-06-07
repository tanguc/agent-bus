// agent-bus — minimal A2A-shaped message bus for coordinating CLI agent sessions.
//
//   agent-bus                       first-time interactive setup wizard
//   agent-bus serve                 MCP stdio server (point a CLI's MCP config at this)
//   agent-bus install [--tool ..]   non-interactive installer (flags) / wizard (no flags)
//   agent-bus send --to X --body Y  CLI client
//   agent-bus poll  [--as alias] [--team t]
//   agent-bus peers [--team t|*]
//   agent-bus register [--card ..]
//
// Identity = AGENT_BUS_TEAM/AGENT_BUS_ALIAS (server) or --team/--as (CLI). Teams are a
// logical namespace over one shared bus (~/.agent-bus/bus.db): same-team is the default
// scope, cross-team is addressable explicitly. A2A-shaped (task_id + lifecycle state).

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id         TEXT,
    sender_team     TEXT,
    sender_alias    TEXT,
    recipient_team  TEXT,
    recipient_alias TEXT,   -- alias, or '*' for a team/global broadcast
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
";

// ---------------------------------------------------------------- paths/util
fn home_dir() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_else(|_| ".".into()))
}
fn bus_home() -> PathBuf {
    std::env::var("AGENT_BUS_HOME").map(PathBuf::from).unwrap_or_else(|_| home_dir().join(".agent-bus"))
}
fn db_path() -> PathBuf {
    std::env::var("AGENT_BUS_DB").map(PathBuf::from).unwrap_or_else(|_| bus_home().join("bus.db"))
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
//   "all"          -> (my_team, "*")     team broadcast
//   "*"|"everyone" -> ("*", "*")         global broadcast
//   "team:NAME"    -> (NAME, "*")        named-team broadcast
//   "team/alias"   -> (team, alias)      cross-team direct
//   "alias"        -> (my_team, alias)   same-team direct
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
    let body = args["body"].as_str().unwrap_or("");
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
        .query_row("SELECT last_id FROM cursors WHERE team=?1 AND alias=?2", params![team, alias], |r| r.get(0))
        .optional()?
        .unwrap_or(0);
    let mut stmt = conn.prepare(
        "SELECT id, task_id, sender_team, sender_alias, recipient_team, recipient_alias, type, state, body, created_at \
         FROM messages WHERE id>?1 AND ( \
           (recipient_team=?2 AND (recipient_alias=?3 OR recipient_alias='*')) \
           OR (recipient_team='*' AND recipient_alias='*') ) ORDER BY id",
    )?;
    let msgs: Vec<Value> = stmt
        .query_map(params![last, team, alias], |r| {
            let st = r.get::<_, String>(2)?;
            let sa = r.get::<_, String>(3)?;
            let rt = r.get::<_, String>(4)?;
            let ra = r.get::<_, String>(5)?;
            Ok(json!({
                "id": r.get::<_, i64>(0)?,
                "task_id": r.get::<_, String>(1)?,
                "from": format!("{}/{}", st, sa),
                "to": format!("{}/{}", rt, ra),
                "type": r.get::<_, String>(6)?,
                "state": r.get::<_, String>(7)?,
                "body": r.get::<_, String>(8)?,
                "at": r.get::<_, i64>(9)?,
            }))
        })?
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
    Ok(json!({"ok": true, "count": msgs.len(), "messages": msgs}))
}

fn tool_peers(conn: &Connection, my_team: &str, args: &Value) -> rusqlite::Result<Value> {
    let filter = args["team"].as_str().unwrap_or(my_team);
    let (sql, p): (&str, Vec<String>) = if filter == "*" {
        ("SELECT team, alias, card, last_seen FROM peers ORDER BY team, alias", vec![])
    } else {
        ("SELECT team, alias, card, last_seen FROM peers WHERE team=?1 ORDER BY alias", vec![filter.to_string()])
    };
    let mut stmt = conn.prepare(sql)?;
    let peers: Vec<Value> = stmt
        .query_map(rusqlite::params_from_iter(p.iter()), |r| {
            let t = r.get::<_, String>(0)?;
            let a = r.get::<_, String>(1)?;
            Ok(json!({
                "id": format!("{}/{}", t, a),
                "team": t,
                "alias": a,
                "card": r.get::<_, Option<String>>(2)?,
                "last_seen": r.get::<_, i64>(3)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"ok": true, "scope": filter, "peers": peers}))
}

// registry helpers — the bus IS the registry (derived from the peers table)
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
    let r = match name {
        "register" => tool_register(conn, team, alias, args),
        "send" => tool_send(conn, team, alias, args),
        "poll" => tool_poll(conn, team, alias),
        "peers" => tool_peers(conn, team, args),
        _ => return json!({"ok": false, "error": format!("unknown tool: {}", name)}),
    };
    match r {
        Ok(v) => v,
        Err(e) => json!({"ok": false, "error": e.to_string()}),
    }
}

fn tools_list() -> Value {
    json!([
        {"name":"register","description":"Register/refresh THIS agent (team+alias from env) in the bus. Call once at session start.",
         "inputSchema":{"type":"object","properties":{"card":{"type":"string","description":"optional capability blurb (Agent Card)"}}}},
        {"name":"send","description":"Send to: a bare alias (same team), 'team/alias' (cross-team), 'team:NAME' (team broadcast), 'all' (my team), or '*' (global). type='task' for work requests (task_id + lifecycle state).",
         "inputSchema":{"type":"object","properties":{
            "to":{"type":"string","description":"alias | team/alias | team:NAME | all | *"},
            "body":{"type":"string"},
            "type":{"type":"string","enum":["task","status","message"]},
            "state":{"type":"string","enum":["submitted","working","completed","failed","info"]},
            "task_id":{"type":"string","description":"reuse to update an existing task"}},
            "required":["to","body"]}},
        {"name":"poll","description":"Fetch new messages addressed to me / my team / global since last poll; advances the cursor.",
         "inputSchema":{"type":"object","properties":{}}},
        {"name":"peers","description":"List agents in my team (default), a named team (team:'NAME'), or everyone (team:'*'). This is the roster.",
         "inputSchema":{"type":"object","properties":{"team":{"type":"string","description":"team name, or '*' for all"}}}}
    ])
}

// ---------------------------------------------------------------- MCP stdio server
fn handle(conn: &Connection, team: &str, alias: &str, req: &Value) -> Option<Value> {
    let method = req["method"].as_str().unwrap_or("");
    let id = req.get("id").cloned();
    let params = &req["params"];
    match method {
        "initialize" => {
            let proto = params["protocolVersion"].as_str().unwrap_or("2025-06-18");
            Some(json!({"jsonrpc":"2.0","id":id,"result":{
                "protocolVersion":proto,
                "capabilities":{"tools":{}},
                "serverInfo":{"name":"agent-bus","version":env!("CARGO_PKG_VERSION")}}}))
        }
        "notifications/initialized" | "initialized" => None,
        "ping" => Some(json!({"jsonrpc":"2.0","id":id,"result":{}})),
        "tools/list" => Some(json!({"jsonrpc":"2.0","id":id,"result":{"tools":tools_list()}})),
        "tools/call" => {
            let name = params["name"].as_str().unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            let out = call_tool(conn, team, alias, name, &args);
            let is_err = !out["ok"].as_bool().unwrap_or(true);
            Some(json!({"jsonrpc":"2.0","id":id,"result":{
                "content":[{"type":"text","text":out.to_string()}],"isError":is_err}}))
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
            Ok(l) => l,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let req: Value = match serde_json::from_str(line) {
            Ok(v) => v,
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
    for (k, j) in [("to", "to"), ("body", "body"), ("type", "type"), ("state", "state"), ("task-id", "task_id")] {
        if let Some(v) = flags.get(k) {
            args[j] = json!(v);
        }
    }
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "send", &args));
}
fn cli_poll(flags: &HashMap<String, String>) {
    let conn = open_db();
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "poll", &json!({})));
}
fn cli_peers(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(t) = flags.get("team") {
        args["team"] = json!(t);
    }
    println!("{}", call_tool(&conn, &cli_team(flags), "cli", "peers", &args));
}
fn cli_register(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("card") {
        args["card"] = json!(v);
    }
    println!("{}", call_tool(&conn, &cli_team(flags), &cli_alias(flags), "register", &args));
}
fn cli_teams() {
    let conn = open_db();
    let teams: Vec<Value> = list_teams(&conn).iter().map(|(t, n)| json!({"team": t, "agents": n})).collect();
    println!("{}", json!({"ok": true, "teams": teams}));
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
// guess team/alias from a repo dir name: "astrub-classic" -> ("astrub","classic")
fn guess_identity(name: &str) -> (String, String) {
    match name.split_once('-') {
        Some((t, a)) if !t.is_empty() && !a.is_empty() => (t.to_string(), a.to_string()),
        _ => ("default".to_string(), name.to_string()),
    }
}
fn basename(path: &str) -> String {
    PathBuf::from(path).file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "agent".into())
}

// registry-aware pickers: show what teams/aliases already exist so you choose from
// reality instead of guessing. The path-guess is just the default.
fn pick_team(conn: &Connection, default: &str) -> String {
    let teams = list_teams(conn);
    if teams.is_empty() {
        println!("(no teams yet — you're creating the first one)");
    } else {
        println!("Existing teams:");
        for (t, n) in &teams {
            println!("  - {} ({} agent{})", t, n, if *n == 1 { "" } else { "s" });
        }
    }
    prompt("Team (logical group)", default)
}
fn pick_alias(conn: &Connection, team: &str, default: &str) -> String {
    let al = list_aliases(conn, team);
    if !al.is_empty() {
        println!("Agents already in '{}': {}", team, al.join(", "));
    }
    let a = prompt("Alias (this agent's name)", default);
    if al.iter().any(|x| x == &a) {
        println!("note: {}/{} already exists — installing reuses that identity", team, a);
    }
    a
}

fn wizard() {
    println!("agent-bus — setup\n");
    let tool = choose("Which tool is this for", &["claude", "codex", "copilot"], "claude");
    let cwd = std::env::current_dir().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|_| ".".into());
    let repo = if tool == "claude" {
        prompt("Target repo path", &cwd)
    } else {
        cwd.clone()
    };
    let (gt, ga) = guess_identity(&basename(&repo));
    let conn = open_db();
    let team = pick_team(&conn, &gt);
    let alias = pick_alias(&conn, &team, &ga);
    apply_install(&tool, &team, &alias, &repo);
}

fn install(flags: &HashMap<String, String>) {
    // fully-specified -> non-interactive; otherwise fall into the wizard for missing bits
    if flags.is_empty() {
        return wizard();
    }
    let cwd = std::env::current_dir().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|_| ".".into());
    let tool = flags.get("tool").cloned().unwrap_or_else(|| choose("Tool", &["claude", "codex", "copilot"], "claude"));
    let repo = flags.get("repo").cloned().unwrap_or_else(|| if tool == "claude" { prompt("Target repo path", &cwd) } else { cwd.clone() });
    let (gt, ga) = guess_identity(&basename(&repo));
    let conn = open_db();
    let team = flags.get("team").cloned().unwrap_or_else(|| pick_team(&conn, &gt));
    let alias = flags.get("alias").cloned().unwrap_or_else(|| pick_alias(&conn, &team, &ga));
    apply_install(&tool, &team, &alias, &repo);
}

fn apply_install(tool: &str, team: &str, alias: &str, repo: &str) {
    let exe = std::env::current_exe().map(|p| p.to_string_lossy().to_string()).unwrap_or_else(|_| "agent-bus".into());
    match tool {
        "claude" => install_claude(repo, team, alias, &exe),
        "codex" => install_codex(team, alias, &exe),
        "copilot" => print_copilot(team, alias, &exe),
        other => {
            eprintln!("unknown tool: {} (use claude|codex|copilot)", other);
            return;
        }
    }
    // seed the registry so this identity appears for the NEXT setup's team/roster picker,
    // even before the session has started. The live session re-registers on start.
    let conn = open_db();
    let _ = tool_register(&conn, team, alias, &json!({"card": format!("installed for {}", tool)}));
    println!("✓ registered {}/{} on the bus", team, alias);
}

fn upsert_block(existing: &str, block: &str) -> String {
    let start = "<!-- agent-bus-bootstrap -->";
    let end = "<!-- /agent-bus-bootstrap -->";
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
    if !root.is_object() {
        root = json!({});
    }
    if !root["mcpServers"].is_object() {
        root["mcpServers"] = json!({});
    }
    root["mcpServers"]["agent-bus"] = json!({
        "command": exe, "args": ["serve"],
        "env": {"AGENT_BUS_TEAM": team, "AGENT_BUS_ALIAS": alias}
    });
    fs::write(&mcp_path, serde_json::to_string_pretty(&root).unwrap() + "\n").expect("write .mcp.json");
    println!("✓ wrote {}", mcp_path.display());

    let cm = PathBuf::from(repo).join("CLAUDE.md");
    let existing = fs::read_to_string(&cm).unwrap_or_default();
    let updated = upsert_block(&existing, &bootstrap_block(team, alias));
    fs::write(&cm, updated).expect("write CLAUDE.md");
    println!("✓ wrote agent-bus bootstrap into {}", cm.display());

    enable_mcp_setting(repo);
    println!("→ restart the Claude Code session in {} (no approval prompt — auto-enabled)", repo);
}

// auto-approve the agent-bus project MCP server so Claude Code skips the
// "New MCP server found, approve?" prompt. Targeted (just agent-bus), personal
// (settings.local.json, gitignored), merge-safe.
fn enable_mcp_setting(repo: &str) {
    let dir = PathBuf::from(repo).join(".claude");
    fs::create_dir_all(&dir).ok();
    let path = dir.join("settings.local.json");
    let mut root: Value = if path.exists() {
        serde_json::from_str(&fs::read_to_string(&path).unwrap_or_default()).unwrap_or(json!({}))
    } else {
        json!({})
    };
    if !root.is_object() {
        root = json!({});
    }
    let mut names = root["enabledMcpjsonServers"].as_array().cloned().unwrap_or_default();
    if !names.iter().any(|v| v == "agent-bus") {
        names.push(json!("agent-bus"));
    }
    root["enabledMcpjsonServers"] = json!(names);
    fs::write(&path, serde_json::to_string_pretty(&root).unwrap() + "\n").ok();
    println!("✓ auto-approved agent-bus in {}", path.display());
}

fn install_codex(team: &str, alias: &str, exe: &str) {
    let path = home_dir().join(".codex").join("config.toml");
    fs::create_dir_all(path.parent().unwrap()).ok();
    let existing = fs::read_to_string(&path).unwrap_or_default();
    if existing.contains("[mcp_servers.agent-bus]") {
        println!("• {} already has [mcp_servers.agent-bus] (edit it by hand to change team/alias)", path.display());
        return;
    }
    let block = format!(
        "\n[mcp_servers.agent-bus]\ncommand = \"{}\"\nargs = [\"serve\"]\nenv = {{ AGENT_BUS_TEAM = \"{}\", AGENT_BUS_ALIAS = \"{}\" }}\n",
        exe, team, alias
    );
    let mut f = fs::OpenOptions::new().create(true).append(true).open(&path).expect("open config.toml");
    write!(f, "{}", block).expect("append config.toml");
    println!("✓ added [mcp_servers.agent-bus] to {}", path.display());
    println!("→ restart Codex to load it");
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
2. Arm a doorbell so inbound mail wakes you:\n\
```\n\
Monitor(persistent:true, timeout_ms:3600000, command: |\n\
  f={flag}; last=\"\"; while true; do\n\
    if [ -f \"$f\" ]; then m=$(stat -f %m \"$f\" 2>/dev/null);\n\
      if [ \"$m\" != \"$last\" ]; then echo \"BUS: new mail for {team}/{alias} — call agent-bus poll()\"; last=\"$m\"; fi; fi;\n\
    sleep 2; done)\n\
```\n\
3. On a `BUS:` ping, call `mcp__agent-bus__poll` and act on the messages.\n\
4. Reply with `mcp__agent-bus__send` — to=\"alias\" (same team), \"team/alias\" (cross-team), or \"team:{team}\" (broadcast).\n\
5. `mcp__agent-bus__peers` lists your team roster (team:\"*\" = everyone).\n\
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
    let cmd = argv.get(1).map(|s| s.as_str()).unwrap_or("");
    let flags = parse_flags(&argv[2.min(argv.len())..]);
    match cmd {
        "serve" => serve(),
        "install" => install(&flags),
        "send" => cli_send(&flags),
        "poll" => cli_poll(&flags),
        "peers" => cli_peers(&flags),
        "teams" => cli_teams(),
        "register" => cli_register(&flags),
        "" | "setup" => wizard(),
        _ => {
            eprintln!(
                "agent-bus <command>\n  (no command) | setup    interactive first-time setup\n  serve                    MCP stdio server\n  install [--tool --team --alias --repo]\n  send --to X --body Y [--type task] [--state s] [--task-id id] [--team t] [--as a]\n  poll [--team t] [--as a]\n  peers [--team t|*]\n  teams                    list known teams (the registry)\n  register [--card ..] [--team t] [--as a]"
            );
        }
    }
}

// ---------------------------------------------------------------- tests
#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Connection {
        // route doorbell flag writes to a throwaway home so tests never touch ~/.agent-bus
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

        // re-poll drains
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 0);

        // completed reply on same task_id
        tool_send(&c, "astrub", "classic", &json!({"to":"sync","type":"status","state":"completed","task_id":task_id,"body":"255 green"})).unwrap();
        let r = tool_poll(&c, "astrub", "sync").unwrap();
        assert_eq!(r["messages"][0]["state"].as_str().unwrap(), "completed");
    }

    #[test]
    fn team_isolation_and_cross_team() {
        let c = mem();
        // astrub/sync broadcasts to its own team
        tool_send(&c, "astrub", "sync", &json!({"to":"all","body":"team note"})).unwrap();
        // a different team must NOT see it
        assert_eq!(tool_poll(&c, "webapp", "api").unwrap()["count"], 0);
        // astrub member sees it
        assert_eq!(tool_poll(&c, "astrub", "classic").unwrap()["count"], 1);

        // explicit cross-team direct
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
        assert_eq!(tool_poll(&c, "ops", "boss").unwrap()["count"], 0); // not addressed to ops

        tool_send(&c, "ops", "boss", &json!({"to":"*","body":"global"})).unwrap();
        // astrub/client never polled: it gets BOTH the earlier team:astrub broadcast AND the global one
        let r = tool_poll(&c, "astrub", "client").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 2);
        assert!(r["messages"].as_array().unwrap().iter().any(|m| m["body"] == "global"));
        // webapp/api is not in astrub, so it only gets the global broadcast
        let r = tool_poll(&c, "webapp", "api").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["body"].as_str().unwrap(), "global");
    }

    #[test]
    fn peers_roster_scoping() {
        let c = mem();
        tool_register(&c, "astrub", "sync", &json!({"card":"planning"})).unwrap();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        tool_register(&c, "webapp", "api", &json!({})).unwrap();

        let mine = tool_peers(&c, "astrub", &json!({})).unwrap();
        let names: Vec<&str> = mine["peers"].as_array().unwrap().iter().map(|p| p["alias"].as_str().unwrap()).collect();
        assert!(names.contains(&"sync") && names.contains(&"classic") && !names.contains(&"api"));

        let all = tool_peers(&c, "astrub", &json!({"team":"*"})).unwrap();
        assert_eq!(all["peers"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn registry_lists_teams_and_aliases() {
        let c = mem();
        tool_register(&c, "astrub", "sync", &json!({})).unwrap();
        tool_register(&c, "astrub", "classic", &json!({})).unwrap();
        tool_register(&c, "webapp", "api", &json!({})).unwrap();
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
        assert_eq!(guess_identity("astrub-client"), ("astrub".into(), "client".into()));
        assert_eq!(guess_identity("standalone"), ("default".into(), "standalone".into()));
    }

    #[test]
    fn mcp_handshake() {
        let c = mem();
        let resp = handle(&c, "astrub", "sync", &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{}})).unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], "agent-bus");
        assert!(handle(&c, "astrub", "sync", &json!({"jsonrpc":"2.0","method":"notifications/initialized"})).is_none());
        let resp = handle(&c, "astrub", "sync", &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).unwrap();
        assert_eq!(resp["result"]["tools"].as_array().unwrap().len(), 4);
    }
}
