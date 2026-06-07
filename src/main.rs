// agent-bus — minimal A2A-shaped message broker for coordinating CLI agent sessions.
//
// One self-contained binary:
//   agent-bus serve                 MCP stdio server (point a CLI's MCP config at this)
//   agent-bus install [--tool ..]   interactive installer: writes MCP config + CLAUDE.md bootstrap
//   agent-bus send --to X --body Y  CLI client
//   agent-bus poll [--as alias]     CLI client
//   agent-bus peers                 CLI client
//   agent-bus register [--card ..]  CLI client
//
// Identity = AGENT_BUS_ALIAS env (or --as for CLI). Shared state in ~/.agent-bus/
// (override with AGENT_BUS_HOME / AGENT_BUS_DB). A2A-shaped: messages carry a
// task_id + lifecycle state so the wire can grow into real A2A later.

use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id    TEXT,
    sender     TEXT,
    recipient  TEXT,
    type       TEXT,
    state      TEXT,
    body       TEXT,
    created_at INTEGER
);
CREATE TABLE IF NOT EXISTS cursors (
    alias   TEXT PRIMARY KEY,
    last_id INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS peers (
    alias     TEXT PRIMARY KEY,
    card      TEXT,
    last_seen INTEGER
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
fn alias_env() -> String {
    std::env::var("AGENT_BUS_ALIAS").unwrap_or_else(|_| "unknown".into())
}
fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}
fn short_id() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
    format!("{:08x}", (n.wrapping_mul(2654435761)) as u32)
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

fn mark_seen(conn: &Connection, alias: &str) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO peers(alias, last_seen) VALUES(?1, ?2) \
         ON CONFLICT(alias) DO UPDATE SET last_seen=?2",
        params![alias, now_ms()],
    )?;
    Ok(())
}

fn touch_doorbell(conn: &Connection, to: &str, self_alias: &str) {
    let targets: Vec<String> = if to == "all" {
        let mut v = vec![];
        if let Ok(mut s) = conn.prepare("SELECT alias FROM peers") {
            if let Ok(rows) = s.query_map([], |r| r.get::<_, String>(0)) {
                for a in rows.flatten() {
                    v.push(a);
                }
            }
        }
        v
    } else {
        vec![to.to_string()]
    };
    let dir = inbox_dir();
    fs::create_dir_all(&dir).ok();
    for t in targets {
        if t == self_alias {
            continue;
        }
        let _ = fs::write(dir.join(format!("{}.flag", t)), now_ms().to_string());
    }
}

// ---------------------------------------------------------------- tools
fn tool_register(conn: &Connection, alias: &str, args: &Value) -> rusqlite::Result<Value> {
    let card = args["card"].as_str();
    conn.execute(
        "INSERT INTO peers(alias, card, last_seen) VALUES(?1, ?2, ?3) \
         ON CONFLICT(alias) DO UPDATE SET card=COALESCE(?2, peers.card), last_seen=?3",
        params![alias, card, now_ms()],
    )?;
    Ok(json!({"ok": true, "alias": alias}))
}

fn tool_send(conn: &Connection, alias: &str, args: &Value) -> rusqlite::Result<Value> {
    let to = match args["to"].as_str() {
        Some(s) => s,
        None => return Ok(json!({"ok": false, "error": "missing 'to'"})),
    };
    let body = args["body"].as_str().unwrap_or("");
    let mtype = args["type"].as_str().unwrap_or("message");
    let state = args["state"].as_str().map(|s| s.to_string()).unwrap_or_else(|| {
        if mtype == "task" { "submitted".into() } else { "info".into() }
    });
    let task_id = args["task_id"].as_str().map(|s| s.to_string()).unwrap_or_else(short_id);
    conn.execute(
        "INSERT INTO messages(task_id, sender, recipient, type, state, body, created_at) \
         VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![task_id, alias, to, mtype, state, body, now_ms()],
    )?;
    let id = conn.last_insert_rowid();
    mark_seen(conn, alias)?;
    touch_doorbell(conn, to, alias);
    Ok(json!({"ok": true, "id": id, "task_id": task_id}))
}

fn tool_poll(conn: &Connection, alias: &str) -> rusqlite::Result<Value> {
    mark_seen(conn, alias)?;
    let last: i64 = conn
        .query_row("SELECT last_id FROM cursors WHERE alias=?1", params![alias], |r| r.get(0))
        .optional()?
        .unwrap_or(0);
    let mut stmt = conn.prepare(
        "SELECT id, task_id, sender, recipient, type, state, body, created_at \
         FROM messages WHERE id>?1 AND (recipient=?2 OR recipient='all') ORDER BY id",
    )?;
    let msgs: Vec<Value> = stmt
        .query_map(params![last, alias], |r| {
            Ok(json!({
                "id": r.get::<_, i64>(0)?,
                "task_id": r.get::<_, String>(1)?,
                "from": r.get::<_, String>(2)?,
                "to": r.get::<_, String>(3)?,
                "type": r.get::<_, String>(4)?,
                "state": r.get::<_, String>(5)?,
                "body": r.get::<_, String>(6)?,
                "at": r.get::<_, i64>(7)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if let Some(lastmsg) = msgs.last() {
        let newlast = lastmsg["id"].as_i64().unwrap();
        conn.execute(
            "INSERT INTO cursors(alias, last_id) VALUES(?1, ?2) \
             ON CONFLICT(alias) DO UPDATE SET last_id=?2",
            params![alias, newlast],
        )?;
        let _ = fs::remove_file(inbox_dir().join(format!("{}.flag", alias)));
    }
    Ok(json!({"ok": true, "count": msgs.len(), "messages": msgs}))
}

fn tool_peers(conn: &Connection) -> rusqlite::Result<Value> {
    let mut stmt = conn.prepare("SELECT alias, card, last_seen FROM peers ORDER BY alias")?;
    let peers: Vec<Value> = stmt
        .query_map([], |r| {
            Ok(json!({
                "alias": r.get::<_, String>(0)?,
                "card": r.get::<_, Option<String>>(1)?,
                "last_seen": r.get::<_, i64>(2)?,
            }))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"ok": true, "peers": peers}))
}

fn call_tool(conn: &Connection, alias: &str, name: &str, args: &Value) -> Value {
    let r = match name {
        "register" => tool_register(conn, alias, args),
        "send" => tool_send(conn, alias, args),
        "poll" => tool_poll(conn, alias),
        "peers" => tool_peers(conn),
        _ => return json!({"ok": false, "error": format!("unknown tool: {}", name)}),
    };
    match r {
        Ok(v) => v,
        Err(e) => json!({"ok": false, "error": e.to_string()}),
    }
}

fn tools_list() -> Value {
    json!([
        {"name":"register","description":"Register/refresh THIS agent (alias from AGENT_BUS_ALIAS) in the bus. Call once at session start.",
         "inputSchema":{"type":"object","properties":{"card":{"type":"string","description":"optional capability blurb (Agent Card)"}}}},
        {"name":"send","description":"Send a message/task to another agent by alias (or to:'all'). type='task' for work requests (gets task_id + lifecycle state).",
         "inputSchema":{"type":"object","properties":{
            "to":{"type":"string","description":"recipient alias, or 'all'"},
            "body":{"type":"string","description":"message text or JSON payload"},
            "type":{"type":"string","enum":["task","status","message"]},
            "state":{"type":"string","enum":["submitted","working","completed","failed","info"]},
            "task_id":{"type":"string","description":"reuse to update an existing task's status"}},
            "required":["to","body"]}},
        {"name":"poll","description":"Fetch new messages addressed to THIS agent (and broadcasts) since last poll; advances the cursor.",
         "inputSchema":{"type":"object","properties":{}}},
        {"name":"peers","description":"List known agents and their last-seen time + Agent Card.",
         "inputSchema":{"type":"object","properties":{}}}
    ])
}

// ---------------------------------------------------------------- MCP stdio server
fn handle(conn: &Connection, alias: &str, req: &Value) -> Option<Value> {
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
            let out = call_tool(conn, alias, name, &args);
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
    let alias = alias_env();
    let conn = open_db();
    eprintln!("[agent-bus] serve alias={} db={}", alias, db_path().display());
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
        if let Some(resp) = handle(&conn, &alias, &req) {
            let _ = writeln!(out, "{}", resp);
            let _ = out.flush();
        }
    }
}

// ---------------------------------------------------------------- CLI client
fn cli_alias(flags: &HashMap<String, String>) -> String {
    flags.get("as").cloned().unwrap_or_else(|| {
        std::env::var("AGENT_BUS_ALIAS").unwrap_or_else(|_| "cli".into())
    })
}

fn cli_send(flags: &HashMap<String, String>) {
    let conn = open_db();
    let alias = cli_alias(flags);
    let mut args = json!({});
    if let Some(v) = flags.get("to") { args["to"] = json!(v); }
    if let Some(v) = flags.get("body") { args["body"] = json!(v); }
    if let Some(v) = flags.get("type") { args["type"] = json!(v); }
    if let Some(v) = flags.get("state") { args["state"] = json!(v); }
    if let Some(v) = flags.get("task-id") { args["task_id"] = json!(v); }
    println!("{}", call_tool(&conn, &alias, "send", &args));
}
fn cli_poll(flags: &HashMap<String, String>) {
    let conn = open_db();
    println!("{}", call_tool(&conn, &cli_alias(flags), "poll", &json!({})));
}
fn cli_peers() {
    let conn = open_db();
    println!("{}", call_tool(&conn, "cli", "peers", &json!({})));
}
fn cli_register(flags: &HashMap<String, String>) {
    let conn = open_db();
    let mut args = json!({});
    if let Some(v) = flags.get("card") { args["card"] = json!(v); }
    println!("{}", call_tool(&conn, &cli_alias(flags), "register", &args));
}

// ---------------------------------------------------------------- installer
fn prompt(label: &str, default: &str) -> String {
    print!("{} [{}]: ", label, default);
    io::stdout().flush().ok();
    let mut s = String::new();
    io::stdin().read_line(&mut s).ok();
    let s = s.trim();
    if s.is_empty() { default.to_string() } else { s.to_string() }
}

fn bootstrap_block(alias: &str) -> String {
    let inbox = inbox_dir().join(format!("{}.flag", alias));
    format!(
        "<!-- agent-bus-bootstrap -->\n\
## agent-bus (cross-session coordination)\n\
This repo has the `agent-bus` MCP server (alias `{a}`). At session start:\n\
1. Call `mcp__agent-bus__register` with a short capability card.\n\
2. Arm a doorbell so inbound mail wakes you:\n\
```\n\
Monitor(persistent:true, timeout_ms:3600000, command: |\n\
  last=\"\"; while true; do\n\
    if [ -f {inbox} ]; then m=$(stat -f %m {inbox} 2>/dev/null);\n\
      if [ \"$m\" != \"$last\" ]; then echo \"BUS: new mail for {a} — call agent-bus poll()\"; last=\"$m\"; fi; fi;\n\
    sleep 2; done)\n\
```\n\
3. On a `BUS:` ping, call `mcp__agent-bus__poll` and act on the messages.\n\
4. Reply with `mcp__agent-bus__send` (to=\"sync\", etc.).\n\
<!-- /agent-bus-bootstrap -->\n",
        a = alias,
        inbox = inbox.display()
    )
}

fn install(flags: &HashMap<String, String>) {
    let tool = flags.get("tool").cloned().unwrap_or_else(|| prompt("Tool (claude/codex/copilot)", "claude"));
    let alias = flags.get("alias").cloned().unwrap_or_else(|| prompt("Alias", "agent"));
    let exe = std::env::current_exe()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap_or_else(|_| "agent-bus".into());

    match tool.as_str() {
        "claude" => {
            let repo = flags.get("repo").cloned().unwrap_or_else(|| prompt("Target repo path", "."));
            install_claude(&repo, &alias, &exe);
        }
        "codex" => install_codex(&alias, &exe),
        "copilot" => print_copilot(&alias, &exe),
        other => eprintln!("unknown tool: {} (use claude|codex|copilot)", other),
    }
}

fn install_claude(repo: &str, alias: &str, exe: &str) {
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
        "command": exe, "args": ["serve"], "env": {"AGENT_BUS_ALIAS": alias}
    });
    fs::write(&mcp_path, serde_json::to_string_pretty(&root).unwrap() + "\n").expect("write .mcp.json");
    println!("✓ wrote {}", mcp_path.display());

    let cm = PathBuf::from(repo).join("CLAUDE.md");
    let existing = fs::read_to_string(&cm).unwrap_or_default();
    if existing.contains("<!-- agent-bus-bootstrap -->") {
        println!("• CLAUDE.md already has the agent-bus bootstrap");
    } else {
        let mut f = fs::OpenOptions::new().create(true).append(true).open(&cm).expect("open CLAUDE.md");
        write!(f, "\n{}\n", bootstrap_block(alias)).expect("append CLAUDE.md");
        println!("✓ appended bootstrap to {}", cm.display());
    }
    println!("→ restart the Claude Code session in {} and approve the agent-bus MCP prompt", repo);
}

fn install_codex(alias: &str, exe: &str) {
    let path = home_dir().join(".codex").join("config.toml");
    fs::create_dir_all(path.parent().unwrap()).ok();
    let existing = fs::read_to_string(&path).unwrap_or_default();
    if existing.contains("[mcp_servers.agent-bus]") {
        println!("• {} already has [mcp_servers.agent-bus]", path.display());
        return;
    }
    let block = format!(
        "\n[mcp_servers.agent-bus]\ncommand = \"{}\"\nargs = [\"serve\"]\nenv = {{ AGENT_BUS_ALIAS = \"{}\" }}\n",
        exe, alias
    );
    let mut f = fs::OpenOptions::new().create(true).append(true).open(&path).expect("open config.toml");
    write!(f, "{}", block).expect("append config.toml");
    println!("✓ added [mcp_servers.agent-bus] to {}", path.display());
    println!("→ restart Codex to load it");
}

fn print_copilot(alias: &str, exe: &str) {
    println!("Add this MCP server to Copilot's config (alias {}):", alias);
    println!("  command: {}", exe);
    println!("  args:    [\"serve\"]");
    println!("  env:     AGENT_BUS_ALIAS={}", alias);
    println!("Then call poll() at the start of each turn (Copilot has no background watcher).");
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

fn usage() {
    eprintln!(
        "agent-bus <command>\n\
  serve                      run the MCP stdio server (used by MCP configs)\n\
  install [--tool t --alias a --repo p]   interactive installer\n\
  send --to X --body Y [--type task] [--state s] [--task-id id] [--as alias]\n\
  poll [--as alias]\n\
  peers\n\
  register [--card ..] [--as alias]"
    );
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
        "peers" => cli_peers(),
        "register" => cli_register(&flags),
        _ => usage(),
    }
}

// ---------------------------------------------------------------- tests
#[cfg(test)]
mod tests {
    use super::*;

    fn mem() -> Connection {
        let c = Connection::open_in_memory().unwrap();
        init_schema(&c);
        c
    }

    #[test]
    fn task_roundtrip_and_lifecycle() {
        let c = mem();
        // sync sends a task to classic
        let r = tool_send(&c, "sync", &json!({"to":"classic","type":"task","body":"execute Phase 7"})).unwrap();
        assert!(r["ok"].as_bool().unwrap());
        let task_id = r["task_id"].as_str().unwrap().to_string();

        // classic polls (registered late / never) -> receives it
        let r = tool_poll(&c, "classic").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["task_id"].as_str().unwrap(), task_id);

        // re-poll -> drained
        assert_eq!(tool_poll(&c, "classic").unwrap()["count"].as_i64().unwrap(), 0);

        // classic replies completed on the same task_id; sync polls
        tool_send(&c, "classic", &json!({"to":"sync","type":"status","state":"completed","task_id":task_id,"body":"255 green"})).unwrap();
        let r = tool_poll(&c, "sync").unwrap();
        assert_eq!(r["count"].as_i64().unwrap(), 1);
        assert_eq!(r["messages"][0]["state"].as_str().unwrap(), "completed");
        assert_eq!(r["messages"][0]["task_id"].as_str().unwrap(), task_id);
    }

    #[test]
    fn peers_and_broadcast() {
        let c = mem();
        tool_register(&c, "sync", &json!({"card":"planning"})).unwrap();
        tool_register(&c, "classic", &json!({"card":"rust"})).unwrap();
        let r = tool_peers(&c).unwrap();
        let aliases: Vec<&str> = r["peers"].as_array().unwrap().iter().map(|p| p["alias"].as_str().unwrap()).collect();
        assert!(aliases.contains(&"sync") && aliases.contains(&"classic"));

        tool_send(&c, "sync", &json!({"to":"all","body":"hello all"})).unwrap();
        let r = tool_poll(&c, "classic").unwrap();
        assert!(r["messages"].as_array().unwrap().iter().any(|m| m["body"] == "hello all"));
    }

    #[test]
    fn missing_to_is_soft_error() {
        let c = mem();
        let r = tool_send(&c, "sync", &json!({"body":"no recipient"})).unwrap();
        assert!(!r["ok"].as_bool().unwrap());
    }

    #[test]
    fn mcp_initialize_handshake() {
        let c = mem();
        let resp = handle(&c, "sync", &json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18"}})).unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], "agent-bus");
        assert_eq!(resp["result"]["protocolVersion"], "2025-06-18");
        // notification -> no response
        assert!(handle(&c, "sync", &json!({"jsonrpc":"2.0","method":"notifications/initialized"})).is_none());
        // tools/list
        let resp = handle(&c, "sync", &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"})).unwrap();
        assert_eq!(resp["result"]["tools"].as_array().unwrap().len(), 4);
    }
}
