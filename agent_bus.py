#!/usr/bin/env python3
"""
agent_bus — minimal A2A-shaped message broker as an MCP server (stdio, stdlib only).

POC: no daemon, no port. Every CLI session launches its own copy over stdio; they all
share ONE SQLite file (WAL mode handles concurrency). The session's identity is its
alias, set via the AGENT_BUS_ALIAS env var (sync / classic / client / ...).

Messages are shaped like A2A tasks (task_id + state lifecycle) so the wire can grow
into the real A2A standard later without a schema rewrite. South side = MCP (what every
CLI speaks today). Doorbell = a per-recipient flag file touched on send, so an external
watcher (Claude Monitor, fswatch) can wake an idle session.

Tools: register, send, poll, peers.
"""
import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("AGENT_BUS_DB", os.path.join(HERE, "bus.db"))
INBOX_DIR = os.path.join(HERE, "inbox")
ALIAS = os.environ.get("AGENT_BUS_ALIAS", "unknown")
SERVER_VERSION = "0.1.0"
DEFAULT_PROTOCOL = "2025-06-18"


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def log(*a):
    # logs MUST go to stderr — stdout is the JSON-RPC channel
    print("[agent_bus]", *a, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- storage
def db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con


def init_db():
    os.makedirs(INBOX_DIR, exist_ok=True)
    con = db()
    con.executescript(
        """
        CREATE TABLE IF NOT EXISTS messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id    TEXT,
            sender     TEXT,
            recipient  TEXT,        -- alias, or 'all' for broadcast
            type       TEXT,        -- task | status | message
            state      TEXT,        -- submitted | working | completed | failed | info
            body       TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS cursors (
            alias   TEXT PRIMARY KEY,
            last_id INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS peers (
            alias     TEXT PRIMARY KEY,
            card      TEXT,
            last_seen TEXT
        );
        """
    )
    con.commit()
    con.close()


def touch_doorbell(recipient):
    """Write a per-recipient flag file (count of pending) so a watcher can wake them."""
    try:
        targets = []
        if recipient == "all":
            con = db()
            targets = [r[0] for r in con.execute("SELECT alias FROM peers").fetchall()]
            con.close()
        else:
            targets = [recipient]
        for t in targets:
            if t == ALIAS:
                continue
            flag = os.path.join(INBOX_DIR, f"{t}.flag")
            with open(flag, "w") as f:
                f.write(now_iso() + "\n")
    except Exception as e:  # doorbell is best-effort
        log("doorbell error:", e)


def mark_seen(alias):
    con = db()
    con.execute(
        "INSERT INTO peers(alias, last_seen) VALUES(?,?) "
        "ON CONFLICT(alias) DO UPDATE SET last_seen=excluded.last_seen",
        (alias, now_iso()),
    )
    con.commit()
    con.close()


# ---------------------------------------------------------------- tools
def tool_register(args):
    card = args.get("card")
    con = db()
    con.execute(
        "INSERT INTO peers(alias, card, last_seen) VALUES(?,?,?) "
        "ON CONFLICT(alias) DO UPDATE SET card=COALESCE(excluded.card, peers.card), "
        "last_seen=excluded.last_seen",
        (ALIAS, card, now_iso()),
    )
    # NOTE: do NOT seed the delivery cursor here — a missing cursor means "deliver all
    # pending mail addressed to me" (poll defaults last_id=0). The cursor only advances
    # on poll, and persists across restarts, so no re-delivery and no missed pre-registration mail.
    con.commit()
    con.close()
    return {"ok": True, "alias": ALIAS}


def tool_send(args):
    to = args.get("to")
    if not to:
        return {"ok": False, "error": "missing 'to'"}
    body = args.get("body", "")
    mtype = args.get("type", "message")
    state = args.get("state", "submitted" if mtype == "task" else "info")
    task_id = args.get("task_id") or str(uuid.uuid4())[:8]
    con = db()
    cur = con.execute(
        "INSERT INTO messages(task_id, sender, recipient, type, state, body, created_at) "
        "VALUES(?,?,?,?,?,?,?)",
        (task_id, ALIAS, to, mtype, state, body, now_iso()),
    )
    mid = cur.lastrowid
    con.commit()
    con.close()
    mark_seen(ALIAS)
    touch_doorbell(to)
    return {"ok": True, "id": mid, "task_id": task_id}


def tool_poll(args):
    mark_seen(ALIAS)
    con = db()
    row = con.execute("SELECT last_id FROM cursors WHERE alias=?", (ALIAS,)).fetchone()
    last = row[0] if row else 0
    rows = con.execute(
        "SELECT id, task_id, sender, recipient, type, state, body, created_at "
        "FROM messages WHERE id>? AND (recipient=? OR recipient='all') ORDER BY id",
        (last, ALIAS),
    ).fetchall()
    msgs = [
        {
            "id": r[0], "task_id": r[1], "from": r[2], "to": r[3],
            "type": r[4], "state": r[5], "body": r[6], "at": r[7],
        }
        for r in rows
    ]
    if msgs:
        newlast = msgs[-1]["id"]
        con.execute(
            "INSERT INTO cursors(alias, last_id) VALUES(?,?) "
            "ON CONFLICT(alias) DO UPDATE SET last_id=excluded.last_id",
            (ALIAS, newlast),
        )
        con.commit()
        # clear our doorbell flag now that we've drained
        try:
            os.remove(os.path.join(INBOX_DIR, f"{ALIAS}.flag"))
        except OSError:
            pass
    con.close()
    return {"ok": True, "count": len(msgs), "messages": msgs}


def tool_peers(args):
    con = db()
    rows = con.execute("SELECT alias, card, last_seen FROM peers ORDER BY alias").fetchall()
    con.close()
    return {"ok": True, "peers": [{"alias": r[0], "card": r[1], "last_seen": r[2]} for r in rows]}


TOOLS = {
    "register": (
        tool_register,
        "Register/refresh THIS agent (alias from AGENT_BUS_ALIAS env) in the bus. Call once at session start.",
        {
            "type": "object",
            "properties": {"card": {"type": "string", "description": "optional capability blurb (Agent Card)"}},
        },
    ),
    "send": (
        tool_send,
        "Send a message/task to another agent by alias. type='task' for work requests (gets a task_id + lifecycle state), else 'message'. recipient 'all' broadcasts.",
        {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "recipient alias, or 'all'"},
                "body": {"type": "string", "description": "message text or JSON payload"},
                "type": {"type": "string", "enum": ["task", "status", "message"]},
                "state": {"type": "string", "enum": ["submitted", "working", "completed", "failed", "info"]},
                "task_id": {"type": "string", "description": "reuse to update an existing task's status"},
            },
            "required": ["to", "body"],
        },
    ),
    "poll": (
        tool_poll,
        "Fetch new messages addressed to THIS agent (and broadcasts) since last poll; advances the cursor.",
        {"type": "object", "properties": {}},
    ),
    "peers": (
        tool_peers,
        "List known agents and their last-seen time + Agent Card.",
        {"type": "object", "properties": {}},
    ),
}


# ---------------------------------------------------------------- MCP stdio
def reply(rid, result=None, error=None):
    msg = {"jsonrpc": "2.0", "id": rid}
    if error is not None:
        msg["error"] = error
    else:
        msg["result"] = result
    sys.stdout.write(json.dumps(msg) + "\n")
    sys.stdout.flush()


def handle(req):
    method = req.get("method")
    rid = req.get("id")
    params = req.get("params") or {}

    if method == "initialize":
        proto = params.get("protocolVersion", DEFAULT_PROTOCOL)
        reply(rid, {
            "protocolVersion": proto,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "agent-bus", "version": SERVER_VERSION},
        })
        return
    if method in ("notifications/initialized", "initialized"):
        return  # notification, no response
    if method == "ping":
        reply(rid, {})
        return
    if method == "tools/list":
        tools = [
            {"name": n, "description": d, "inputSchema": s}
            for n, (_, d, s) in TOOLS.items()
        ]
        reply(rid, {"tools": tools})
        return
    if method == "tools/call":
        name = params.get("name")
        args = params.get("arguments") or {}
        entry = TOOLS.get(name)
        if not entry:
            reply(rid, error={"code": -32602, "message": f"unknown tool: {name}"})
            return
        try:
            out = entry[0](args)
            reply(rid, {"content": [{"type": "text", "text": json.dumps(out)}], "isError": not out.get("ok", True)})
        except Exception as e:
            log("tool error:", e)
            reply(rid, {"content": [{"type": "text", "text": json.dumps({"ok": False, "error": str(e)})}], "isError": True})
        return
    if rid is not None:
        reply(rid, error={"code": -32601, "message": f"method not found: {method}"})


def main():
    init_db()
    log(f"started alias={ALIAS} db={DB_PATH}")
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        try:
            handle(req)
        except Exception as e:
            log("handler crash:", e)


if __name__ == "__main__":
    main()
