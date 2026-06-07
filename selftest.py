#!/usr/bin/env python3
"""End-to-end self-test: drives agent_bus.py over the real MCP stdio protocol,
two simulated sessions sharing one temp DB. Exits non-zero on failure."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(HERE, "agent_bus.py")


def run(alias, db, *calls):
    """Run one stdio session: initialize, then each tools/call; return parsed results."""
    lines = ['{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}',
             '{"jsonrpc":"2.0","method":"notifications/initialized"}']
    for i, (name, args) in enumerate(calls, start=2):
        lines.append(json.dumps({"jsonrpc": "2.0", "id": i, "method": "tools/call",
                                 "params": {"name": name, "arguments": args}}))
    env = dict(os.environ, AGENT_BUS_ALIAS=alias, AGENT_BUS_DB=db)
    p = subprocess.run([sys.executable, SERVER], input="\n".join(lines) + "\n",
                       capture_output=True, text=True, env=env, timeout=15)
    out = []
    for ln in p.stdout.splitlines():
        msg = json.loads(ln)
        if msg.get("id", 0) >= 2 and "result" in msg:
            out.append(json.loads(msg["result"]["content"][0]["text"]))
    return out


def check(cond, label):
    print(("PASS" if cond else "FAIL") + " - " + label)
    if not cond:
        sys.exit(1)


def main():
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "bus.db")
        # sync registers and sends a task to classic
        r = run("sync", db, ("register", {"card": "planning"}),
                ("send", {"to": "classic", "type": "task", "body": "execute Phase 7"}))
        check(r[0]["ok"], "sync register")
        task_id = r[1]["task_id"]
        check(bool(task_id), "send returns task_id")

        # classic registers LATE and polls -> must receive the pre-registration task
        r = run("classic", db, ("register", {"card": "rust"}), ("poll", {}))
        msgs = r[1]["messages"]
        check(len(msgs) == 1 and msgs[0]["task_id"] == task_id, "classic receives task sent before it registered")

        # classic re-polls -> cursor drained
        r = run("classic", db, ("poll", {}))
        check(r[0]["count"] == 0, "cursor drains (no re-delivery)")

        # classic replies completed on same task_id; sync polls
        run("classic", db, ("send", {"to": "sync", "type": "status", "state": "completed",
                                      "task_id": task_id, "body": "255 green"}))
        r = run("sync", db, ("poll", {}))
        msgs = r[0]["messages"]
        check(len(msgs) == 1 and msgs[0]["state"] == "completed" and msgs[0]["task_id"] == task_id,
              "sync receives completed status on same task_id (lifecycle)")

        # peers discovery
        r = run("sync", db, ("peers", {}))
        aliases = sorted(p["alias"] for p in r[0]["peers"])
        check(aliases == ["classic", "sync"], "peers lists both agents")

        # broadcast
        run("sync", db, ("send", {"to": "all", "type": "message", "body": "hello all"}))
        r = run("classic", db, ("poll", {}))
        check(any(m["body"] == "hello all" for m in r[0]["messages"]), "broadcast reaches classic")

    print("\nALL PASS")


if __name__ == "__main__":
    main()
