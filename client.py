"""
client.py  –  Distributed Chat Room  (User Node)

Usage:
  python client.py <node_id>

  <node_id>  must match a key in config.json  (e.g. 1 or 2)

Commands (interactive prompt):
  view          – display the full contents of the shared chat file
  post <text>   – append a timestamped entry to the shared chat file
                  (uses Ricart-Agrawala DME to ensure mutual exclusion)
  quit / exit   – shut down this node

Architecture
------------
  - This module is the APPLICATION layer.
  - Distributed Mutual Exclusion is handled entirely by dme.py (RicartAgrawala).
  - The server (server.py) stores the shared file; it does NOT participate in DME.
  - Only "post" requires the CS; "view" is read-only and runs without DME.

Log files
---------
  node<id>.log  –  all events logged by this node (in addition to stdout)
"""

import socket
import json
import logging
import sys
import os
from datetime import datetime

from dme import RicartAgrawala   # DME middleware (separate module as required)

# ── Configuration ─────────────────────────────────────────────────────────────

CONFIG_FILE = "config.json"
RECV_BUFFER = 4096


def load_config():
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


# ── Logging ───────────────────────────────────────────────────────────────────

class RemoteLogHandler(logging.Handler):
    """Sends each log record to the server's log collector as a JSON message."""

    def __init__(self, host: str, port: int, node_id: int, node_name: str):
        super().__init__()
        self.host      = host
        self.port      = port
        self.node_id   = node_id
        self.node_name = node_name

    def emit(self, record: logging.LogRecord):
        try:
            from datetime import datetime
            ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
            payload = json.dumps({
                "node_id":   self.node_id,
                "node_name": self.node_name,
                "level":     record.levelname,
                "timestamp": ts,
                "message":   record.getMessage(),
            }).encode("utf-8")
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(3)
                s.connect((self.host, self.port))
                s.sendall(payload)
        except Exception:
            pass   # never let a logging failure crash the node


def setup_logger(node_id: int, cfg: dict, node_name: str) -> logging.Logger:
    log_file = f"node{node_id}.log"
    logger = logging.getLogger(f"NODE{node_id}")
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        f"%(asctime)s [Node {node_id}] %(levelname)s  %(message)s"
    )

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(log_file, mode="a")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Remote handler — ships logs to server's all_nodes.log
    srv_host = cfg["server"]["host"]
    log_port = int(cfg["server"].get("log_port", 5010))
    rh = RemoteLogHandler(srv_host, log_port, node_id, node_name)
    logger.addHandler(rh)

    return logger


# ── Server communication ──────────────────────────────────────────────────────

def server_request(cfg: dict, payload: dict) -> dict:
    """Send one JSON request to the file server and return its response."""
    host = cfg["server"]["host"]
    port = int(cfg["server"]["port"])

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(10)
        s.connect((host, port))
        s.sendall(json.dumps(payload).encode("utf-8"))
        s.shutdown(socket.SHUT_WR)   # signal end-of-request

        data = b""
        while True:
            chunk = s.recv(RECV_BUFFER)
            if not chunk:
                break
            data += chunk

    return json.loads(data.decode("utf-8"))


# ── Command handlers ──────────────────────────────────────────────────────────

def cmd_view(cfg: dict, logger: logging.Logger):
    """View chat file — no DME required (read-only operation)."""
    logger.info("Sending VIEW request to server")
    try:
        resp = server_request(cfg, {"cmd": "view"})
        if resp.get("status") == "ok":
            content = resp.get("content", "")
            print("\n" + ("─" * 50))
            if content.strip():
                print(content, end="")
            else:
                print("(chat file is empty)")
            print("─" * 50 + "\n")
        else:
            logger.error(f"VIEW failed: {resp.get('message')}")
            print(f"[ERROR] {resp.get('message')}")
    except Exception as e:
        logger.error(f"Cannot reach server: {e}")
        print(f"[ERROR] Server unreachable — make sure server.py is running.")


def cmd_post(cfg: dict, dme: RicartAgrawala,
             node_id: int, node_name: str,
             text: str, logger: logging.Logger):
    """
    Post a message — acquires CS via Ricart-Agrawala DME before writing.

    Steps:
      1. request_cs()  →  blocks until all peers grant access
      2. Send POST to file server
      3. release_cs()  →  allows waiting peers to proceed
    """
    logger.info(f"Requesting CS to post: '{text}'")

    # ── Step 1: acquire CS ────────────────────────────────────────────────────
    dme.request_cs()

    # ── Step 2: write to server ───────────────────────────────────────────────
    timestamp = datetime.now().strftime("%d %b %I:%M%p")   # e.g. 24 Apr 09:01AM
    payload = {
        "cmd":            "post",
        "node_name":      node_name,
        "node_id":        node_id,
        "user_timestamp": timestamp,
        "text":           text,
    }
    logger.info(f"Inside CS — sending POST to server: '{timestamp} {node_name}: {text}'")

    try:
        resp = server_request(cfg, payload)
        if resp.get("status") == "ok":
            logger.info("POST acknowledged by server")
            print(f"[Posted] {timestamp} {node_name}: {text}")
        else:
            logger.error(f"POST failed: {resp.get('message')}")
            print(f"[ERROR] {resp.get('message')}")
    except Exception as e:
        logger.error(f"Cannot reach server: {e}")
        print(f"[ERROR] Server unreachable — make sure server.py is running.")
    finally:
        # ── Step 3: release CS regardless of server outcome ───────────────────
        dme.release_cs()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <node_id>")
        sys.exit(1)

    node_id = int(sys.argv[1])
    cfg     = load_config()

    if str(node_id) not in cfg["nodes"]:
        print(f"[ERROR] Node ID {node_id} not found in {CONFIG_FILE}")
        sys.exit(1)

    node_name = cfg["nodes"][str(node_id)]["name"]
    logger    = setup_logger(node_id, cfg, node_name)

    logger.info(f"Starting chat node: id={node_id}, name={node_name}")

    # ── Start DME middleware ──────────────────────────────────────────────────
    dme = RicartAgrawala(node_id, cfg, logger=logger)
    dme.start()

    logger.info(
        f"Node {node_id} ({node_name}) ready.  "
        f"Commands: view | post <text> | quit"
    )
    print(f"\nChat node {node_id} ({node_name}) is running.")
    print("Commands:  view  |  post <message>  |  quit\n")

    # ── Interactive command loop ──────────────────────────────────────────────
    while True:
        try:
            line = input(f"{node_name}> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            break

        if not line:
            continue

        parts = line.split(None, 1)   # split into at most 2 parts
        cmd   = parts[0].lower()

        if cmd == "view":
            cmd_view(cfg, logger)

        elif cmd == "post":
            if len(parts) < 2 or not parts[1].strip():
                print("[ERROR] Usage: post <message text>")
            else:
                cmd_post(cfg, dme, node_id, node_name, parts[1].strip(), logger)

        elif cmd in ("quit", "exit"):
            logger.info(f"Node {node_id} ({node_name}) shutting down.")
            break

        else:
            print(f"Unknown command '{cmd}'.  Commands: view | post <text> | quit")

    logger.info("Node stopped.")


if __name__ == "__main__":
    main()
