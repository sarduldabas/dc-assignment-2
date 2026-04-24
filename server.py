"""
server.py  –  Shared File Server (Node 0 / Server Node)

Responsibilities:
  - Maintain the shared chat file (chat.txt) as a persistent resource.
  - Respond to two commands from any user node:
      view  -> return full contents of chat.txt
      post  -> append a timestamped entry to chat.txt

The server is NOT involved in Distributed Mutual Exclusion (DME).
DME is handled entirely by the user nodes (see dme.py / client.py).
The server-side file_lock is a LOCAL lock only – it guards against
concurrent I/O from two simultaneous network connections hitting
the same Python process; it is NOT a distributed lock.

Protocol (JSON over TCP):
  Request:  {"cmd": "view"}
            {"cmd": "post", "node_name": "Lucy", "node_id": 1,
             "user_timestamp": "12 Oct 09:01AM", "text": "Hello"}
  Response: {"status": "ok", "content": "..."}   (view)
            {"status": "ok"}                       (post)
            {"status": "error", "message": "..."}

Run:
  python server.py
"""

import socket
import threading
import json
import os
import logging
import sys

# ── Configuration ────────────────────────────────────────────────────────────

CONFIG_FILE  = "config.json"
CHAT_FILE    = "chat.txt"
ALL_LOGS     = "all_nodes.log"
RECV_BUFFER  = 4096

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SERVER] %(levelname)s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("server.log", mode="a"),
    ],
)
logger = logging.getLogger("SERVER")


# ── Load config ───────────────────────────────────────────────────────────────

def load_config():
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)


# ── File helpers ──────────────────────────────────────────────────────────────

file_lock = threading.Lock()   # LOCAL only – protects file I/O within this process


def do_view():
    with file_lock:
        if not os.path.exists(CHAT_FILE):
            return ""
        with open(CHAT_FILE, "r") as f:
            return f.read()


def do_post(node_name, user_timestamp, text):
    entry = f"{user_timestamp} {node_name}: {text}\n"
    with file_lock:
        with open(CHAT_FILE, "a") as f:
            f.write(entry)
    return entry


# ── Request handler ───────────────────────────────────────────────────────────

def recv_all(conn):
    """Read until the client closes its send side."""
    data = b""
    while True:
        chunk = conn.recv(RECV_BUFFER)
        if not chunk:
            break
        data += chunk
    return data


def handle_client(conn, addr):
    try:
        raw = recv_all(conn)
        if not raw:
            return

        request = json.loads(raw.decode("utf-8").strip())
        cmd = request.get("cmd", "")

        if cmd == "view":
            logger.info(f"VIEW  from {addr}")
            content = do_view()
            response = {"status": "ok", "content": content}

        elif cmd == "post":
            node_name      = request.get("node_name", "unknown")
            node_id        = request.get("node_id", "?")
            user_timestamp = request.get("user_timestamp", "")
            text           = request.get("text", "")
            entry = do_post(node_name, user_timestamp, text)
            logger.info(f"POST  from Node {node_id} ({node_name}): {entry.strip()}")
            response = {"status": "ok"}

        else:
            logger.warning(f"Unknown command '{cmd}' from {addr}")
            response = {"status": "error", "message": f"Unknown command: {cmd}"}

        conn.sendall(json.dumps(response).encode("utf-8"))

    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error from {addr}: {e}")
        try:
            conn.sendall(json.dumps({"status": "error", "message": "Bad JSON"}).encode())
        except Exception:
            pass
    except Exception as e:
        logger.error(f"Error handling {addr}: {e}", exc_info=True)
    finally:
        conn.close()


# ── Remote log collector ──────────────────────────────────────────────────────

all_logs_lock = threading.Lock()


def handle_log_client(conn):
    """Receive a log line from a node and append it to all_nodes.log."""
    try:
        data = b""
        while True:
            chunk = conn.recv(RECV_BUFFER)
            if not chunk:
                break
            data += chunk
        if data:
            entry = json.loads(data.decode("utf-8"))
            line = (
                f"{entry.get('timestamp', '')}  "
                f"[Node {entry.get('node_id', '?')} – {entry.get('node_name', '?')}]  "
                f"{entry.get('level', 'INFO')}  "
                f"{entry.get('message', '')}\n"
            )
            with all_logs_lock:
                with open(ALL_LOGS, "a") as f:
                    f.write(line)
    except Exception as e:
        logger.error(f"Log collector error: {e}")
    finally:
        conn.close()


def start_log_collector(port: int):
    """Listen on log_port for incoming log lines from all nodes."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", port))
    sock.listen(20)
    logger.info(f"Log collector listening on 0.0.0.0:{port}  -> {ALL_LOGS}")

    def _accept_loop():
        while True:
            try:
                conn, _ = sock.accept()
                threading.Thread(
                    target=handle_log_client, args=(conn,), daemon=True
                ).start()
            except Exception as e:
                logger.error(f"Log collector accept error: {e}")
                break

    threading.Thread(target=_accept_loop, daemon=True).start()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    cfg  = load_config()
    host = cfg["server"]["host"]
    port = int(cfg["server"]["port"])

    # Create chat file if missing
    if not os.path.exists(CHAT_FILE):
        open(CHAT_FILE, "w").close()
        logger.info(f"Created empty chat file: {CHAT_FILE}")

    # Start remote log collector
    log_port = int(cfg["server"].get("log_port", 5010))
    start_log_collector(log_port)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))   # listen on all interfaces
    srv.listen(20)
    logger.info(f"File server listening on 0.0.0.0:{port}  (advertised as {host}:{port})")
    logger.info(f"Shared chat file: {os.path.abspath(CHAT_FILE)}")

    while True:
        conn, addr = srv.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()


if __name__ == "__main__":
    main()
