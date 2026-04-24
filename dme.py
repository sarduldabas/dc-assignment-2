"""
dme.py  –  Ricart-Agrawala Distributed Mutual Exclusion (DME) Module

Algorithm: Non-token, assertion-based (Ricart-Agrawala, 1981)
Reference:  DC-07 lecture slides (CS ZG526, BITS Pilani WILP)

Key properties:
  - Uses Lamport logical clocks for timestamping CS requests.
  - Messages: REQUEST(ts, id) and REPLY(ts, id) only — 2(N-1) msgs per CS entry.
  - Requires FIFO channels (satisfied by TCP).
  - Priority rule: lower (timestamp, node_id) = higher priority.
    When process pi (WANTED or HELD) receives REQUEST from pj:
      if (pi.request_ts, pi.node_id) < (msg_ts, sender_id)  →  defer REPLY
      else                                                    →  send REPLY immediately

States (from DC-07 slides):
  RELEASED  – not interested in CS
  WANTED    – has broadcast REQUEST, waiting for all REPLYs
  HELD      – inside critical section

Public API (called by client.py):
  dme = RicartAgrawala(node_id, config)
  dme.start()        – start background listener thread
  dme.request_cs()   – block until CS is granted
  dme.release_cs()   – exit CS, send any deferred REPLYs
"""

import socket
import threading
import json
import logging
import sys

# ── States ────────────────────────────────────────────────────────────────────

RELEASED = "RELEASED"
WANTED   = "WANTED"
HELD     = "HELD"

# ── Message types ─────────────────────────────────────────────────────────────

MSG_REQUEST = "REQUEST"
MSG_REPLY   = "REPLY"

RECV_BUFFER = 4096


class RicartAgrawala:
    """
    Ricart-Agrawala DME middleware.

    Parameters
    ----------
    node_id : int
        This node's ID (1-based, must match config.json).
    config  : dict
        Parsed config.json content.
    logger  : logging.Logger, optional
        If provided, DME events are written to the caller's logger.
    """

    def __init__(self, node_id: int, config: dict, logger=None):
        self.node_id   = node_id
        self.config    = config
        self.peers     = self._build_peer_table()   # {peer_id: (host, dme_port)}
        self.N         = len(self.peers)             # number of OTHER nodes

        # Lamport logical clock
        self._clock     = 0
        self._clock_lock = threading.Lock()

        # DME state
        self._state         = RELEASED
        self._state_lock    = threading.Lock()
        self._request_ts    = 0          # clock value when last REQUEST was sent
        self._deferred      = []         # peer_ids whose REPLY we have deferred
        self._replies_needed = 0         # replies still awaited before entering CS
        self._reply_event   = threading.Event()   # set when _replies_needed == 0

        # Logging
        self._log = logger or logging.getLogger(f"DME.Node{node_id}")

        # Listener socket
        self._server_sock = None

    # ── Peer table ────────────────────────────────────────────────────────────

    def _build_peer_table(self):
        peers = {}
        for nid_str, info in self.config["nodes"].items():
            nid = int(nid_str)
            if nid != self.node_id:
                peers[nid] = (info["host"], int(info["dme_port"]))
        return peers

    # ── Lamport clock helpers ─────────────────────────────────────────────────

    def _tick(self):
        """R1: increment clock before any event."""
        with self._clock_lock:
            self._clock += 1
            return self._clock

    def _update(self, received_ts: int):
        """R2: on receive, sync clock then tick."""
        with self._clock_lock:
            self._clock = max(self._clock, received_ts) + 1
            return self._clock

    def current_clock(self):
        with self._clock_lock:
            return self._clock

    # ── Networking helpers ────────────────────────────────────────────────────

    def _send_message(self, peer_id: int, msg: dict) -> bool:
        """Open a fresh TCP connection to peer and send one JSON message.
        Returns True on success, False if peer is unreachable."""
        host, port = self.peers[peer_id]
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((host, port))
                s.sendall(json.dumps(msg).encode("utf-8"))
            return True
        except Exception as e:
            self._log.warning(
                f"[DME] Failed to send {msg['type']} to Node {peer_id} "
                f"at {host}:{port} — {e}"
            )
            return False

    # ── Listener thread ───────────────────────────────────────────────────────

    def start(self):
        """Bind the DME listen port and start the background listener thread."""
        my_info  = self.config["nodes"][str(self.node_id)]
        dme_port = int(my_info["dme_port"])

        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(("0.0.0.0", dme_port))
        self._server_sock.listen(10)

        t = threading.Thread(target=self._listen, daemon=True)
        t.start()
        self._log.info(
            f"[DME] Node {self.node_id} listening for DME messages on port {dme_port}"
        )

    def _listen(self):
        while True:
            try:
                conn, addr = self._server_sock.accept()
                t = threading.Thread(
                    target=self._handle_connection, args=(conn,), daemon=True
                )
                t.start()
            except Exception as e:
                self._log.error(f"[DME] Listener error: {e}")
                break

    def _handle_connection(self, conn):
        try:
            data = b""
            while True:
                chunk = conn.recv(RECV_BUFFER)
                if not chunk:
                    break
                data += chunk
            if data:
                msg = json.loads(data.decode("utf-8").strip())
                self._handle_message(msg)
        except Exception as e:
            self._log.error(f"[DME] Error reading DME message: {e}")
        finally:
            conn.close()

    # ── Core Ricart-Agrawala logic ─────────────────────────────────────────────

    def _handle_message(self, msg: dict):
        msg_type  = msg.get("type")
        msg_ts    = int(msg.get("timestamp", 0))
        sender_id = int(msg.get("node_id", 0))

        self._update(msg_ts)   # R2: update Lamport clock on receive

        if msg_type == MSG_REQUEST:
            self._on_request(msg_ts, sender_id)
        elif msg_type == MSG_REPLY:
            self._on_reply(sender_id)
        else:
            self._log.warning(f"[DME] Unknown message type '{msg_type}' from Node {sender_id}")

    def _on_request(self, msg_ts: int, sender_id: int):
        """
        Ricart-Agrawala REQUEST handler.

        Send REPLY immediately UNLESS this node has higher priority (lower ts,id)
        for an outstanding REQUEST — in that case defer the REPLY until after
        we have used the CS.
        """
        with self._state_lock:
            state = self._state
            own_ts = self._request_ts

        # Decide: defer or reply
        # Own priority is higher when (own_ts, self.node_id) < (msg_ts, sender_id)
        own_priority_higher = (
            state in (WANTED, HELD)
            and (own_ts, self.node_id) < (msg_ts, sender_id)
        )

        if own_priority_higher:
            # Defer REPLY — will be sent when we call release_cs()
            with self._state_lock:
                self._deferred.append(sender_id)
            self._log.info(
                f"[DME] Deferred REPLY to Node {sender_id} "
                f"(their req ts={msg_ts}, my req ts={own_ts}, "
                f"my id={self.node_id} < their id={sender_id})"
            )
        else:
            # Send REPLY immediately
            reply_ts = self._tick()
            reply = {"type": MSG_REPLY, "timestamp": reply_ts, "node_id": self.node_id}
            self._log.info(
                f"[DME] Sending REPLY to Node {sender_id} "
                f"(their req ts={msg_ts}, my state={state})"
            )
            self._send_message(sender_id, reply)

    def _on_reply(self, sender_id: int):
        """
        REPLY received from sender_id.
        Decrement outstanding reply counter; if zero, signal that CS may be entered.
        """
        with self._state_lock:
            self._replies_needed -= 1
            remaining = self._replies_needed

        self._log.info(
            f"[DME] Received REPLY from Node {sender_id} "
            f"(still waiting for {remaining} more)"
        )

        if remaining == 0:
            self._reply_event.set()

    # ── Public API ─────────────────────────────────────────────────────────────

    def request_cs(self):
        """
        Request entry to Critical Section.

        Steps (Ricart-Agrawala):
          1. Set state = WANTED.
          2. Increment Lamport clock (R1) and record request timestamp.
          3. Broadcast REQUEST(ts, id) to all peers.
          4. Block until N-1 REPLYs received.
          5. Set state = HELD and return.
        """
        with self._state_lock:
            self._state          = WANTED
            ts                   = self._tick()
            self._request_ts     = ts
            self._replies_needed = self.N
            self._reply_event.clear()

        self._log.info(
            f"[DME] Node {self.node_id} WANTS CS  "
            f"(Lamport ts={ts}, expecting {self.N} REPLYs)"
        )

        # Broadcast REQUEST to all peers
        # If a peer is unreachable (offline), treat it as an immediate REPLY —
        # an offline node is not competing for the CS.
        req_msg = {"type": MSG_REQUEST, "timestamp": ts, "node_id": self.node_id}
        for peer_id in self.peers:
            self._log.info(f"[DME] Sending REQUEST to Node {peer_id}")
            ok = self._send_message(peer_id, req_msg)
            if not ok:
                self._log.warning(
                    f"[DME] Node {peer_id} unreachable — counting as implicit REPLY"
                )
                self._on_reply(peer_id)

        # Wait until all REPLYs have arrived
        self._reply_event.wait()

        with self._state_lock:
            self._state = HELD

        self._log.info(f"[DME] Node {self.node_id} ENTERED CS (ts={ts})")

    def release_cs(self):
        """
        Release Critical Section.

        Steps (Ricart-Agrawala):
          1. Set state = RELEASED.
          2. Send REPLY to all deferred peers (in order received).
        """
        with self._state_lock:
            self._state      = RELEASED
            deferred         = list(self._deferred)
            self._deferred   = []
            self._request_ts = 0

        self._log.info(
            f"[DME] Node {self.node_id} RELEASED CS  "
            f"(sending deferred REPLYs to {deferred if deferred else 'none'})"
        )

        for peer_id in deferred:
            reply_ts = self._tick()
            reply = {"type": MSG_REPLY, "timestamp": reply_ts, "node_id": self.node_id}
            self._log.info(f"[DME] Sending deferred REPLY to Node {peer_id}")
            self._send_message(peer_id, reply)
