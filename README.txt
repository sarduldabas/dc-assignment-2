====================================================================
  Distributed Chat Room — Assignment 2
  CC ZG526  Distributed Computing  (BITS Pilani WILP)
====================================================================

GROUP MEMBERS & ROLES
----------------------
  Sardul   — Node 0  File Server   (runs server.py)
  Tanya    — Node 1  Chat Client   (runs client.py 1)
  Archana  — Node 2  Chat Client   (runs client.py 2)
  Yash     — Node 3  Chat Client   (runs client.py 3)
  Phani    — Node 4  Chat Client   (runs client.py 4)

Distributed Mutual Exclusion (DME) is implemented using the
Ricart-Agrawala algorithm (DC-07 lecture, assertion-based,
non-token, Lamport logical clocks).  Only "post" (write) acquires
the Critical Section; "view" (read) runs without DME.

FILES
-----
  config.json    — all hostnames and ports (ONLY file to edit for lab)
  server.py      — file server (Sardul's machine)
  dme.py         — Ricart-Agrawala DME middleware  [separate module]
  client.py      — interactive chat application    [separate module]
  clear_logs.py  — wipe log files before a fresh demo run
  README.txt     — this file


====================================================================
  PART 1 — LOCAL TESTING (all 5 processes on one machine)
====================================================================

Requirements:  Python 3.8+  (no third-party packages needed)

Step 1 – Ensure config.json has all hosts set to "localhost" (default).

Step 2 – Open FIVE terminal windows in the Assignment 2 directory.

  Terminal 1:  python server.py
  Terminal 2:  python client.py 1      ← Tanya
  Terminal 3:  python client.py 2      ← Archana
  Terminal 4:  python client.py 3      ← Yash
  Terminal 5:  python client.py 4      ← Phani

Step 3 – Try commands in any client terminal:

  Tanya>   post Hello everyone!
  Archana> post Hi Tanya!
  Yash>    view
  Phani>   post Good morning all.

Step 4 – To test mutual exclusion, type "post" in multiple terminals
at nearly the same time.  The logs (node1.log … node4.log) will show
the full Ricart-Agrawala REQUEST / REPLY / CS ENTERED / RELEASED trace.


====================================================================
  PART 2 — LAB MACHINE DEPLOYMENT (5 separate machines)
====================================================================

*** ONLY config.json needs to change — no code changes required ***

Obtain each machine's IP from the lab (example IPs used below):

  Sardul's machine   192.168.1.10   (server)
  Tanya's machine    192.168.1.11   (Node 1)
  Archana's machine  192.168.1.12   (Node 2)
  Yash's machine     192.168.1.13   (Node 3)
  Phani's machine    192.168.1.14   (Node 4)

Edit config.json on ALL FIVE machines (same file on every machine):

  "server": { "host": "192.168.1.10", "port": 5000, "log_port": 5010 },
  "nodes": {
    "1": { "name": "Tanya",   "host": "192.168.1.11", "dme_port": 5001 },
    "2": { "name": "Archana", "host": "192.168.1.12", "dme_port": 5002 },
    "3": { "name": "Yash",    "host": "192.168.1.13", "dme_port": 5003 },
    "4": { "name": "Phani",   "host": "192.168.1.14", "dme_port": 5004 }
  }

Then run (each person on their own machine):

  Sardul:   python server.py
  Tanya:    python client.py 1
  Archana:  python client.py 2
  Yash:     python client.py 3
  Phani:    python client.py 4

Firewall / ports to open (TCP inbound) on each machine:
  Sardul's machine:   5000  (file server)
                      5010  (remote log collector)
  Tanya's machine:    5001  (DME listener)
  Archana's machine:  5002  (DME listener)
  Yash's machine:     5003  (DME listener)
  Phani's machine:    5004  (DME listener)


====================================================================
  PROTOCOL SUMMARY
====================================================================

Client ↔ Server  (JSON/TCP, port 5000):
  {"cmd": "view"}
    → {"status": "ok", "content": "<full chat.txt>"}

  {"cmd": "post", "node_name": "Tanya", "node_id": 1,
   "user_timestamp": "24 Apr 09:01AM", "text": "Hello"}
    → {"status": "ok"}

DME peer messages  (JSON/TCP, ports 5001–5004):
  {"type": "REQUEST", "timestamp": <int>, "node_id": <int>}
  {"type": "REPLY",   "timestamp": <int>, "node_id": <int>}


====================================================================
  LOG FILES
====================================================================

  server.log        — file server events  (Sardul's machine)
  all_nodes.log     — combined log from ALL nodes  (Sardul's machine)
  node1.log         — Tanya's  DME + chat trace    (Tanya's machine)
  node2.log         — Archana's DME + chat trace   (Archana's machine)
  node3.log         — Yash's   DME + chat trace    (Yash's machine)
  node4.log         — Phani's  DME + chat trace    (Phani's machine)

Each log line is written in two places simultaneously:
  - Local file on the node's own machine
  - Shipped over TCP to all_nodes.log on the server (port 5010)

To clear all logs before a fresh demo run:
  python clear_logs.py          # wipes all *.log files
  python clear_logs.py --all    # also resets chat.txt to empty

Lines tagged [DME] show the full Ricart-Agrawala trace:
  WANTS CS -> REQUEST sent to peers -> REPLYs received
  -> ENTERED CS -> POST sent to server -> RELEASED CS
  -> deferred REPLYs sent to any waiting peers
