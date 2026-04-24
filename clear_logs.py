"""
clear_logs.py  –  Erase all log files and optionally reset chat.txt

Usage:
  python clear_logs.py           # clears logs only
  python clear_logs.py --all     # clears logs AND resets chat.txt to empty
"""

import os
import sys

LOG_FILES  = ["server.log", "node1.log", "node2.log", "node3.log", "node4.log", "all_nodes.log"]
CHAT_FILE  = "chat.txt"


def clear_file(path: str):
    if os.path.exists(path):
        open(path, "w").close()
        print(f"  Cleared : {path}")
    else:
        print(f"  Skipped : {path}  (not found)")


def main():
    reset_chat = "--all" in sys.argv

    print("=== Clearing log files ===")
    for f in LOG_FILES:
        clear_file(f)

    if reset_chat:
        print("\n=== Resetting chat file ===")
        clear_file(CHAT_FILE)

    print("\nDone.")


if __name__ == "__main__":
    main()
