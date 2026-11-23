#!/usr/bin/env python3
"""Docker-Healthcheck: Exit 0, wenn die Heartbeat-Datei (WH_HEARTBEAT_FILE) jünger als
WH_HEARTBEAT_MAX_AGE Sekunden ist (Standard 180)."""
import os
import sys
import time

path = os.environ.get("WH_HEARTBEAT_FILE")
max_age = float(os.environ.get("WH_HEARTBEAT_MAX_AGE", "180"))
try:
    age = time.time() - os.path.getmtime(path)
except (TypeError, OSError):
    print("kein Heartbeat", file=sys.stderr)
    sys.exit(1)
if age > max_age:
    print(f"Heartbeat zu alt: {age:.0f} s", file=sys.stderr)
    sys.exit(1)
