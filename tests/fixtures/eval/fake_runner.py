#!/usr/bin/env python3
"""Replay deterministic eval SSE fixtures for CLI integration tests."""

import json
import os
import socket
import sys
import time
from pathlib import Path


def attempt_number() -> int:
    state_file = os.environ.get("BT_TEST_ATTEMPT_FILE")
    if not state_file:
        return 1
    path = Path(state_file)
    try:
        attempt = int(path.read_text()) + 1
    except (FileNotFoundError, ValueError):
        attempt = 1
    path.write_text(str(attempt))
    return attempt


def connect():
    unix_path = os.environ.get("BT_EVAL_SSE_SOCK")
    if unix_path:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(unix_path)
        return client
    host, port = os.environ["BT_EVAL_SSE_ADDR"].rsplit(":", 1)
    return socket.create_connection((host, int(port)))


def main() -> int:
    fixture = Path(os.environ["BT_TEST_FRAME_SCRIPT"])
    attempt = attempt_number()
    directives = [
        json.loads(line)
        for line in fixture.read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    mode = os.environ.get("BT_EVAL_DEV_MODE")
    request_name = None
    if os.environ.get("BT_EVAL_DEV_REQUEST_JSON"):
        request_name = json.loads(os.environ["BT_EVAL_DEV_REQUEST_JSON"]).get("name")
    selected = [
        d
        for d in directives
        if d.get("attempt", attempt) == attempt
        and d.get("mode", mode) == mode
        and d.get("request", request_name) == request_name
    ]

    with connect() as client:
        stream = client.makefile("w", encoding="utf-8", newline="\n")
        exit_code = 0
        for directive in selected:
            if "exit" in directive:
                exit_code = int(directive["exit"])
                continue
            if "process_stdout" in directive:
                print(directive["process_stdout"], flush=True)
                continue
            if "process_stderr" in directive:
                print(directive["process_stderr"], file=sys.stderr, flush=True)
                continue
            event = directive.get("event")
            if event is None:
                continue
            data = directive.get("data", "")
            encoded = data if isinstance(data, str) else json.dumps(data, separators=(",", ":"))
            stream.write(f"event: {event}\n")
            for line in encoded.splitlines() or [""]:
                stream.write(f"data: {line}\n")
            stream.write("\n")
            stream.flush()
    # Keep the process alive briefly after closing SSE so the CLI drains every
    # queued frame before observing process exit.
    time.sleep(0.05)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
