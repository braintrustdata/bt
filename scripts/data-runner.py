#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any

try:
    from braintrust.util import eprint
    from runner_common import call_evaluator_data, load_evaluators, to_async_iterator
except Exception as exc:  # pragma: no cover - runtime guard
    print(
        "Unable to import the braintrust package. Please install it in your Python environment.",
        file=sys.stderr,
    )
    print(str(exc), file=sys.stderr)
    sys.exit(1)


@dataclass
class PullChannel:
    sock: socket.socket

    def send(self, payload: Any) -> None:
        self.sock.sendall((json.dumps(payload) + "\n").encode("utf-8"))

    async def lines(self):
        buffer = ""
        while True:
            chunk = await asyncio.to_thread(self.sock.recv, 4096)
            if not chunk:
                break
            buffer += chunk.decode("utf-8")
            while True:
                newline = buffer.find("\n")
                if newline == -1:
                    break
                line = buffer[:newline].strip()
                buffer = buffer[newline + 1 :]
                if line:
                    yield line

        trailing = buffer.strip()
        if trailing:
            yield trailing

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def create_pull_channel() -> PullChannel:
    sock_path = os.getenv("BT_EVAL_PULL_SOCK")
    if not sock_path:
        raise ValueError("Missing BT_EVAL_PULL_SOCK")

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(sock_path)
    return PullChannel(sock)


def parse_start_request(raw: str) -> str:
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("Start request must be a JSON object.")
    if parsed.get("type") != "start":
        raise ValueError("Expected initial start command.")
    name = parsed.get("name")
    if not isinstance(name, str) or not name:
        raise ValueError("Start request must include a non-empty evaluator name.")
    return name


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stream eval rows over a unix socket for bt.")
    parser.add_argument("files", nargs="*", help="Eval files or directories to load.")
    return parser


async def run(files: list[str]) -> int:
    evaluators, _reporters = load_evaluators(files)
    channel = create_pull_channel()

    try:
        line_iter = channel.lines()
        try:
            start_line = await anext(line_iter)
        except StopAsyncIteration:
            return 0

        try:
            target_name = parse_start_request(start_line)
        except Exception as exc:
            channel.send({"type": "error", "message": str(exc)})
            return 1

        evaluator_instance = next(
            (candidate for candidate in evaluators if candidate.evaluator.eval_name == target_name),
            None,
        )
        if evaluator_instance is None:
            channel.send({"type": "error", "message": f"Evaluator '{target_name}' not found"})
            return 1

        evaluator = evaluator_instance.evaluator
        raw_data, _base_experiment_name = await call_evaluator_data(evaluator.data)
        data_iterator = to_async_iterator(raw_data)
        iterator = data_iterator.__aiter__()

        trial_count = getattr(evaluator, "trial_count", 1)
        try:
            trial_count = int(trial_count)
        except Exception:
            trial_count = 1
        if trial_count < 1:
            trial_count = 1

        max_concurrency = getattr(evaluator, "max_concurrency", None)
        try:
            max_concurrency = int(max_concurrency) if max_concurrency is not None else 10
        except Exception:
            max_concurrency = 10
        if max_concurrency < 1:
            max_concurrency = 1

        experiment_name = getattr(evaluator, "experiment_name", None)
        if not isinstance(experiment_name, str) or not experiment_name:
            experiment_name = f"{evaluator.eval_name}-{int(time.time() * 1000)}"

        channel.send(
            {
                "type": "ready",
                "evaluator_name": evaluator.eval_name,
                "max_concurrency": max_concurrency,
                "experiment_name": experiment_name,
            }
        )

        current_datum = None
        trial_index = 0
        async for line in line_iter:
            parsed = json.loads(line)
            command_type = parsed.get("type") if isinstance(parsed, dict) else None
            if command_type == "close":
                break
            if command_type != "next":
                channel.send(
                    {
                        "type": "error",
                        "message": f"Unsupported pull command '{command_type}'",
                    }
                )
                return 1

            if current_datum is None:
                try:
                    current_datum = await iterator.__anext__()
                    trial_index = 0
                except StopAsyncIteration:
                    channel.send({"type": "eof"})
                    continue

            channel.send(
                {
                    "type": "row",
                    "datum": current_datum,
                    "trial_index": trial_index,
                }
            )
            trial_index += 1
            if trial_index >= trial_count:
                current_datum = None

        return 0
    finally:
        channel.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    files = args.files or ["."]

    try:
        return asyncio.run(run(files))
    except Exception as exc:
        eprint(str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
