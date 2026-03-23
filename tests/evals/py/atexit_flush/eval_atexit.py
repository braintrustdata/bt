import asyncio
import atexit
import os
import time

from braintrust import Eval

MARKER_FILE = os.environ.get("ATEXIT_MARKER_FILE", "")


def _write_marker():
    if MARKER_FILE:
        time.sleep(1)
        with open(MARKER_FILE, "w") as f:
            f.write("exited")


atexit.register(_write_marker)


def data():
    return [
        {"input": "hello", "expected": "hello"},
        {"input": "world", "expected": "world"},
    ]


async def task(value, hooks):
    await asyncio.sleep(0.01)
    return value


def exact_match(output, expected):
    return int(output == expected)


Eval(
    "cli-atexit-flush",
    data=data,
    task=task,
    scores=[exact_match],
)
