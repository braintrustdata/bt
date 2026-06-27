import os
import threading
import time
from pathlib import Path

from braintrust import Eval, Score


def log_event(event, index):
    out_file = os.environ["BT_SYNC_TASK_PARALLEL_OUT"]
    with Path(out_file).open("a") as f:
        f.write(f"{time.time():.6f} {event} {index} {threading.get_ident()}\n")


def task(input):
    index = input["index"]
    log_event("start", index)
    time.sleep(float(os.getenv("BT_SYNC_TASK_SLEEP_S", "0.5")))
    log_event("end", index)
    return {"ok": True}


def score(input, output, expected):
    return Score(name="ok", score=1.0)


Eval(
    "sync-task-parallelization",
    data=[{"input": {"index": index}, "expected": {}} for index in range(4)],
    task=task,
    scores=[score],
    experiment_name="sync-task-parallelization",
    max_concurrency=4,
)
