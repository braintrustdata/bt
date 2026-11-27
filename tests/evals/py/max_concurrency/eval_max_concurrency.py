import os
import threading
import time

from braintrust import Eval


OUTPUT_PATH = os.environ.get("BT_MAX_CONCURRENCY_TEST_OUT")
# Appends from concurrent tasks are not atomic on Windows.
OUTPUT_LOCK = threading.Lock()


def record(event):
    if OUTPUT_PATH:
        with OUTPUT_LOCK, open(OUTPUT_PATH, "a") as output:
            output.write(f"{event}\n")


def task(name, hooks=None):
    record(f"start:{name}")
    time.sleep(0.25)
    record(f"end:{name}")
    return name


def register_evaluator(name):
    Eval(
        "test-max-concurrency",
        data=lambda: [{"input": name}],
        task=task,
        scores=[],
        experiment_name=f"test-{name}",
    )


register_evaluator("alpha")
register_evaluator("beta")
register_evaluator("gamma")
