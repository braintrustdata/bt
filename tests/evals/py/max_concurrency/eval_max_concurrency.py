import os
import time

from braintrust import Eval


OUTPUT_PATH = os.environ.get("BT_MAX_CONCURRENCY_TEST_OUT")


def task(name, hooks=None):
    if OUTPUT_PATH:
        with open(OUTPUT_PATH, "a") as output:
            output.write(f"start:{name}\n")
    time.sleep(0.25)
    if OUTPUT_PATH:
        with open(OUTPUT_PATH, "a") as output:
            output.write(f"end:{name}\n")
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
