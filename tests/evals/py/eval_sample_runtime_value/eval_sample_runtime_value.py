import builtins

from braintrust import Eval


sample = getattr(builtins, "__bt_eval_sample_rate", None)
if sample != 5:
    raise RuntimeError(f"expected sample runtime value 5, received {sample!r}")


def data():
    return [{"input": "synthetic", "expected": "synthetic"}]


def task(value, hooks=None):
    return value


Eval(
    "cli-python-sample-runtime-value",
    data=data,
    task=task,
    scores=[],
)
