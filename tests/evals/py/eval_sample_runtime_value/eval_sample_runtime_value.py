import builtins

from braintrust import Eval


internal_btql = getattr(builtins, "__bt_eval_internal_btql", None)
if internal_btql != {"sample": 5}:
    raise RuntimeError(
        f"expected internal BTQL runtime value {{'sample': 5}}, received {internal_btql!r}"
    )


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
