from braintrust import Eval


def data():
    return [
        {"input": 1, "expected": 1},
        {"input": 2, "expected": 2},
    ]


def task(value, hooks=None):
    return value


Eval(
    "cli-basic",
    data=data,
    task=task,
    scores=[],
)
