from braintrust import Eval

from pkg.helpers import cases


def task(value, hooks=None):
    return value


Eval(
    "cli-absolute-import",
    data=cases,
    task=task,
    scores=[],
)
