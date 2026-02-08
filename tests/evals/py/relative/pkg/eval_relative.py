from braintrust import Eval

from .helpers import build_cases


def task(value, hooks=None):
    return value


Eval(
    "cli-relative-import",
    data=build_cases,
    task=task,
    scores=[],
)
