from braintrust import Eval

import helper


def task(value, hooks=None):
    return value


Eval(
    "cli-local-import",
    data=helper.make_cases,
    task=task,
    scores=[],
)
