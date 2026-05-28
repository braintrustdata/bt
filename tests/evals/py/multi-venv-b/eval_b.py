import sys

assert sys.version_info[:2] == (3, 12), (
    f"Expected Python 3.12, got {sys.version_info[:2]}"
)

from braintrust import Eval

Eval(
    "multi-venv-b",
    data=lambda: [{"input": 1, "expected": 1}],
    task=lambda x, *args, **kwargs: x,
    scores=[],
)
