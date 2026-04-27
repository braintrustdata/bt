from braintrust import Eval


def data():
    return [{"input": "test", "expected": "test"}]


def task(input, hooks):
    params = hooks.parameters
    if params["model"] != "gpt-4o":
        raise ValueError(f'Expected model "gpt-4o", got {params["model"]!r}')
    if params["count"] != 5:
        raise ValueError(f"Expected count 5, got {params['count']!r}")
    return input


Eval(
    "test-params-json-obj",
    parameters={"model": None, "count": None},
    data=data,
    task=task,
    scores=[],
)
