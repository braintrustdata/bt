from braintrust import Eval


def data():
    return [{"input": "test", "expected": "test"}]


def task(input, hooks):
    params = hooks.parameters
    if params["model"] != "gpt-4o":
        raise ValueError(f'Expected model "gpt-4o", got {params["model"]!r}')
    if params["count"] != 5:
        raise ValueError(f"Expected count 5, got {params['count']!r}")
    if params["enabled"] is not True:
        raise ValueError(f"Expected enabled True, got {params['enabled']!r}")
    return input


Eval(
    "test-params-kv",
    parameters={"model": None, "count": None, "enabled": None},
    data=data,
    task=task,
    scores=[],
)
