from braintrust import Eval


def data_a():
    return [{"input": "a", "expected": "a"}]


def task_a(input, hooks):
    params = hooks.parameters
    if params["model"] != "gpt-4o":
        raise ValueError(f'A: expected model "gpt-4o", got {params["model"]!r}')
    if params["count"] != 5:
        raise ValueError(f"A: expected count 5, got {params['count']!r}")
    if "enabled" in params:
        raise ValueError(f"A: unexpected param 'enabled' leaked in: {params!r}")
    return input


Eval(
    "test-params-multi-a",
    parameters={"model": None, "count": None},
    data=data_a,
    task=task_a,
    scores=[],
)


def data_b():
    return [{"input": "b", "expected": "b"}]


def task_b(input, hooks):
    params = hooks.parameters
    if params["enabled"] is not True:
        raise ValueError(f"B: expected enabled True, got {params['enabled']!r}")
    if "model" in params or "count" in params:
        raise ValueError(f"B: unexpected params leaked in: {params!r}")
    return input


Eval(
    "test-params-multi-b",
    parameters={"enabled": None},
    data=data_b,
    task=task_b,
    scores=[],
)
