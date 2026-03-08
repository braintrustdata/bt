import asyncio

from braintrust import Eval


def data():
    return [
        {"input": "hello", "expected": "hello"},
        {"input": "world", "expected": "world"},
    ]


async def task(value, hooks):
    await asyncio.sleep(0.01)
    return value


def exact_match(output, expected):
    return int(output == expected)


Eval(
    "cli-streaming",
    data=data,
    task=task,
    scores=[exact_match],
)
