from braintrust import Eval
from pydantic import BaseModel, Field


class ThresholdParams(BaseModel):
    cluster_min_impression_share: float = Field(
        default=0.05,
        description="Fail clusters below this impression share.",
    )
    unassigned_max_impression_share: float = Field(
        default=0.15,
        description="Fail rows above this unassigned impression share.",
    )


def data():
    return [{"input": "test", "expected": "test"}]


def task(input, hooks):
    return input


Eval(
    "test-cli-python-remote-list-params",
    parameters={"thresholds": ThresholdParams},
    data=data,
    task=task,
    scores=[],
)
