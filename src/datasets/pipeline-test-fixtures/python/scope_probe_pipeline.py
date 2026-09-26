"""Reports the exact transform arg set the runner hands it.

`__SCOPE__` is replaced with the scope under test, so both scopes assert the
transform-args contract with the same pipeline.
"""

from braintrust import DatasetPipeline


def transform(**kwargs):
    return {
        "input": {
            "args": sorted(kwargs),
            "span_input": kwargs.get("input"),
            "root_span_id": kwargs["trace"].get_configuration()["root_span_id"],
        }
    }


DatasetPipeline(
    name="py-scope-smoke",
    source={"project_name": "test-project", "scope": "__SCOPE__"},
    target={"project_name": "test-target-project", "dataset_name": "test-dataset"},
    transform=transform,
)
