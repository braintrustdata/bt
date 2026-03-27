# Braintrust URL Formats

## App Links (Current Format)

### Experiments

`https://www.braintrust.dev/app/{org}/p/{project}/experiments/{experiment_name}?r={root_span_id}&s={span_id}`

### Datasets

`https://www.braintrust.dev/app/{org}/p/{project}/datasets/{dataset_name}?r={root_span_id}`

### Project Logs

`https://www.braintrust.dev/app/{org}/p/{project}/logs?r={root_span_id}&s={span_id}`

## Legacy Object URLs

`https://www.braintrust.dev/app/object?object_type=...&object_id=...&id=...`

## URL Parameters

| Parameter | Description                                               |
| --------- | --------------------------------------------------------- |
| r         | The root_span_id - identifies a trace                     |
| s         | The span_id - identifies a specific span within the trace |
| id        | Legacy parameter for root_span_id in object URLs          |

## Notes

- The `r=` parameter is always the root_span_id
- For logs and experiments, use `s=` to reference a specific span within a trace
