# Braintrust URL Formats

## App Links (Current Format)

### Experiments

`https://BRAINTRUST_APP_URL/{org}/p/{project}/experiments/{experiment_name}?r={root_span_id}&s={span_id}`

### Datasets

`https://BRAINTRUST_APP_URL/{org}/p/{project}/datasets/{dataset_name}?r={root_span_id}`

### Project Logs

`https://BRAINTRUST_APP_URL/{org}/p/{project}/logs?r={root_span_id}&s={span_id}`

## Legacy Object URLs

`https://BRAINTRUST_APP_URL/object?object_type=...&object_id=...&id=...`

## URL Parameters

| Parameter          | Description                                                                                                                                                                      |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| r                  | The root_span_id - identifies a trace                                                                                                                                            |
| s                  | The span_id - identifies a specific span within the trace                                                                                                                        |
| id                 | Legacy parameter for root_span_id in object URLs                                                                                                                                 |
| BRAINTRUST_APP_URL | url where the app is accessible, www.braintrust.dev/app for non self-hosted instances. See the env variable BRAINTRUST_APP_URL (and use www.braintrust.dev/app if it is not set) |

## Notes

- The `r=` parameter is always the root_span_id
- For logs and experiments, use `s=` to reference a specific span within a trace
