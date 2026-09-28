export function DatasetPipeline(definition) {
  globalThis.__braintrust_dataset_pipelines ??= [];
  globalThis.__braintrust_dataset_pipelines.push({
    ...definition,
    source: {
      ...definition.source,
      scope: definition.source.scope ?? "span",
    },
  });
}

export class JSONAttachment {
  constructor(data, options) {
    const hook = globalThis.__BT_DATASET_PIPELINE_DEFER_JSON_ATTACHMENT__;
    if (hook) {
      return hook(data, options);
    }
    throw new Error("dataset pipeline deferred JSON hook was not installed");
  }
}
