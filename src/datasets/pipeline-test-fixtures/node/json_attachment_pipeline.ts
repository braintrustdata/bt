import { DatasetPipeline, JSONAttachment } from "braintrust";

export default DatasetPipeline({
  name: "ts-json-attachment-smoke",
  source: { projectName: "source-project", scope: "span" },
  target: { projectName: "target-project", datasetName: "traces" },
  transform: (args) => {
    if (args.id !== "source-row") {
      throw new Error(`expected source row id, got ${args.id}`);
    }
    return {
      id: undefined,
      origin: { object_type: "dataset", object_id: "wrong", id: "wrong" },
      input: {
        source_id: args.id,
        source_input: args.input,
        full_trace: new JSONAttachment(
          { ok: true, root: args.trace.getConfiguration().root_span_id },
          { filename: "trace.json", pretty: true },
        ),
      },
    };
  },
});
