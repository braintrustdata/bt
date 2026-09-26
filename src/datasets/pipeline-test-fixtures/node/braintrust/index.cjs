class OriginalJSONAttachment {
  constructor() {
    throw new Error("original JSONAttachment should be shimmed");
  }
}

module.exports = {
  DatasetPipeline(definition) {
    globalThis.__braintrust_dataset_pipelines ??= [];
    globalThis.__braintrust_dataset_pipelines.push({
      ...definition,
      source: {
        ...definition.source,
        scope: definition.source.scope ?? "span",
      },
    });
  },
  LocalTrace: class {
    constructor(options) {
      this.options = options;
    }
    getConfiguration() {
      return { root_span_id: this.options.rootSpanId };
    }
    async getSpans() {
      return [
        {
          id: "source-row",
          span_id: "source-span",
          input: { prompt: "hello" },
          output: { answer: "world" },
          expected: "ok",
          metadata: { topic: "smoke" },
        },
      ];
    }
  },
  _internalGetGlobalState() {
    return {
      loggedIn: true,
      orgName: "source-org",
      login: async function () {
        return this;
      },
    };
  },
  loginToState: async function ({ orgName }) {
    return {
      loggedIn: true,
      orgName,
      login: async function () {
        return this;
      },
    };
  },
  JSONAttachment: OriginalJSONAttachment,
};
