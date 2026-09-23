import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

const html = readFileSync(
  new URL("./custom-views-preview.html", import.meta.url),
  "utf8",
);
const source = html.match(/<script type="module">([\s\S]*?)<\/script>/)[1];

async function preview(kind, data, component = "() => null") {
  const helper = kind === "trace" ? "customTraceView" : "customDatasetView";
  const view = `import React from ${JSON.stringify(import.meta.resolve("react"))};
    import { ${helper} } from ${JSON.stringify(import.meta.resolve("braintrust/custom-views"))};
    export default ${helper}({ name: "Test View", slug: "test-view", dataset: { name: "test-dataset" } }, ${component});`;
  let props;
  const requests = [];
  const run = new Function(
    "React",
    "ReactDOM",
    "document",
    "fetch",
    source
      .replace(
        "__PREVIEW_CONFIG__",
        JSON.stringify({
          title: "Test preview",
          viewType: kind,
          token: "test-token",
          sourceModuleUrl: `data:text/javascript,${encodeURIComponent(view)}`,
        }),
      )
      .replace("main().catch(showError);", "return main();"),
  );
  await run(
    { createElement: (component, props) => ({ component, props }) },
    {
      createRoot: () => ({
        render: (element) => {
          props = element.props;
        },
      }),
    },
    { getElementById: () => ({}) },
    async (url, options) => {
      assert.equal(options.headers["x-bt-preview-token"], "test-token");
      requests.push({ url, body: JSON.parse(options.body) });
      return { ok: true, json: async () => structuredClone(data) };
    },
  );
  return { props: () => props, requests };
}

test("released trace view supports async updates, targets, selection, and legacy update", async () => {
  const root = {
    id: "row-root",
    span_id: "root",
    root_span_id: "root",
    children: ["child"],
    data: { metadata: { preserved: true }, scores: { existing: 1 } },
  };
  const child = {
    id: "row-child",
    span_id: "child",
    root_span_id: "root",
    children: [],
    data: {},
  };
  const p = await preview("trace", {
    trace: {
      rootSpanId: "root",
      selectedSpanId: "child",
      spanOrder: ["root", "child"],
      spans: { root, child },
    },
    span: child,
  });
  assert.deepEqual(
    await p.props().trace.update({ metadata: { reviewed: true } }),
    { transactionId: null },
  );
  assert.equal(p.props().span.data.metadata.reviewed, true);
  await p.props().trace.update({
    target: "root",
    metadata: { reviewed: true },
    scores: { quality: 0.5 },
    tags: ["reviewed"],
  });
  assert.deepEqual(p.props().trace.spans.root.data, {
    metadata: { preserved: true, reviewed: true },
    scores: { existing: 1, quality: 0.5 },
    tags: ["reviewed"],
  });
  assert.equal(p.props().span.span_id, "child");
  await p.props().trace.update({ target: { spanId: "root" }, tags: null });
  p.props().selectSpan("root");
  assert.deepEqual(p.props().span.data.tags, []);
  p.props().update("legacy", "value");
  assert.equal(p.props().span.data.metadata.legacy, "value");
  await assert.rejects(
    p.props().trace.update({ target: { spanId: "missing" }, tags: [] }),
    /not found/,
  );
  assert.equal(p.requests.length, 1, "editing must not write to the server");
  await p.props().trace.fetchSpanFields("root", ["input", "metadata"]);
  assert.deepEqual(p.requests[1], {
    url: "/span-fields",
    body: { spanIds: ["root"], fields: ["input", "metadata"] },
  });
});

test("released dataset view updates fields locally and preserves untouched data", async () => {
  const p = await preview("dataset", {
    props: {
      id: "test-row",
      input: "before",
      expected: "before",
      metadata: { preserved: true },
      tags: ["before"],
    },
  });
  assert.deepEqual(
    await p.props().update({
      input: { message: "after" },
      expected: null,
      metadata: { reviewed: true },
      tags: null,
    }),
    { transactionId: null },
  );
  const { update, ...data } = p.props();
  assert.equal(typeof update, "function");
  assert.deepEqual(data, {
    id: "test-row",
    input: { message: "after" },
    expected: null,
    metadata: { preserved: true, reviewed: true },
    tags: [],
  });
  assert.equal(p.requests.length, 1, "editing must not write to the server");
});

for (const component of [
  "React.memo(() => null)",
  "React.forwardRef(() => null)",
  "React.lazy(() => Promise.resolve({ default: () => null }))",
]) {
  test(`preview accepts ${component}`, async () => {
    const p = await preview(
      "dataset",
      { props: { id: "test-row" } },
      component,
    );
    assert.equal(p.props().id, "test-row");
  });
}

test("preview rejects objects that are not React component types", async () => {
  await assert.rejects(
    preview("dataset", { props: { id: "test-row" } }, "{}"),
    /component must be a React component/,
  );
});
