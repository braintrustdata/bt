export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export type JsonObject = { [key: string]: JsonValue };

export type InlineAttachment = {
  type: "inline_attachment";
  src: string;
  content_type: string;
  filename?: string;
  data?: unknown;
};

export type SpanFieldName = "input" | "output" | "expected" | "metadata";

export type SpanFields = {
  input?: unknown;
  output?: unknown;
  expected?: unknown;
  metadata?: Record<string, unknown>;
};

export type TraceViewSpan<
  TInput = unknown,
  TOutput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  id: string;
  span_id: string;
  root_span_id: string;
  parent_span_id?: string | null;
  span_parents?: string[];
  data: {
    input?: TInput;
    output?: TOutput;
    expected?: TExpected;
    metadata?: TMetadata;
    scores?: Record<string, number>;
    metrics?: Record<string, number | string>;
    error?: string;
    tags?: string[];
    span_attributes?: Record<string, unknown>;
    [key: string]: unknown;
  };
  children: string[];
};

export type TraceViewTrace = {
  rootSpanId: string;
  selectedSpanId: string;
  spanOrder: string[];
  spans: Record<string, TraceViewSpan>;
  fetchSpanFields: (
    spanIds: string | string[],
    fields?: SpanFieldName[],
  ) => Promise<Record<string, SpanFields>>;
};

export type TraceViewUpdate<
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  (field: string, value: unknown): void;
  (patch: Partial<{ metadata: TMetadata; tags: string[] }>): void;
};

export type TraceViewProps<
  TInput = unknown,
  TOutput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  trace: TraceViewTrace;
  span: TraceViewSpan<TInput, TOutput, TExpected, TMetadata>;
  update: TraceViewUpdate<TMetadata>;
  selectSpan: (spanId: string) => void;
};

export type DatasetViewProps<
  TInput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  id: string;
  input: TInput;
  expected: TExpected;
  metadata: TMetadata;
  tags: string[];
};

export type ReactComponent<Props> = (props: Props) => unknown;

type ProjectRef = string | { id: string } | { name: string };
type DatasetRef = { id: string } | { name: string };

export type CustomTraceViewDefinition<
  TInput = unknown,
  TOutput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  name: string;
  slug: string;
  component: ReactComponent<
    TraceViewProps<TInput, TOutput, TExpected, TMetadata>
  >;
  description?: string;
  project?: ProjectRef;
  tags?: string[];
  metadata?: JsonObject;
};

export type CustomDatasetViewDefinition<
  TInput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
> = {
  name: string;
  slug: string;
  dataset: DatasetRef;
  component: ReactComponent<DatasetViewProps<TInput, TExpected, TMetadata>>;
  description?: string;
  project?: ProjectRef;
  tags?: string[];
  metadata?: JsonObject;
};

export type RegisteredCustomView = {
  kind: "trace" | "dataset";
  name: string;
  slug: string;
  component: ReactComponent<any>;
  description?: string;
  project?: ProjectRef;
  dataset?: DatasetRef;
  tags?: string[];
  metadata?: JsonObject;
};

declare global {
  // eslint-disable-next-line no-var
  var __braintrust_custom_views: RegisteredCustomView[] | undefined;
}

function registry(): RegisteredCustomView[] {
  const current = globalThis.__braintrust_custom_views;
  if (Array.isArray(current)) {
    return current;
  }
  globalThis.__braintrust_custom_views = [];
  return globalThis.__braintrust_custom_views;
}

function registerView(view: RegisteredCustomView): void {
  const views = registry();
  const existing = views.findIndex(
    (candidate) =>
      candidate.kind === view.kind && candidate.slug === view.slug,
  );
  if (existing >= 0) {
    views.splice(existing, 1);
  }
  views.push(view);
}

export function __getCustomViews(): RegisteredCustomView[] {
  return [...registry()];
}

export function __clearCustomViews(): void {
  registry().splice(0);
}

export function customTraceView<
  TInput = unknown,
  TOutput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
>(
  definition: CustomTraceViewDefinition<
    TInput,
    TOutput,
    TExpected,
    TMetadata
  >,
): ReactComponent<TraceViewProps<TInput, TOutput, TExpected, TMetadata>> {
  registerView({
    kind: "trace",
    name: definition.name,
    slug: definition.slug,
    component: definition.component,
    description: definition.description,
    project: definition.project,
    tags: definition.tags,
    metadata: definition.metadata,
  });
  return definition.component;
}

export function customDatasetView<
  TInput = unknown,
  TExpected = unknown,
  TMetadata extends Record<string, unknown> = Record<string, unknown>,
>(
  definition: CustomDatasetViewDefinition<TInput, TExpected, TMetadata>,
): ReactComponent<DatasetViewProps<TInput, TExpected, TMetadata>> {
  registerView({
    kind: "dataset",
    name: definition.name,
    slug: definition.slug,
    component: definition.component,
    description: definition.description,
    project: definition.project,
    dataset: definition.dataset,
    tags: definition.tags,
    metadata: definition.metadata,
  });
  return definition.component;
}

export function isInlineAttachment(value: unknown): value is InlineAttachment {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as { type?: unknown }).type === "inline_attachment" &&
    typeof (value as { src?: unknown }).src === "string" &&
    typeof (value as { content_type?: unknown }).content_type === "string"
  );
}

export function parseMaybeJson(value: unknown): unknown {
  if (typeof value !== "string") {
    return value;
  }
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}

export function formatJson(value: unknown): string {
  return JSON.stringify(value, null, 2);
}
