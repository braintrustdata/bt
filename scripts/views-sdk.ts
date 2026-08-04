type CustomViewDefinition = Record<string, unknown>;
type Component = (props: unknown) => unknown;

export function customTraceView(
  definition: CustomViewDefinition,
  component: Component,
) {
  return { ...definition, component, kind: "trace" as const };
}

export function customDatasetView(
  definition: CustomViewDefinition,
  component: Component,
) {
  return { ...definition, component, kind: "dataset" as const };
}
