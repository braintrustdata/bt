export async function load(url, context, nextLoad) {
  const result = await nextLoad(url, context);
  if (!url.endsWith("/esm-instrumentation-target/index.mjs")) {
    return result;
  }

  const source =
    typeof result.source === "string"
      ? result.source
      : new TextDecoder().decode(result.source);
  return {
    ...result,
    source: source.replace(
      "export const autoInstrumented = false;",
      "export const autoInstrumented = true;",
    ),
  };
}
