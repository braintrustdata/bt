import { Eval } from "braintrust";
import { createRequire } from "module";
import path from "path";
import { pathToFileURL } from "url";

type EvaluatorEntry = {
  evaluator: {
    evalName: string;
    projectName: string;
  } & Record<string, unknown>;
  reporter?: unknown;
};

function normalizeFiles(files: string[]): string[] {
  return files.map((file) => path.resolve(process.cwd(), file));
}

function initRegistry() {
  (globalThis as any)._evals = {
    functions: [],
    prompts: [],
    parameters: [],
    evaluators: {},
    reporters: {},
  };
  (globalThis as any)._lazy_load = true;
}

async function loadFiles(files: string[]) {
  const require = createRequire(import.meta.url);
  for (const file of files) {
    const fileUrl = pathToFileURL(file).href;
    try {
      await import(fileUrl);
    } catch (err) {
      if (shouldTryRequire(file, err)) {
        try {
          require(file);
          continue;
        } catch (requireErr) {
          throw new Error(
            `Failed to load ${file} as ESM (${formatError(err)}) or CJS (${formatError(requireErr)}).`,
          );
        }
      }
      throw err;
    }
  }
}

function shouldTryRequire(file: string, err: unknown): boolean {
  if (process.env.BT_EVAL_CJS === "1" || file.endsWith(".cjs")) {
    return true;
  }
  if (!(err instanceof Error)) {
    return false;
  }
  const message = err.message || "";
  return (
    message.includes("require is not defined") ||
    message.includes("exports is not defined") ||
    message.includes("module is not defined") ||
    message.includes("Cannot use import statement outside a module")
  );
}

function formatError(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

function getEvaluators(): EvaluatorEntry[] {
  const evals = (globalThis as any)._evals;
  if (!evals || !evals.evaluators) {
    return [];
  }
  return Object.values(evals.evaluators) as EvaluatorEntry[];
}

async function runEvals(evaluators: EvaluatorEntry[]) {
  (globalThis as any)._lazy_load = false;

  let ok = true;
  for (const entry of evaluators) {
    try {
      await Eval(entry.evaluator.projectName, entry.evaluator as any);
    } catch (err) {
      ok = false;
      console.error(err);
    }
  }

  if (!ok) {
    process.exitCode = 1;
  }
}

async function main() {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    console.error("No eval files provided.");
    process.exit(1);
  }

  const normalized = normalizeFiles(files);
  initRegistry();
  await loadFiles(normalized);
  const evaluators = getEvaluators();

  if (evaluators.length === 0) {
    console.error("No evaluators found. Did you call Eval() in the file?");
    process.exit(1);
  }

  await runEvals(evaluators);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
