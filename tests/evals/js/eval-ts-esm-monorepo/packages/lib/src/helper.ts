// Extensionless import — this is the pattern that breaks without the fix.
// Node's ESM resolver requires explicit extensions, so `./utils` (without .ts)
// fails unless a resolve hook adds the extension.
import { greet } from "./utils";

export function greetAll(names: string[]): string[] {
  return names.map(greet);
}
