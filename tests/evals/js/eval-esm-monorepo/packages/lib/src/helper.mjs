import { greet } from "./utils.mjs";

export function greetAll(names) {
  return names.map(greet);
}
