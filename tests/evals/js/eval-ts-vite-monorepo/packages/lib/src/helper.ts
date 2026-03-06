import { greet } from "./utils";

export function greetAll(names) {
  return names.map(greet);
}
