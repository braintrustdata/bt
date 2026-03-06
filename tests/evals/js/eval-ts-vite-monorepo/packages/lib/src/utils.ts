// this forces vite only
const prefix = import.meta.env.VITE_GREETING_PREFIX ?? "Hello";

export function greet(name: string): string {
  return `${prefix} ${name}`;
}
