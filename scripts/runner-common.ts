export type JsonPrimitive = string | number | boolean | null;
export type JsonArray = JsonValue[];
export type JsonObject = { [key: string]: JsonValue };
export type JsonValue = JsonPrimitive | JsonArray | JsonObject;

export type ProjectSelector = {
  project_id?: string;
  project_name?: string;
};

export type ProjectRef = {
  id?: string;
  name?: string;
};

export function asProjectSelector(
  project: ProjectRef | undefined,
): ProjectSelector {
  if (!project) {
    return {};
  }

  if (typeof project.id === "string" && project.id.trim().length > 0) {
    return { project_id: project.id };
  }

  if (typeof project.name === "string" && project.name.trim().length > 0) {
    return { project_name: project.name };
  }

  return {};
}

export function selectorToProjectId(selector: ProjectSelector): string {
  if (
    typeof selector.project_id === "string" &&
    selector.project_id.trim().length > 0
  ) {
    return selector.project_id;
  }

  if (
    typeof selector.project_name === "string" &&
    selector.project_name.trim().length > 0
  ) {
    return `name:${selector.project_name}`;
  }

  return "";
}

export function isJsonObject(
  value: JsonValue | undefined,
): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function toJsonValue(input: JsonValue): JsonValue {
  if (Array.isArray(input)) {
    return input.map((item) => toJsonValue(item));
  }

  if (input !== null && typeof input === "object") {
    const out: JsonObject = {};
    for (const [key, value] of Object.entries(input)) {
      if (
        value === null ||
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      ) {
        out[key] = value;
      } else if (Array.isArray(value)) {
        out[key] = value.map((entry) => toJsonValue(entry));
      } else if (typeof value === "object") {
        out[key] = toJsonValue(value as JsonObject);
      }
    }
    return out;
  }

  return input;
}
