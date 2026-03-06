#!/usr/bin/env python3
import asyncio
import json
import os
import sys
from contextlib import nullcontext
from typing import Any

from python_runner_common import (
    collect_python_sources,
    import_file,
    normalize_file_list,
    purge_local_modules,
    python_version,
    resolve_module_info,
)


def to_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, list):
        return [to_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [to_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): to_json_value(val) for key, val in value.items()}
    if hasattr(value, "model_dump"):
        return to_json_value(value.model_dump())
    if hasattr(value, "dict"):
        return to_json_value(value.dict())
    if hasattr(value, "__dict__"):
        result: dict[str, Any] = {}
        for key, val in vars(value).items():
            if key.startswith("_"):
                continue
            result[key] = to_json_value(val)
        return result
    return str(value)


def load_framework_globals() -> tuple[Any, Any, Any]:
    try:
        from braintrust.framework2.global_ import functions, prompts
    except Exception:
        from braintrust.framework2 import global_ as global_state

        functions = getattr(global_state, "functions", [])
        prompts = getattr(global_state, "prompts", [])

    lazy = None
    try:
        from braintrust.framework2.lazy_load import _set_lazy_load as lazy
    except Exception:
        try:
            from braintrust.framework import _set_lazy_load as lazy
        except Exception:
            lazy = None

    return functions, prompts, lazy


def normalize_project_selector(project: Any) -> tuple[str | None, str | None]:
    if project is None:
        return None, None

    if isinstance(project, str):
        trimmed = project.strip()
        if trimmed:
            return None, trimmed
        return None, None

    if isinstance(project, dict):
        project_id = project.get("project_id")
        project_name = project.get("project_name")
        if isinstance(project_id, str) and project_id.strip():
            return project_id.strip(), None
        if isinstance(project_name, str) and project_name.strip():
            return None, project_name.strip()
        return None, None

    project_id = getattr(project, "project_id", None)
    project_name = getattr(project, "project_name", None)
    if isinstance(project_id, str) and project_id.strip():
        return project_id.strip(), None
    if isinstance(project_name, str) and project_name.strip():
        return None, project_name.strip()
    return None, None


def normalize_function_type(raw: Any) -> str | None:
    if isinstance(raw, str):
        value = raw.strip()
        return value if value else None

    value_attr = getattr(raw, "value", None)
    if isinstance(value_attr, str):
        value = value_attr.strip()
        return value if value else None

    name_attr = getattr(raw, "name", None)
    if isinstance(name_attr, str):
        value = name_attr.strip().lower()
        return value if value else None

    return None


def selector_to_project_placeholder(project: Any) -> str:
    project_id, project_name = normalize_project_selector(project)
    if project_id:
        return project_id
    if project_name:
        return f"name:{project_name}"
    return ""


class Resolver:
    async def resolve(self, project: Any) -> str:
        return selector_to_project_placeholder(project)


def clear_registry(registry: Any) -> None:
    if hasattr(registry, "clear"):
        registry.clear()


def collect_code_entries(functions_registry: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    items = functions_registry if isinstance(functions_registry, list) else list(functions_registry)
    for index, item in enumerate(items):
        name = getattr(item, "name", None)
        slug = getattr(item, "slug", None)
        if not isinstance(name, str) or not isinstance(slug, str) or not name or not slug:
            continue

        project_id, project_name = normalize_project_selector(getattr(item, "project", None))

        entry: dict[str, Any] = {
            "kind": "code",
            "name": name,
            "slug": slug,
            "location": {"type": "function", "index": index},
        }
        description = getattr(item, "description", None)
        if isinstance(description, str):
            entry["description"] = description
        function_type = (
            getattr(item, "type", None)
            or getattr(item, "function_type", None)
            or getattr(item, "type_", None)
        )
        normalized_function_type = normalize_function_type(function_type)
        if normalized_function_type:
            entry["function_type"] = normalized_function_type
        if_exists = getattr(item, "if_exists", None) or getattr(item, "ifExists", None)
        if isinstance(if_exists, str):
            entry["if_exists"] = if_exists
        metadata = getattr(item, "metadata", None)
        if metadata is not None:
            entry["metadata"] = to_json_value(metadata)
        if project_id:
            entry["project_id"] = project_id
        if project_name:
            entry["project_name"] = project_name

        preview = getattr(item, "preview", None)
        if isinstance(preview, str):
            entry["preview"] = preview

        entries.append(entry)
    return entries


def collect_legacy_prompt_event(item: Any, resolver: Resolver) -> dict[str, Any] | None:
    name = getattr(item, "name", None)
    slug = getattr(item, "slug", None)
    if not isinstance(name, str) or not isinstance(slug, str) or not name or not slug:
        return None

    prompt = to_json_value(getattr(item, "prompt", {}) or {})
    if not isinstance(prompt, dict):
        prompt = {}

    tool_functions = getattr(item, "tool_functions", None)
    if isinstance(tool_functions, list) and tool_functions:
        resolved_tools: list[Any] = []
        for tool in tool_functions:
            if isinstance(tool, dict):
                slug_value = tool.get("slug")
                project = tool.get("project")
                if isinstance(slug_value, str) and project is not None:
                    placeholder = selector_to_project_placeholder(project)
                    if placeholder:
                        resolved_tools.append(
                            {"type": "slug", "project_id": placeholder, "slug": slug_value}
                        )
                        continue
                resolved_tools.append(to_json_value(tool))
            else:
                resolved_tools.append(to_json_value(tool))
        if resolved_tools:
            prompt["tool_functions"] = resolved_tools

    event: dict[str, Any] = {
        "name": name,
        "slug": slug,
        "description": getattr(item, "description", "") or "",
        "function_data": {"type": "prompt"},
        "prompt_data": prompt,
    }

    if_exists = getattr(item, "if_exists", None) or getattr(item, "ifExists", None)
    if isinstance(if_exists, str):
        event["if_exists"] = if_exists
    metadata = getattr(item, "metadata", None)
    if metadata is not None:
        event["metadata"] = to_json_value(metadata)

    project_id, project_name = normalize_project_selector(getattr(item, "project", None))
    out: dict[str, Any] = {"kind": "function_event", "event": event}
    if project_id:
        out["project_id"] = project_id
    if project_name:
        out["project_name"] = project_name
    return out


async def collect_function_event_entries(prompts_registry: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    resolver = Resolver()
    items = prompts_registry if isinstance(prompts_registry, list) else list(prompts_registry)
    for item in items:
        to_definition = getattr(item, "to_function_definition", None)
        if callable(to_definition):
            definition = to_definition(resolver)
            if asyncio.iscoroutine(definition):
                definition = await definition
            normalized = to_json_value(definition)
            if isinstance(normalized, dict):
                project_id, project_name = normalize_project_selector(getattr(item, "project", None))
                event_entry: dict[str, Any] = {"kind": "function_event", "event": normalized}
                if project_id:
                    event_entry["project_id"] = project_id
                if project_name:
                    event_entry["project_name"] = project_name
                entries.append(event_entry)
            continue

        legacy = collect_legacy_prompt_event(item, resolver)
        if legacy is not None:
            entries.append(legacy)

    return entries


async def process_file(file_path: str) -> dict[str, Any]:
    abs_path = os.path.abspath(file_path)
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    functions_registry, prompts_registry, lazy_loader = load_framework_globals()
    clear_registry(functions_registry)
    clear_registry(prompts_registry)
    purge_local_modules(cwd, preserve_modules={__name__, "python_runner_common"})

    module_name, extra_paths = resolve_module_info(abs_path)
    lazy_ctx = lazy_loader(True) if callable(lazy_loader) else nullcontext()
    with lazy_ctx:
        import_file(module_name, abs_path, extra_paths)
        code_entries = collect_code_entries(functions_registry)
        event_entries = await collect_function_event_entries(prompts_registry)
        entries = [*code_entries, *event_entries]
        file_manifest: dict[str, Any] = {
            "source_file": abs_path,
            "entries": entries,
        }
        if code_entries:
            file_manifest["python_bundle"] = {
                "entry_module": module_name,
                "sources": collect_python_sources(cwd, abs_path),
            }

    clear_registry(functions_registry)
    clear_registry(prompts_registry)
    return file_manifest


async def main() -> None:
    files = normalize_file_list(sys.argv[1:])
    if not files:
        raise RuntimeError("functions-runner.py requires at least one input file")

    manifest: dict[str, Any] = {
        "runtime_context": {"runtime": "python", "version": python_version()},
        "files": [],
    }
    for file_path in files:
        manifest["files"].append(await process_file(file_path))

    sys.stdout.write(json.dumps(manifest))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        sys.stderr.write(f"{exc}\n")
        sys.exit(1)
