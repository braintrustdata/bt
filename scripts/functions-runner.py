#!/usr/bin/env python3
import asyncio
import inspect
import json
import os
import re
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
    if hasattr(value, "model_dump") and not isinstance(value, type):
        return to_json_value(value.model_dump())
    if hasattr(value, "dict") and not isinstance(value, type):
        return to_json_value(value.dict())
    if hasattr(value, "__dict__"):
        result: dict[str, Any] = {}
        for key, val in vars(value).items():
            if key.startswith("_"):
                continue
            result[key] = to_json_value(val)
        return result
    return str(value)


def load_framework_globals() -> tuple[Any, Any, Any, Any]:
    # Prefer current SDK layout first:
    # - braintrust.framework2 exposes module-level `global_`
    # - braintrust.framework exposes `_set_lazy_load`
    try:
        from braintrust.framework import _set_lazy_load as lazy
        from braintrust.framework2 import global_ as global_state

        try:
            from braintrust.framework import _evals
        except (ImportError, ModuleNotFoundError):
            _evals = None

        return global_state.functions, global_state.prompts, lazy, _evals
    except (ImportError, ModuleNotFoundError):
        # Backward compatibility with older SDK layout.
        from braintrust.framework2.global_ import functions, prompts
        from braintrust.framework2.lazy_load import _set_lazy_load as lazy

        try:
            from braintrust.framework import _evals
        except (ImportError, ModuleNotFoundError):
            _evals = None

        return functions, prompts, lazy, _evals


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
    # braintrust.framework2.Project exposes `.name`.
    project_display_name = getattr(project, "name", None)
    if isinstance(project_display_name, str) and project_display_name.strip():
        return None, project_display_name.strip()
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


def pydantic_to_json_schema(model: Any) -> Any | None:
    if model is None:
        return None
    if hasattr(model, "model_json_schema"):
        return to_json_value(model.model_json_schema())
    if hasattr(model, "schema"):
        return to_json_value(model.schema())
    return None


def selector_to_project_placeholder(project: Any) -> str:
    project_id, project_name = normalize_project_selector(project)
    if project_id:
        return project_id
    if project_name:
        return f"name:{project_name}"
    return ""


def import_module_name_from_cwd(cwd: str, source_file: str) -> str | None:
    try:
        rel = os.path.relpath(source_file, cwd)
    except ValueError:
        return None

    parent_prefix = os.pardir + os.sep
    if rel == os.pardir or rel.startswith(parent_prefix):
        return None

    module = os.path.splitext(rel)[0]
    module = module.replace("-", "_")
    module = module.replace(os.sep, ".")
    if os.altsep:
        module = module.replace(os.altsep, ".")
    return module


def package_init_sources_for_module(cwd: str, module_name: str) -> list[str]:
    package_parts = [part for part in module_name.split(".")[:-1] if part]
    if not package_parts:
        return []
    sources: list[str] = []
    current = cwd
    for part in package_parts:
        current = os.path.join(current, part)
        init_path = os.path.join(current, "__init__.py")
        if os.path.isfile(init_path):
            sources.append(os.path.abspath(init_path))
    return sources


class Resolver:
    def get(self, project: Any) -> str:
        return selector_to_project_placeholder(project)

    async def resolve(self, project: Any) -> str:
        return self.get(project)


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
        parameters_model = getattr(item, "parameters", None)
        if parameters_model is None:
            raise ValueError(f"Function {name} has no supplied parameters")
        parameters_schema = pydantic_to_json_schema(parameters_model)
        if parameters_schema is None:
            raise ValueError(f"Function {name} has invalid parameters schema")
        function_schema: dict[str, Any] = {"parameters": parameters_schema}
        returns_model = getattr(item, "returns", None)
        if returns_model is not None:
            returns_schema = pydantic_to_json_schema(returns_model)
            if returns_schema is not None:
                function_schema["returns"] = returns_schema
        entry["function_schema"] = function_schema
        if_exists = getattr(item, "if_exists", None)
        if isinstance(if_exists, str):
            entry["if_exists"] = if_exists
        metadata = getattr(item, "metadata", None)
        if metadata is not None:
            entry["metadata"] = to_json_value(metadata)
        tags = getattr(item, "tags", None)
        if isinstance(tags, list):
            normalized_tags = [tag for tag in tags if isinstance(tag, str)]
            if normalized_tags:
                entry["tags"] = normalized_tags
        if project_id:
            entry["project_id"] = project_id
        if project_name:
            entry["project_name"] = project_name

        preview = getattr(item, "preview", None)
        if isinstance(preview, str):
            entry["preview"] = preview

        entries.append(entry)
    return entries


async def collect_function_event_entries(prompts_registry: Any) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    resolver = Resolver()
    items = prompts_registry if isinstance(prompts_registry, list) else list(prompts_registry)
    for item in items:
        to_definition = getattr(item, "to_function_definition", None)
        if callable(to_definition):
            definition: Any
            signature = inspect.signature(to_definition)
            positional_params = [
                parameter
                for parameter in signature.parameters.values()
                if parameter.kind
                in (
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                )
            ]
            if len(positional_params) >= 2:
                definition = to_definition(None, resolver)
            elif len(positional_params) == 1:
                definition = to_definition(resolver)
            else:
                definition = to_definition()
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

    return entries


def slugify(text: str) -> str:
    return re.sub(r"^-|-$", "", re.sub(r"[^a-z0-9]+", "-", text.lower()))


def collect_evaluator_entries(evals_registry: Any, source_file: str) -> list[dict[str, Any]]:
    if evals_registry is None:
        return []

    evaluators = getattr(evals_registry, "evaluators", None)
    if not evaluators or not isinstance(evaluators, dict):
        return []

    entries: list[dict[str, Any]] = []
    stem_base, _ = os.path.splitext(os.path.basename(source_file))
    stem = re.sub(r"\.eval$", "", stem_base)

    for eval_name, instance in evaluators.items():
        if instance is None:
            continue
        evaluator = getattr(instance, "evaluator", None)
        if evaluator is None:
            continue

        project_name = getattr(evaluator, "project_name", None)
        project_id, proj_name = normalize_project_selector(
            {"project_name": project_name} if isinstance(project_name, str) else None
        )

        scores = getattr(evaluator, "scores", []) or []
        score_descriptors = [
            {"name": getattr(score, "__name__", f"scorer_{i}")}
            for i, score in enumerate(scores)
        ]

        evaluator_definition: dict[str, Any] = {"scores": score_descriptors}

        raw_params = getattr(evaluator, "parameters", None)
        if raw_params is not None:
            marker = getattr(raw_params, "__braintrust_parameters_marker", None)
            if marker is True:
                evaluator_definition["parameters"] = {
                    "type": "braintrust.parameters",
                    "schema": getattr(raw_params, "schema", None),
                    "source": {
                        "parametersId": getattr(raw_params, "id", None),
                        "slug": getattr(raw_params, "slug", None),
                        "name": getattr(raw_params, "name", None),
                        "projectId": getattr(raw_params, "projectId", None),
                        "version": getattr(raw_params, "version", None),
                    },
                }
            else:
                # Use the braintrust SDK's parameters_to_json_schema when
                # available so that Pydantic model classes are converted to
                # proper staticParametersSchema entries (type: "data" with a
                # JSON Schema) that the UI can parse.
                try:
                    from braintrust.parameters import parameters_to_json_schema
                    serialized = parameters_to_json_schema(raw_params)
                except Exception:
                    serialized = to_json_value(raw_params)
                if serialized is not None:
                    evaluator_definition["parameters"] = serialized

        base_entry: dict[str, Any] = {"kind": "code"}
        if project_id:
            base_entry["project_id"] = project_id
        if proj_name:
            base_entry["project_name"] = proj_name

        # Sandbox entry only — task and scorer entries are pushed separately
        # when the eval is actually run, matching the Python SDK behavior.
        sandbox_entry = {
            **base_entry,
            "name": f"Eval {eval_name} sandbox",
            "slug": slugify(f"{stem}-{eval_name}-sandbox"),
            "function_type": "sandbox",
            "location": {
                "type": "sandbox",
                "sandbox_spec": {"provider": "lambda"},
                "entrypoints": [os.path.relpath(source_file)],
                "eval_name": eval_name,
                "evaluator_definition": evaluator_definition,
            },
            "metadata": {"_bt_sandbox_group_name": stem},
        }
        entries.append(sandbox_entry)

    return entries


async def process_file(file_path: str) -> dict[str, Any]:
    abs_path = os.path.abspath(file_path)
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    functions_registry, prompts_registry, lazy_loader, evals_registry = load_framework_globals()
    clear_registry(functions_registry)
    clear_registry(prompts_registry)
    if (
        evals_registry is not None
        and hasattr(evals_registry, "evaluators")
        and isinstance(evals_registry.evaluators, dict)
    ):
        evals_registry.evaluators.clear()
    purge_local_modules(cwd, preserve_modules={__name__, "python_runner_common"})

    module_name = import_module_name_from_cwd(cwd, abs_path)
    if module_name is None:
        module_name, extra_paths = resolve_module_info(abs_path)
    else:
        extra_paths = [cwd]
    lazy_ctx = lazy_loader(True) if callable(lazy_loader) else nullcontext()
    with lazy_ctx:
        import_file(module_name, abs_path, extra_paths)
        code_entries = collect_code_entries(functions_registry)
        event_entries = await collect_function_event_entries(prompts_registry)
        evaluator_entries = collect_evaluator_entries(evals_registry, abs_path)
        entries = [*code_entries, *event_entries, *evaluator_entries]
        file_manifest: dict[str, Any] = {
            "source_file": abs_path,
            "entries": entries,
        }
        if code_entries or evaluator_entries:
            runner_root = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.abspath(cwd)
            path_rest: list[str] = []
            for path_entry in sys.path:
                if not path_entry:
                    continue
                entry_abs = os.path.abspath(path_entry)
                if entry_abs == project_root:
                    continue
                path_rest.append(entry_abs)
            bundled_sources: list[str] = []
            seen_sources: set[str] = set()
            for source in collect_python_sources(cwd, abs_path):
                source_abs = os.path.abspath(source)
                try:
                    common = os.path.commonpath([source_abs, runner_root])
                except ValueError:
                    common = ""
                if common == runner_root:
                    continue
                try:
                    project_common = os.path.commonpath([source_abs, project_root])
                except ValueError:
                    project_common = ""
                if project_common != project_root:
                    continue
                covered_by_other_path_root = False
                for root in path_rest:
                    try:
                        root_common = os.path.commonpath([source_abs, root])
                    except ValueError:
                        continue
                    if root_common == root:
                        covered_by_other_path_root = True
                        break
                if covered_by_other_path_root:
                    continue
                if source_abs in seen_sources:
                    continue
                seen_sources.add(source_abs)
                bundled_sources.append(source_abs)
            for init_source in package_init_sources_for_module(cwd, module_name):
                if init_source in seen_sources:
                    continue
                seen_sources.add(init_source)
                bundled_sources.append(init_source)
            # Compute entry_module as a CWD-relative dotted path so that the
            # archive root inferred by push.rs walks back to CWD, matching
            # the Python SDK behavior and allowing sibling-package imports.
            rel_path = os.path.relpath(abs_path, cwd)
            archive_module = re.sub(r"\.py$", "", rel_path).replace("-", "_").replace(os.sep, ".")
            file_manifest["python_bundle"] = {
                "entry_module": archive_module,
                "sources": bundled_sources,
            }

    clear_registry(functions_registry)
    clear_registry(prompts_registry)
    if (
        evals_registry is not None
        and hasattr(evals_registry, "evaluators")
        and isinstance(evals_registry.evaluators, dict)
    ):
        evals_registry.evaluators.clear()
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
