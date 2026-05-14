from __future__ import annotations

import asyncio
import importlib.util
import inspect
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, AsyncIterator

try:
    from braintrust.framework import (
        BaseExperiment,
        EvaluatorInstance,
        _evals,
        _set_lazy_load,
    )
    from braintrust.logger import Dataset
except Exception:
    raise


@dataclass(frozen=True)
class EvalFilter:
    path: list[str]
    pattern: re.Pattern[str]


def env_flag(name: str) -> bool:
    value = os.getenv(name)
    if value is None:
        return False
    return value.lower() not in {"0", "false", "no", "off", ""}


def parse_serialized_filters(serialized: str | None) -> list[EvalFilter]:
    if not serialized:
        return []

    parsed = json.loads(serialized)
    if not isinstance(parsed, list):
        raise ValueError("BT_EVAL_FILTER_PARSED must be a JSON array")

    filters: list[EvalFilter] = []
    for i, entry in enumerate(parsed):
        if not isinstance(entry, dict):
            raise ValueError("BT_EVAL_FILTER_PARSED entries must be objects with {path, pattern}")
        key_path = entry.get("path")
        pattern = entry.get("pattern")
        if not isinstance(key_path, list) or not all(isinstance(part, str) for part in key_path):
            raise ValueError(f"BT_EVAL_FILTER_PARSED entry {i} path must be an array of strings")
        if not isinstance(pattern, str):
            raise ValueError(f"BT_EVAL_FILTER_PARSED entry {i} pattern must be a string")
        filters.append(EvalFilter(path=key_path, pattern=re.compile(pattern)))
    return filters


def _to_mapping(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _to_mapping(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_mapping(v) for v in value]
    if hasattr(value, "__dict__"):
        return {
            key: _to_mapping(val)
            for key, val in vars(value).items()
            if not key.startswith("_")
        }
    return value


def serialize_json_with_plain_string(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value)


def evaluate_filter(value: Any, filt: EvalFilter) -> bool:
    current = _to_mapping(value)
    for part in filt.path:
        if not isinstance(current, dict) or part not in current:
            return False
        current = current[part]
    return bool(filt.pattern.search(serialize_json_with_plain_string(current)))


def filter_evaluators(
    evaluators: list[EvaluatorInstance], filters: list[EvalFilter]
) -> list[EvaluatorInstance]:
    if not filters:
        return evaluators
    return [
        evaluator
        for evaluator in evaluators
        if all(evaluate_filter(evaluator.evaluator, filt) for filt in filters)
    ]


async def call_evaluator_data(data: Any) -> tuple[Any, str | None]:
    data_result = data
    if inspect.isclass(data_result):
        data_result = data_result()
    if inspect.isfunction(data_result) or inspect.isroutine(data_result):
        data_result = data_result()
    if inspect.isawaitable(data_result):
        data_result = await data_result

    base_experiment_name = None
    if isinstance(data_result, BaseExperiment):
        base_experiment_name = data_result.name

    return data_result, base_experiment_name


def to_async_iterator(value: Any) -> AsyncIterator[Any]:
    if inspect.isasyncgen(value):
        return value

    async def to_async(it):
        for item in it:
            yield item

    return to_async(value)


def collect_files(input_path: str) -> list[str]:
    if os.path.isdir(input_path):
        matches: list[str] = []
        for root, _, files in os.walk(input_path):
            for filename in files:
                matches.append(os.path.join(root, filename))
        return matches
    return [input_path]


def resolve_module_info(in_file: str) -> tuple[str, list[str]]:
    in_file = os.path.abspath(in_file)
    module_dir = os.path.dirname(in_file)
    module_name = os.path.splitext(os.path.basename(in_file))[0]

    package_parts: list[str] = []
    current = module_dir
    while os.path.isfile(os.path.join(current, "__init__.py")):
        package_parts.insert(0, os.path.basename(current))
        current = os.path.dirname(current)

    extra_paths = [module_dir]
    if package_parts:
        module_name = ".".join(package_parts + [module_name])
        if current not in extra_paths:
            extra_paths.append(current)

    return module_name, extra_paths


def load_evaluators(files: list[str]) -> tuple[list[EvaluatorInstance], dict[str, Any]]:
    evaluator_instances: list[EvaluatorInstance] = []
    reporters: dict[str, Any] = {}
    cwd = os.getcwd()
    if cwd not in sys.path:
        sys.path.insert(0, cwd)

    for f in files:
        d = os.path.dirname(os.path.abspath(f))
        while d and d != os.path.dirname(d):
            if os.path.isfile(os.path.join(d, "register.py")):
                if d not in sys.path:
                    sys.path.insert(0, d)
                break
            d = os.path.dirname(d)

    unique_files: set[str] = set()
    for file_path in files:
        for candidate in collect_files(file_path):
            unique_files.add(os.path.abspath(candidate))

    for file_path in sorted(unique_files):
        module_name, extra_paths = resolve_module_info(file_path)
        with _set_lazy_load(True):
            _evals.clear()
            try:
                for extra_path in reversed(extra_paths):
                    if extra_path not in sys.path:
                        sys.path.insert(0, extra_path)

                spec = importlib.util.spec_from_file_location(module_name, file_path)
                if spec is None or spec.loader is None:
                    raise ImportError(f"Unable to load module spec for {file_path}")

                sys.modules.pop(module_name, None)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)

                evaluator_instances.extend(
                    [
                        instance
                        for instance in _evals.evaluators.values()
                        if isinstance(instance, EvaluatorInstance)
                    ]
                )
                for reporter_name, reporter in _evals.reporters.items():
                    if reporter_name not in reporters:
                        reporters[reporter_name] = reporter
            finally:
                _evals.clear()

    return evaluator_instances, reporters


__all__ = [
    "BaseExperiment",
    "Dataset",
    "EvalFilter",
    "EvaluatorInstance",
    "call_evaluator_data",
    "env_flag",
    "filter_evaluators",
    "load_evaluators",
    "parse_serialized_filters",
    "serialize_json_with_plain_string",
    "to_async_iterator",
]
