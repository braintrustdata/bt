#!/usr/bin/env python3
import argparse
import asyncio
import fnmatch
import importlib.util
import inspect
import json
import os
import random
import re
import socket
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Callable

try:
    from braintrust import init_dataset, invoke, login
    from braintrust.framework import (
        BaseExperiment,
        EvaluatorInstance,
        _evals,
        _set_lazy_load,
        run_evaluator,
        set_thread_pool_max_workers,
    )
    from braintrust.logger import Dataset, init as init_logger_experiment, parent_context, _internal_get_global_state
    from braintrust.parameters import parameters_to_json_schema, validate_parameters
    from braintrust.util import eprint
    from braintrust.span_identifier_v4 import parse_parent
except Exception as exc:  # pragma: no cover - runtime guard
    print(
        "Unable to import the braintrust package. Please install it in your Python environment.",
        file=sys.stderr,
    )
    print(str(exc), file=sys.stderr)
    sys.exit(1)

WATCHABLE_PYTHON_EXTENSIONS = {".py"}
WATCH_EXCLUDE_SEGMENTS = (
    "/site-packages/",
    "/dist-packages/",
    "/__pycache__/",
    "/.venv/",
    "/venv/",
)
_DATASET_TOTAL_CACHE: dict[str, int] = {}


@dataclass(frozen=True)
class EvalFilter:
    path: list[str]
    pattern: re.Pattern[str]


@dataclass(frozen=True)
class RunnerConfig:
    jsonl: bool
    list_only: bool
    terminate_on_failure: bool
    num_workers: int | None
    filters: list[EvalFilter]
    first: int | None
    sample: int | None
    sample_seed: int | None
    dev_mode: str | None
    dev_request_json: str | None


@dataclass
class SseWriter:
    sock: socket.socket

    def send(self, event: str, data: Any) -> None:
        payload = serialize_sse_event(event, data)
        self.sock.sendall(payload.encode("utf-8"))

    def close(self) -> None:
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.sock.close()


def serialize_sse_event(event: str, data: Any) -> str:
    if isinstance(data, (dict, list)):
        data_str = json.dumps(data)
    else:
        data_str = str(data)
    return f"event: {event}\ndata: {data_str}\n\n"


def create_sse_writer() -> SseWriter | None:
    sock_path = os.getenv("BT_EVAL_SSE_SOCK")
    if sock_path:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(sock_path)
        return SseWriter(sock)

    addr = os.getenv("BT_EVAL_SSE_ADDR")
    if addr:
        if ":" not in addr:
            raise ValueError("BT_EVAL_SSE_ADDR must be in host:port format")
        host, port_str = addr.rsplit(":", 1)
        sock = socket.create_connection((host, int(port_str)))
        return SseWriter(sock)

    return None


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


def parse_dev_mode(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    if value in {"list", "eval"}:
        return value
    raise ValueError(f"Invalid BT_EVAL_DEV_MODE value: {value}")


def parse_positive_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    parsed = int(value)
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def parse_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or value == "":
        return None
    return int(value)


def read_runner_config() -> RunnerConfig:
    num_workers_value = os.getenv("BT_EVAL_NUM_WORKERS")
    num_workers = int(num_workers_value) if num_workers_value else None
    return RunnerConfig(
        jsonl=env_flag("BT_EVAL_JSONL"),
        list_only=env_flag("BT_EVAL_LIST"),
        terminate_on_failure=env_flag("BT_EVAL_TERMINATE_ON_FAILURE"),
        num_workers=num_workers,
        filters=parse_serialized_filters(os.getenv("BT_EVAL_FILTER_PARSED")),
        first=parse_positive_int_env("BT_EVAL_FIRST"),
        sample=parse_positive_int_env("BT_EVAL_SAMPLE"),
        sample_seed=parse_int_env("BT_EVAL_SAMPLE_SEED"),
        dev_mode=parse_dev_mode(os.getenv("BT_EVAL_DEV_MODE")),
        dev_request_json=os.getenv("BT_EVAL_DEV_REQUEST_JSON"),
    )


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


def filter_evaluators(evaluators: list[EvaluatorInstance], filters: list[EvalFilter]) -> list[EvaluatorInstance]:
    if not filters:
        return evaluators
    return [
        evaluator
        for evaluator in evaluators
        if all(evaluate_filter(evaluator.evaluator, filt) for filt in filters)
    ]


def snake_to_camel(value: str) -> str:
    parts = value.split("_")
    if not parts:
        return value
    return parts[0] + "".join(word.title() for word in parts[1:])


def sampling_metadata(config: RunnerConfig) -> dict[str, Any]:
    if config.first is not None:
        return {
            "runMode": "first",
            "isFinal": False,
            "runLabel": f"Run mode: first {config.first} examples (non-final smoke run)",
            "sampleCount": config.first,
        }
    if config.sample is not None:
        seed = config.sample_seed if config.sample_seed is not None else 0
        return {
            "runMode": "sample",
            "isFinal": False,
            "runLabel": f"Run mode: random sample of {config.sample} examples (seed {seed}, non-final smoke run)",
            "sampleCount": config.sample,
            "sampleSeed": seed,
        }
    return {
        "runMode": "full",
        "isFinal": True,
        "runLabel": "Run mode: full dataset",
    }


def format_summary(summary: dict[str, Any], config: RunnerConfig) -> dict[str, Any]:
    return {
        **{snake_to_camel(k): v for k, v in summary.items()},
        **sampling_metadata(config),
    }


def send_eval_progress(sse: SseWriter | None, evaluator_name: str, kind: str, total: int | None = None) -> None:
    if not sse:
        return
    payload = {
        "id": f"eval-progress:{evaluator_name}",
        "object_type": "task",
        "format": "global",
        "output_type": "any",
        "name": evaluator_name,
        "event": "progress",
        "data": json.dumps({
            "type": "eval_progress",
            "kind": kind,
            **({"total": total} if total is not None else {}),
        }),
    }
    sse.send("progress", payload)


def create_progress_reporter(sse: SseWriter | None, evaluator_name: str) -> Callable[[str, int | None], None] | None:
    if not sse:
        return None

    def report(event: str, total: int | None) -> None:
        send_eval_progress(sse, evaluator_name, event, total)

    return report


def serialize_error(
    message: str,
    stack: str | None = None,
    status: int | None = None,
) -> dict[str, Any]:
    data = {"message": message}
    if stack:
        data["stack"] = stack
    if status is not None:
        data["status"] = status
    return data


def infer_eval_error_status(message: str) -> int:
    text = message.lower()
    if "not found" in text:
        return 404
    if (
        "invalid parameter" in text
        or "invalid parameters" in text
        or "must include" in text
        or "invalid eval" in text
        or "invalid request" in text
        or "failed to load dataset" in text
    ):
        return 400
    return 500


def send_eval_error(sse: SseWriter | None, message: str, stack: str | None = None, status: int | None = None) -> None:
    payload = serialize_error(message, stack, status)
    if sse:
        sse.send("error", payload)
    else:
        eprint(message)


def parse_eval_request(raw: str | None) -> dict[str, Any]:
    if not raw:
        raise ValueError("Missing BT_EVAL_DEV_REQUEST_JSON")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid BT_EVAL_DEV_REQUEST_JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise ValueError("BT_EVAL_DEV_REQUEST_JSON must be a JSON object.")
    if not isinstance(parsed.get("name"), str) or not parsed["name"]:
        raise ValueError("Eval request must include a non-empty name.")
    if not isinstance(parsed.get("data"), dict):
        raise ValueError("Eval request must include a data object.")

    scores = parsed.get("scores")
    if scores is not None:
        if not isinstance(scores, list):
            raise ValueError("scores must be an array")
        for i, score in enumerate(scores):
            if not isinstance(score, dict):
                raise ValueError(f"scores[{i}] must be an object")
            if not isinstance(score.get("name"), str) or not score["name"]:
                raise ValueError(f"scores[{i}].name must be a non-empty string")
            if not isinstance(score.get("function_id"), dict):
                raise ValueError(f"scores[{i}].function_id must be an object")

    return parsed


def resolve_eval_data(data: dict[str, Any]) -> Any:
    if "data" in data:
        return data["data"]

    dataset_name = data.get("dataset_name")
    if isinstance(dataset_name, str):
        if isinstance(data.get("project_name"), str):
            return init_dataset(
                project=data["project_name"],
                name=dataset_name,
                _internal_btql=data.get("_internal_btql"),
            )
        if isinstance(data.get("project_id"), str):
            return init_dataset(
                project_id=data["project_id"],
                name=dataset_name,
                _internal_btql=data.get("_internal_btql"),
            )

    raise ValueError("Invalid eval data payload.")


async def resolve_sampling_source(data: Any) -> Any:
    current = data
    while True:
        if callable(current):
            current = current()
            continue
        if inspect.isawaitable(current):
            current = await current
            continue
        return current


async def iter_data_source(data: Any, batch_size_hint: int | None = None):
    resolved = await resolve_sampling_source(data)
    if isinstance(resolved, Dataset):
        fetched = resolved.fetch(batch_size=batch_size_hint)
        if hasattr(fetched, "__aiter__"):
            async for item in fetched:
                yield item
            return
        for item in fetched:
            yield item
        return
    if isinstance(resolved, (str, bytes, dict)):
        raise ValueError(
            "Sampling is only supported for arrays, iterables, and Braintrust datasets."
        )
    if hasattr(resolved, "__aiter__"):
        async for item in resolved:
            yield item
        return
    try:
        iterator = iter(resolved)
    except TypeError as exc:
        raise ValueError(
            "Sampling is only supported for arrays, iterables, and Braintrust datasets."
        ) from exc
    for item in iterator:
        yield item


async def collect_first_records(data: Any, count: int) -> list[Any]:
    items: list[Any] = []
    async for item in iter_data_source(data, batch_size_hint=count):
        items.append(item)
        if len(items) >= count:
            break
    return items


async def reservoir_sample_records(data: Any, count: int, seed: int) -> list[Any]:
    rng = random.Random(seed)
    sample: list[Any] = []
    seen = 0
    async for item in iter_data_source(data):
        seen += 1
        if len(sample) < count:
            sample.append(item)
            continue
        index = rng.randrange(seen)
        if index < count:
            sample[index] = item
    return sample


async def apply_sampling_to_data(data: Any, config: RunnerConfig) -> Any:
    if config.first is not None:
        return await collect_first_records(data, config.first)
    if config.sample is not None:
        return await reservoir_sample_records(data, config.sample, config.sample_seed or 0)
    return data


def make_eval_scorer(
    score: dict[str, Any],
    project_id: str | None,
) -> Callable[..., Any]:
    function_id = dict(score["function_id"])
    score_name = score["name"]

    def scorer(input: Any, output: Any, expected: Any = None, metadata: Any = None, **_kwargs: Any) -> Any:
        kwargs = {
            **function_id,
            "input": {
                "input": input,
                "output": output,
                "expected": expected,
                "metadata": metadata,
            },
            "stream": False,
            "mode": "auto",
            "strict": True,
        }
        if project_id:
            kwargs["project_id"] = project_id
        return invoke(**kwargs)

    scorer.__name__ = score_name
    return scorer


def build_eval_definitions(evaluator_instances: list[EvaluatorInstance]) -> dict[str, Any]:
    definitions: dict[str, Any] = {}
    for evaluator_instance in evaluator_instances:
        evaluator = evaluator_instance.evaluator
        scores = [{"name": getattr(score, "__name__", f"scorer_{i}")} for i, score in enumerate(evaluator.scores)]
        definitions[evaluator.eval_name] = {
            "parameters": parameters_to_json_schema(evaluator.parameters) if evaluator.parameters else {},
            "scores": scores,
        }
    return definitions


def collect_files(input_path: str) -> list[str]:
    if os.path.isdir(input_path):
        matches: list[str] = []
        for root, _, files in os.walk(input_path):
            for filename in files:
                matches.append(os.path.join(root, filename))
        return matches
    return [input_path]


def is_watchable_dependency(path_input: str, cwd: str) -> bool:
    path = os.path.abspath(path_input)
    normalized = path.replace("\\", "/")
    if not os.path.isfile(path):
        return False
    if os.path.splitext(path)[1].lower() not in WATCHABLE_PYTHON_EXTENSIONS:
        return False
    if any(segment in normalized for segment in WATCH_EXCLUDE_SEGMENTS):
        return False

    try:
        common = os.path.commonpath([path, cwd])
    except ValueError:
        return False
    return common == cwd


def collect_dependency_files(cwd: str, input_files: list[str]) -> list[str]:
    dependencies: set[str] = set()
    for module in list(sys.modules.values()):
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        candidate = module_file[:-1] if module_file.endswith(".pyc") else module_file
        if is_watchable_dependency(candidate, cwd):
            dependencies.add(os.path.abspath(candidate))

    for file_path in input_files:
        path = os.path.abspath(file_path)
        if is_watchable_dependency(path, cwd):
            dependencies.add(path)

    return sorted(dependencies)


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


def resolve_reporter(
    reporter: Any,
    reporters: dict[str, Any],
) -> Any | None:
    if isinstance(reporter, str):
        if reporter not in reporters:
            raise ValueError(f"Reporter {reporter} not found")
        return reporters[reporter]
    if reporter is not None:
        return reporter

    if len(reporters) == 0:
        return None
    if len(reporters) == 1:
        return next(iter(reporters.values()))
    reporter_names = ", ".join(reporters.keys())
    raise ValueError(
        f"Multiple reporters found ({reporter_names}). Please specify a reporter explicitly."
    )


def _init_experiment_for_eval(evaluator):
    base_experiment_name = None
    if isinstance(evaluator.data, BaseExperiment):
        base_experiment_name = evaluator.data.name

    dataset = None
    if isinstance(evaluator.data, Dataset):
        dataset = evaluator.data

    return init_logger_experiment(
        project=evaluator.project_name,
        project_id=evaluator.project_id,
        experiment=evaluator.experiment_name,
        description=evaluator.description,
        metadata=evaluator.metadata,
        is_public=evaluator.is_public,
        update=evaluator.update,
        base_experiment=base_experiment_name,
        base_experiment_id=evaluator.base_experiment_id,
        git_metadata_settings=evaluator.git_metadata_settings,
        repo_info=evaluator.repo_info,
        dataset=dataset,
        set_current=False,
    )


def send_experiment_start(
    sse: SseWriter | None,
    evaluator: Any,
    experiment: Any | None,
) -> None:
    if not sse:
        return

    if experiment is not None:
        try:
            summary = experiment.summarize(summarize_scores=False)
            sse.send(
                "start",
                {
                    "projectName": getattr(summary, "project_name", None),
                    "experimentName": getattr(summary, "experiment_name", None),
                    "projectId": getattr(summary, "project_id", None),
                    "experimentId": getattr(summary, "experiment_id", None),
                    "projectUrl": getattr(summary, "project_url", None),
                    "experimentUrl": getattr(summary, "experiment_url", None),
                },
            )
            return
        except Exception:
            pass

    experiment_name = getattr(evaluator, "experiment_name", None) or getattr(
        evaluator, "eval_name", None
    )
    if experiment_name:
        sse.send("start", {"experimentName": experiment_name})


def run_evaluator_progress_mode() -> str:
    try:
        parameters = inspect.signature(run_evaluator).parameters
        if "progress" in parameters:
            return "progress"
        if "stream" in parameters:
            return "stream"
    except Exception:
        pass
    return "none"


def infer_data_total(data: Any) -> int | None:
    if data is None:
        return None

    try:
        total = len(data)
        if isinstance(total, int) and total >= 0:
            return total
    except Exception:
        pass

    if isinstance(data, Dataset):
        dataset_id = None
        try:
            dataset_id = data.id
        except Exception:
            dataset_id = None

        if dataset_id and dataset_id in _DATASET_TOTAL_CACHE:
            return _DATASET_TOTAL_CACHE[dataset_id]

        try:
            summary = data.summarize(summarize_data=True)
            data_summary = getattr(summary, "data_summary", None)
            total_records = getattr(data_summary, "total_records", None)
            if isinstance(total_records, int) and total_records >= 0:
                if dataset_id:
                    _DATASET_TOTAL_CACHE[dataset_id] = total_records
                return total_records
        except Exception:
            return None

    if inspect.isclass(data) or inspect.isroutine(data):
        return None
    if inspect.isgenerator(data) or inspect.isasyncgen(data):
        return None

    return None


def normalize_trial_count(value: Any) -> int:
    try:
        trial_count = int(value)
    except Exception:
        trial_count = 1
    if trial_count < 1:
        trial_count = 1
    return trial_count


def infer_evaluator_total(evaluator: Any) -> int | None:
    data_total = infer_data_total(getattr(evaluator, "data", None))
    if data_total is None:
        return None

    trial_count = normalize_trial_count(getattr(evaluator, "trial_count", 1))

    return data_total * trial_count


def wrap_data_with_adaptive_total(
    data: Any,
    progress_cb: Callable[[str, int | None], None] | None,
    trial_count: int,
    state: dict[str, int] | None = None,
) -> Any:
    if progress_cb is None:
        return data
    if state is None:
        state = {"rows_seen": 0, "last_emitted_total": 0}

    def report_rows_seen() -> None:
        rows_seen = state["rows_seen"]
        if rows_seen <= 0:
            return
        total = rows_seen * trial_count
        if total <= state["last_emitted_total"]:
            return
        state["last_emitted_total"] = total
        progress_cb("set_total", total)

    def maybe_report_empty_total(exhausted: bool) -> None:
        # Unknown-total evaluators that exhaust without yielding should still
        # emit an explicit total of 0. The renderer decides whether that
        # threshold is shown as spinner or determinate progress.
        if not exhausted:
            return
        if state["rows_seen"] != 0:
            return
        if state["last_emitted_total"] != 0:
            return
        progress_cb("set_total", 0)

    if inspect.isclass(data):
        return data

    if inspect.isroutine(data):
        def wrapped_data(*args, **kwargs):
            resolved = data(*args, **kwargs)
            return wrap_data_with_adaptive_total(
                resolved,
                progress_cb,
                trial_count,
                state,
            )

        if hasattr(data, "__name__"):
            setattr(wrapped_data, "__name__", getattr(data, "__name__"))
        if hasattr(data, "__qualname__"):
            setattr(wrapped_data, "__qualname__", getattr(data, "__qualname__"))
        return wrapped_data

    if inspect.isawaitable(data):
        async def wrapped_awaitable():
            resolved = await data
            return wrap_data_with_adaptive_total(
                resolved,
                progress_cb,
                trial_count,
                state,
            )

        return wrapped_awaitable()

    if inspect.isasyncgen(data) or hasattr(data, "__aiter__"):
        async def wrapped_async_iter():
            exhausted = False
            try:
                async for item in data:
                    state["rows_seen"] += 1
                    report_rows_seen()
                    yield item
                exhausted = True
            finally:
                maybe_report_empty_total(exhausted)

        return wrapped_async_iter()

    if inspect.isgenerator(data):
        async def wrapped_iter():
            exhausted = False
            try:
                for item in data:
                    state["rows_seen"] += 1
                    report_rows_seen()
                    yield item
                    # Let scheduled eval tasks make progress while we keep ingesting rows.
                    await asyncio.sleep(0)
                exhausted = True
            finally:
                maybe_report_empty_total(exhausted)

        return wrapped_iter()

    if isinstance(data, (str, bytes, dict)):
        return data

    try:
        iterator = iter(data)
    except Exception:
        return data

    async def wrapped_iterable():
        exhausted = False
        try:
            for item in iterator:
                state["rows_seen"] += 1
                report_rows_seen()
                yield item
                # Avoid starving task execution while iterating synchronous data sources.
                await asyncio.sleep(0)
            exhausted = True
        finally:
            maybe_report_empty_total(exhausted)

    return wrapped_iterable()


def wrap_task(
    task: Any,
    progress_cb: Callable[[str, int | None], None] | None = None,
    stream_results: bool = False,
) -> Any:
    if not callable(task):
        return task
    if progress_cb is None and not stream_results:
        return task

    task_signature = None
    try:
        task_signature = inspect.signature(task)
    except Exception:
        task_signature = None

    takes_hooks = task_signature is not None and len(task_signature.parameters) >= 2

    async def wrapped_task(input, hooks):
        result = None
        try:
            if takes_hooks:
                result = task(input, hooks)
            else:
                result = task(input)
            if inspect.isawaitable(result):
                result = await result
            return result
        finally:
            if progress_cb is not None:
                progress_cb("increment", None)
            if stream_results and result is not None:
                try:
                    hooks.report_progress({
                        "format": "code",
                        "output_type": "completion",
                        "event": "json_delta",
                        "data": json.dumps(result),
                    })
                except Exception:
                    pass

    if hasattr(task, "__name__"):
        setattr(wrapped_task, "__name__", getattr(task, "__name__"))
    if hasattr(task, "__qualname__"):
        setattr(wrapped_task, "__qualname__", getattr(task, "__qualname__"))

    return wrapped_task


def run_evaluator_supports_stream() -> bool:
    try:
        return "stream" in inspect.signature(run_evaluator).parameters
    except Exception:
        return False


async def run_evaluator_task(
    evaluator,
    position: int,
    no_send_logs: bool,
    progress_cb,
    progress_mode: str,
    sse: SseWriter | None,
    config: RunnerConfig,
    parent: str | None = None,
):
    experiment = None
    if not no_send_logs and parent is None:
        experiment = _init_experiment_for_eval(evaluator)
    send_experiment_start(sse, evaluator, experiment)

    fallback_progress = progress_cb is not None and progress_mode != "progress"
    original_task = evaluator.task
    original_data = evaluator.data
    sampled_data = await apply_sampling_to_data(original_data, config)
    evaluator.data = sampled_data
    supports_stream = run_evaluator_supports_stream()

    if fallback_progress:
        inferred_total = infer_evaluator_total(evaluator)
        progress_cb("start", inferred_total)
        if inferred_total is None:
            trial_count = normalize_trial_count(getattr(evaluator, "trial_count", 1))
            evaluator.data = wrap_data_with_adaptive_total(
                evaluator.data,
                progress_cb,
                trial_count,
            )

    evaluator.task = wrap_task(
        original_task,
        progress_cb=progress_cb if fallback_progress else None,
        stream_results=bool(sse and supports_stream),
    )

    try:
        kwargs = {}
        if progress_cb and progress_mode == "progress":
            kwargs["progress"] = progress_cb
        if sse and supports_stream:
            kwargs["stream"] = lambda event: sse.send("progress", event if isinstance(event, dict) else event.__dict__)

        if parent:
            with parent_context(parent):
                return await run_evaluator(
                    experiment,
                    evaluator,
                    None,
                    [],
                    **kwargs,
                )
        else:
            return await run_evaluator(
                experiment,
                evaluator,
                None,
                [],
                **kwargs,
            )
    finally:
        evaluator.task = original_task
        evaluator.data = original_data
        if fallback_progress:
            progress_cb("stop", None)
        if experiment:
            experiment.flush()


async def run_requested_eval(
    evaluator_instances: list[EvaluatorInstance],
    reporters: dict[str, Any],
    no_send_logs: bool,
    sse: SseWriter | None,
    config: RunnerConfig,
) -> bool:
    try:
        request = parse_eval_request(config.dev_request_json)
    except Exception as exc:
        send_eval_error(sse, str(exc), traceback.format_exc(), 400)
        return False

    target_name = request["name"]
    evaluator_instance = next(
        (candidate for candidate in evaluator_instances if candidate.evaluator.eval_name == target_name),
        None,
    )
    if evaluator_instance is None:
        send_eval_error(sse, f"Evaluator '{target_name}' not found", None, 404)
        return False

    evaluator = evaluator_instance.evaluator
    try:
        evaluator.data = resolve_eval_data(request["data"])

        if "experiment_name" in request:
            evaluator.experiment_name = request["experiment_name"]
        if "project_id" in request:
            evaluator.project_id = request["project_id"]

        request_parameters = request.get("parameters")
        if evaluator.parameters is not None:
            if request_parameters is None:
                request_parameters = {}
            if not isinstance(request_parameters, dict):
                raise ValueError("parameters must be an object")
            evaluator.parameters = validate_parameters(request_parameters, evaluator.parameters)
        elif request_parameters is not None:
            if not isinstance(request_parameters, dict):
                raise ValueError("parameters must be an object")
            evaluator.parameters = request_parameters

        if "scores" in request and request["scores"]:
            scorer_functions = [
                make_eval_scorer(score, request.get("project_id"))
                for score in request["scores"]
            ]
            evaluator.scores = [*evaluator.scores, *scorer_functions]
    except Exception as exc:
        message = str(exc)
        send_eval_error(sse, message, traceback.format_exc(), infer_eval_error_status(message))
        return False

    try:
        resolved_reporter = resolve_reporter(
            getattr(evaluator_instance, "reporter", None),
            reporters,
        )
    except Exception as exc:
        message = str(exc)
        send_eval_error(sse, message, traceback.format_exc(), infer_eval_error_status(message))
        return False

    progress_mode = run_evaluator_progress_mode()
    progress_cb = create_progress_reporter(sse, evaluator.eval_name)

    parent = request.get("parent")
    if parent is not None:
        parent = parse_parent(parent)

    try:
        result = await run_evaluator_task(
            evaluator,
            0,
            no_send_logs,
            progress_cb,
            progress_mode,
            sse,
            config,
            parent=parent,
        )
    except Exception as exc:
        message = str(exc)
        send_eval_error(sse, message, traceback.format_exc(), infer_eval_error_status(message))
        return False

    if sse:
        sse.send("summary", format_summary(result.summary.as_dict(), config))
    elif config.jsonl:
        print(json.dumps(format_summary(result.summary.as_dict(), config)))
    else:
        print(result.summary)

    failures = [row for row in result.results if row.error]
    if failures and resolved_reporter is None:
        first_error = failures[0]
        message = f"Evaluator {evaluator.eval_name} failed with {len(failures)} error(s)."
        send_eval_error(sse, message, first_error.exc_info, 500)
        return False

    return True


async def run_once(
    files: list[str],
    no_send_logs: bool,
    sse: SseWriter | None,
    config: RunnerConfig,
) -> bool:
    evaluators, reporters = load_evaluators(files)
    if not evaluators and not config.list_only and config.dev_mode != "list":
        message = "No evaluators found. Did you call Eval() in the file?"
        if sse:
            sse.send("console", {"stream": "stderr", "message": message})
        else:
            eprint(message)
        return True

    evaluators = filter_evaluators(evaluators, config.filters)
    if config.dev_mode == "list":
        print(json.dumps(build_eval_definitions(evaluators)))
        return True
    if config.dev_mode == "eval":
        return await run_requested_eval(evaluators, reporters, no_send_logs, sse, config)

    if config.list_only:
        for evaluator_instance in evaluators:
            print(evaluator_instance.evaluator.eval_name)
        return True

    if sse:
        sse.send("processing", {"evaluators": len(evaluators)})

    progress_mode = run_evaluator_progress_mode()

    async def run_single_evaluator(
        idx: int, evaluator_instance: EvaluatorInstance
    ) -> tuple[EvaluatorInstance, Any | None, Any | None, dict[str, Any] | None]:
        try:
            resolved_reporter = resolve_reporter(
                getattr(evaluator_instance, "reporter", None),
                reporters,
            )
        except Exception as exc:
            err = serialize_error(str(exc), traceback.format_exc())
            return evaluator_instance, None, None, err

        progress_cb = create_progress_reporter(sse, evaluator_instance.evaluator.eval_name)
        try:
            result = await run_evaluator_task(
                evaluator_instance.evaluator,
                idx,
                no_send_logs,
                progress_cb,
                progress_mode,
                sse,
                config,
            )
        except Exception as exc:
            err = serialize_error(str(exc), traceback.format_exc())
            return evaluator_instance, resolved_reporter, None, err

        return evaluator_instance, resolved_reporter, result, None

    execution_results: list[tuple[EvaluatorInstance, Any | None, Any | None, dict[str, Any] | None]] = []
    if config.terminate_on_failure:
        for idx, evaluator_instance in enumerate(evaluators):
            run_result = await run_single_evaluator(idx, evaluator_instance)
            execution_results.append(run_result)
            if run_result[3] is not None:
                break
    else:
        tasks = [
            asyncio.create_task(run_single_evaluator(idx, evaluator_instance))
            for idx, evaluator_instance in enumerate(evaluators)
        ]
        execution_results = list(await asyncio.gather(*tasks))

    all_success = True
    for evaluator_instance, resolved_reporter, result, err in execution_results:
        if err is not None:
            all_success = False
            if sse:
                sse.send("error", err)
            else:
                eprint(err.get("message"))
            continue

        if sse:
            sse.send("summary", format_summary(result.summary.as_dict(), config))
        elif config.jsonl:
            print(json.dumps(format_summary(result.summary.as_dict(), config)))
        else:
            print(result.summary)

        failures = [row for row in result.results if row.error]
        if failures and resolved_reporter is None:
            all_success = False
            first_error = failures[0]
            message = (
                f"Evaluator {evaluator_instance.evaluator.eval_name} failed with {len(failures)} error(s)."
            )
            stack = first_error.exc_info
            if sse:
                sse.send("error", serialize_error(message, stack))
            else:
                eprint(message)
            if config.terminate_on_failure:
                break

    return all_success


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run evals and emit SSE events for bt.")
    parser.add_argument("files", nargs="*", help="Eval files or directories to run.")
    parser.add_argument("--local", action="store_true", help="Do not send logs to Braintrust.")
    parser.add_argument("--api-key", help="Specify a braintrust API key.")
    parser.add_argument("--org-name", help="Organization name.")
    parser.add_argument("--app-url", help="Braintrust app URL.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = read_runner_config()
    local = args.local or env_flag("BT_EVAL_LOCAL") or env_flag("BT_EVAL_NO_SEND_LOGS")
    files = args.files or ["."]
    if config.num_workers is not None:
        set_thread_pool_max_workers(config.num_workers)

    if not local:
        login(api_key=args.api_key, org_name=args.org_name, app_url=args.app_url)

    sse = create_sse_writer()
    cwd = os.path.abspath(os.getcwd())
    try:
        success = asyncio.run(run_once(files, local, sse, config))

        if not local:
            try:
                state = _internal_get_global_state()
                if state:
                    state.flush()
            except Exception:
                pass

        if sse:
            sse.send("dependencies", {"files": collect_dependency_files(cwd, files)})
            sse.send("done", {"success": success})
        return 0 if success else 1
    finally:
        if sse:
            sse.close()


if __name__ == "__main__":
    sys.exit(main())
