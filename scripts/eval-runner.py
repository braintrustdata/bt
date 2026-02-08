#!/usr/bin/env python3
import argparse
import asyncio
import fnmatch
import importlib.util
import json
import os
import re
import socket
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Callable
from pathlib import PurePosixPath

try:
    from braintrust import login
    from braintrust.framework import (
        BaseExperiment,
        EvaluatorInstance,
        _evals,
        _set_lazy_load,
        run_evaluator,
        set_thread_pool_max_workers,
    )
    from braintrust.logger import Dataset
    from braintrust.util import eprint
except Exception as exc:  # pragma: no cover - runtime guard
    print(
        "Unable to import the braintrust package. Please install it in your Python environment.",
        file=sys.stderr,
    )
    print(str(exc), file=sys.stderr)
    sys.exit(1)

INCLUDE = ["**/eval_*.py", "**/*.eval.py"]
EXCLUDE = ["**/site-packages/**", "**/__pycache__/**"]


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


def parse_filter_expressions(serialized: str | None) -> list[EvalFilter]:
    if not serialized:
        return []

    parsed = json.loads(serialized)
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise ValueError("BT_EVAL_FILTER must be a JSON array of strings")

    filters: list[EvalFilter] = []
    for expression in parsed:
        equals_idx = expression.find("=")
        if equals_idx == -1:
            raise ValueError(f"Invalid filter expression: {expression}")
        key_path = expression[:equals_idx].strip()
        pattern = expression[equals_idx + 1 :]
        if not key_path:
            raise ValueError(f"Invalid filter expression: {expression}")
        filters.append(EvalFilter(path=key_path.split("."), pattern=re.compile(pattern)))
    return filters


def read_runner_config() -> RunnerConfig:
    num_workers_value = os.getenv("BT_EVAL_NUM_WORKERS")
    num_workers = int(num_workers_value) if num_workers_value else None
    return RunnerConfig(
        jsonl=env_flag("BT_EVAL_JSONL"),
        list_only=env_flag("BT_EVAL_LIST"),
        terminate_on_failure=env_flag("BT_EVAL_TERMINATE_ON_FAILURE"),
        num_workers=num_workers,
        filters=parse_filter_expressions(os.getenv("BT_EVAL_FILTER")),
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


def format_summary(summary: dict[str, Any]) -> dict[str, Any]:
    return {snake_to_camel(k): v for k, v in summary.items()}


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


def serialize_error(message: str, stack: str | None = None) -> dict[str, Any]:
    data = {"message": message}
    if stack:
        data["stack"] = stack
    return data


def check_match(path_input: str) -> bool:
    p = PurePosixPath(os.path.abspath(path_input).replace("\\", "/"))
    if INCLUDE:
        matched = any(p.match(pattern) for pattern in INCLUDE)
        if not matched:
            return False
    if EXCLUDE:
        if any(p.match(pattern) for pattern in EXCLUDE):
            return False
    return True


def collect_files(input_path: str) -> list[str]:
    if os.path.isdir(input_path):
        matches: list[str] = []
        for root, _, files in os.walk(input_path):
            for filename in files:
                fname = os.path.join(root, filename)
                if check_match(fname):
                    matches.append(fname)
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

    from braintrust.framework import init_experiment

    return init_experiment(
        project_name=evaluator.project_name,
        project_id=evaluator.project_id,
        experiment_name=evaluator.experiment_name,
        description=evaluator.description,
        metadata=evaluator.metadata,
        is_public=evaluator.is_public,
        update=evaluator.update,
        base_experiment=base_experiment_name,
        base_experiment_id=evaluator.base_experiment_id,
        git_metadata_settings=evaluator.git_metadata_settings,
        repo_info=evaluator.repo_info,
        dataset=dataset,
    )


def run_evaluator_supports_progress() -> bool:
    try:
        from inspect import signature

        return "progress" in signature(run_evaluator).parameters
    except Exception:
        return False


async def run_evaluator_task(
    evaluator, position: int, no_send_logs: bool, progress_cb, supports_progress: bool
):
    experiment = None
    if not no_send_logs:
        experiment = _init_experiment_for_eval(evaluator)

    if progress_cb and not supports_progress:
        progress_cb("start", None)

    try:
        kwargs = {}
        if progress_cb and supports_progress:
            kwargs["progress"] = progress_cb
        return await run_evaluator(
            experiment,
            evaluator,
            None,
            [],
            **kwargs,
        )
    finally:
        if progress_cb and not supports_progress:
            progress_cb("stop", None)
        if experiment:
            experiment.flush()

async def run_once(
    files: list[str],
    no_send_logs: bool,
    sse: SseWriter | None,
    config: RunnerConfig,
) -> bool:
    evaluators, reporters = load_evaluators(files)
    if not evaluators and not config.list_only:
        message = "No evaluators found. Did you call Eval() in the file?"
        if sse:
            sse.send("error", serialize_error(message))
        else:
            eprint(message)
        return False

    evaluators = filter_evaluators(evaluators, config.filters)
    if config.list_only:
        for evaluator_instance in evaluators:
            print(evaluator_instance.evaluator.eval_name)
        return True

    supports_progress = run_evaluator_supports_progress()

    all_success = True
    for idx, evaluator_instance in enumerate(evaluators):
        try:
            resolved_reporter = resolve_reporter(
                getattr(evaluator_instance, "reporter", None),
                reporters,
            )
        except Exception as exc:
            all_success = False
            err = serialize_error(str(exc), traceback.format_exc())
            if sse:
                sse.send("error", err)
            else:
                eprint(err.get("message"))
            if config.terminate_on_failure:
                break
            continue

        progress_cb = create_progress_reporter(sse, evaluator_instance.evaluator.eval_name)
        try:
            result = await run_evaluator_task(
                evaluator_instance.evaluator,
                idx,
                no_send_logs,
                progress_cb,
                supports_progress,
            )
        except Exception as exc:
            all_success = False
            err = serialize_error(str(exc), traceback.format_exc())
            if sse:
                sse.send("error", err)
            else:
                eprint(err.get("message"))
            if config.terminate_on_failure:
                break
            continue

        if sse:
            sse.send("summary", format_summary(result.summary.as_dict()))
        elif config.jsonl:
            print(json.dumps(format_summary(result.summary.as_dict())))
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
    try:
        success = asyncio.run(run_once(files, local, sse, config))
        if sse:
            sse.send("done", {"success": success})
        return 0 if success else 1
    finally:
        if sse:
            sse.close()


if __name__ == "__main__":
    sys.exit(main())
