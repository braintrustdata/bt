#!/usr/bin/env python3
import argparse
import asyncio
import json
import os
import socket
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Callable
import importlib.util
import fnmatch
from pathlib import PurePosixPath

try:
    from braintrust import login
    from braintrust.framework import (
        BaseExperiment,
        EvaluatorInstance,
        _evals,
        _set_lazy_load,
        run_evaluator,
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
WATCHABLE_PYTHON_EXTENSIONS = {".py"}
WATCH_EXCLUDE_SEGMENTS = (
    "/site-packages/",
    "/dist-packages/",
    "/__pycache__/",
    "/.venv/",
    "/venv/",
)


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


def load_evaluators(files: list[str]) -> list[EvaluatorInstance]:
    evaluator_instances: list[EvaluatorInstance] = []
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
            finally:
                _evals.clear()

    return evaluator_instances


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

async def run_once(files: list[str], no_send_logs: bool, sse: SseWriter | None) -> bool:
    evaluators = load_evaluators(files)
    if not evaluators:
        message = "No evaluators found. Did you call Eval() in the file?"
        if sse:
            sse.send("error", serialize_error(message))
        else:
            eprint(message)
        return False

    supports_progress = run_evaluator_supports_progress()

    tasks = []
    progress_callbacks = []
    for idx, evaluator_instance in enumerate(evaluators):
        progress_cb = create_progress_reporter(sse, evaluator_instance.evaluator.eval_name)
        progress_callbacks.append(progress_cb)
        tasks.append(
            asyncio.create_task(
                run_evaluator_task(
                    evaluator_instance.evaluator, idx, no_send_logs, progress_cb, supports_progress
                )
            )
        )

    all_success = True
    for evaluator_instance, task, progress_cb in zip(evaluators, tasks, progress_callbacks):
        try:
            result = await task
        except Exception as exc:
            all_success = False
            err = serialize_error(str(exc), traceback.format_exc())
            if sse:
                sse.send("error", err)
            else:
                eprint(err.get("message"))
            continue

        if sse:
            sse.send("summary", format_summary(result.summary.as_dict()))
        else:
            print(result.summary)

        failures = [row for row in result.results if row.error]
        if failures:
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

    local = args.local or env_flag("BT_EVAL_LOCAL") or env_flag("BT_EVAL_NO_SEND_LOGS")
    files = args.files or ["."]

    if not local:
        login(api_key=args.api_key, org_name=args.org_name, app_url=args.app_url)

    sse = create_sse_writer()
    cwd = os.path.abspath(os.getcwd())
    try:
        success = asyncio.run(run_once(files, local, sse))
        if sse:
            sse.send("dependencies", {"files": collect_dependency_files(cwd, files)})
            sse.send("done", {"success": success})
        return 0 if success else 1
    finally:
        if sse:
            sse.close()


if __name__ == "__main__":
    sys.exit(main())
