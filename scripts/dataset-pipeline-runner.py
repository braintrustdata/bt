#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import socket
import sys
import traceback
import uuid
from pathlib import Path
from typing import Any

try:
    import braintrust
    from braintrust import dataset_pipeline as braintrust_dataset_pipeline
    from braintrust.framework import call_user_fn
    from braintrust.logger import _internal_get_global_state, login_to_state
    from braintrust.trace import LocalTrace
except Exception as exc:  # pragma: no cover - runtime guard
    print(
        "Unable to import the braintrust package. Please install it in your Python environment.",
        file=sys.stderr,
    )
    print(str(exc), file=sys.stderr)
    sys.exit(1)


SOURCE_KEY_MAP = {
    "project_id": "projectId",
    "project_name": "projectName",
    "org_name": "orgName",
}
TARGET_KEY_MAP = {
    "project_id": "projectId",
    "project_name": "projectName",
    "org_name": "orgName",
    "dataset_name": "datasetName",
}

_DEFERRED_ATTACHMENT_DIR: Path | None = None


class DeferredJSONAttachment:
    def __init__(
        self,
        data: Any,
        *,
        filename: str = "data.json",
        pretty: bool = False,
    ) -> None:
        self._reference = deferred_json_attachment_reference(data, filename, pretty)

    @property
    def reference(self) -> dict[str, Any]:
        return self._reference

    def upload(self) -> dict[str, Any]:
        return {"upload_status": "done", "deferred": True}

    def debug_info(self) -> dict[str, Any]:
        return {"reference": self._reference}


def set_deferred_attachment_dir(path: str | None) -> None:
    global _DEFERRED_ATTACHMENT_DIR
    _DEFERRED_ATTACHMENT_DIR = Path(path).resolve() if path else None
    if _DEFERRED_ATTACHMENT_DIR is not None:
        _DEFERRED_ATTACHMENT_DIR.mkdir(parents=True, exist_ok=True)


def deferred_json_attachment_reference(
    data: Any,
    filename: str,
    pretty: bool,
) -> dict[str, Any]:
    serialized = json.dumps(data, indent=2 if pretty else None, ensure_ascii=False)
    marker: dict[str, Any] = {
        "type": "braintrust_deferred_attachment",
        "kind": "json",
        "filename": filename,
        "content_type": "application/json",
    }
    if _DEFERRED_ATTACHMENT_DIR is None:
        marker["data"] = data
        marker["pretty"] = pretty
        return marker

    path = _DEFERRED_ATTACHMENT_DIR / f"{uuid.uuid4()}.json"
    path.write_text(serialized, encoding="utf-8")
    marker["path"] = str(path)
    return marker


def install_deferred_attachment_shims() -> None:
    braintrust.JSONAttachment = DeferredJSONAttachment
    import braintrust.logger as logger

    logger.JSONAttachment = DeferredJSONAttachment


def normalize_deferred_attachments(value: Any) -> Any:
    if isinstance(value, DeferredJSONAttachment):
        return value.reference
    if isinstance(value, dict):
        return {
            key: normalize_deferred_attachments(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [normalize_deferred_attachments(item) for item in value]
    return value


class SseWriter:
    def __init__(self, sock: socket.socket):
        self._socket = sock

    def send(self, event: str, payload: Any) -> None:
        data = payload if isinstance(payload, str) else json.dumps(payload, separators=(",", ":"))
        frame = f"event: {event}\ndata: {data}\n\n".encode("utf-8")
        self._socket.sendall(frame)

    def close(self) -> None:
        self._socket.close()


def create_sse_writer() -> SseWriter | None:
    sock_path = os.getenv("BT_DATASET_PIPELINE_SSE_SOCK")
    if not sock_path:
        addr = os.getenv("BT_DATASET_PIPELINE_SSE_ADDR")
        if not addr:
            return None
        if ":" not in addr:
            raise ValueError("BT_DATASET_PIPELINE_SSE_ADDR must be in host:port format")
        host, port_str = addr.rsplit(":", 1)
        sock = socket.create_connection((host, int(port_str)))
        return SseWriter(sock)
    try:
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(sock_path)
        return SseWriter(sock)
    except Exception as exc:
        print(f"Failed to connect to dataset pipeline socket: {exc}", file=sys.stderr)
        return None


def camelize_mapping(value: Any, key_map: dict[str, str]) -> Any:
    if not isinstance(value, dict):
        return value
    return {
        key_map.get(key, key): camelize_mapping(item, key_map)
        for key, item in value.items()
    }


def object_get(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def pipeline_source(pipeline: Any) -> dict[str, Any]:
    source = object_get(pipeline, "source")
    if not isinstance(source, dict):
        raise RuntimeError("Dataset pipeline source is required.")
    return source


def pipeline_transform(pipeline: Any) -> Any:
    transform = object_get(pipeline, "transform")
    if not callable(transform):
        raise RuntimeError("Dataset pipeline transform must be callable.")
    return transform


def load_pipeline_file(file: str) -> Any:
    absolute = os.path.abspath(file)
    cwd = os.getcwd()
    file_dir = os.path.dirname(absolute)
    for path in (file_dir, cwd):
        if path and path not in sys.path:
            sys.path.insert(0, path)

    module_name = f"_bt_dataset_pipeline_{abs(hash(absolute))}"
    spec = importlib.util.spec_from_file_location(module_name, absolute)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load {file}.")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def collect_pipelines() -> list[Any]:
    pipelines: list[Any] = []
    seen: set[int] = set()

    registered = getattr(braintrust_dataset_pipeline, "_DATASET_PIPELINES", [])
    for pipeline in registered:
        if id(pipeline) not in seen:
            seen.add(id(pipeline))
            pipelines.append(pipeline)

    return pipelines


def select_pipeline(pipelines: list[Any], name: str | None) -> Any:
    if name:
        matches = [
            pipeline
            for pipeline in pipelines
            if object_get(pipeline, "name") == name
        ]
        if not matches:
            raise RuntimeError(f"No dataset pipeline named {json.dumps(name)} found.")
        if len(matches) > 1:
            raise RuntimeError(
                f"Multiple dataset pipelines named {json.dumps(name)} found."
            )
        return matches[0]

    if not pipelines:
        raise RuntimeError("No dataset pipelines found. Did you call DatasetPipeline()?")
    if len(pipelines) > 1:
        names = ", ".join(
            object_get(pipeline, "name") or "<unnamed>" for pipeline in pipelines
        )
        raise RuntimeError(f"Multiple dataset pipelines found ({names}). Pass --name.")
    return pipelines[0]


def parse_stage() -> str:
    stage = os.getenv("BT_DATASET_PIPELINE_STAGE")
    if stage in {"inspect", "transform"}:
        return stage
    raise RuntimeError("BT_DATASET_PIPELINE_STAGE must be inspect or transform.")


def read_request() -> dict[str, Any]:
    text = sys.stdin.read().strip()
    if not text:
        return {}
    value = json.loads(text)
    if not isinstance(value, dict):
        raise RuntimeError("Dataset pipeline runner request must be an object.")
    return value


def write_response(value: Any, sse: SseWriter | None) -> None:
    if sse is not None:
        sse.send("response", value)
        sse.close()
    else:
        print(json.dumps(value, separators=(",", ":")))


def write_progress(sse: SseWriter | None, rows: int) -> None:
    if sse is None:
        return
    sse.send(
        "progress",
        {
            "type": "dataset_pipeline_progress",
            "kind": "candidate",
            "rows": rows,
        },
    )


def require_array_field(request: dict[str, Any], field: str) -> list[Any]:
    value = request.get(field)
    if not isinstance(value, list):
        raise RuntimeError(f"Request field {field} must be an array.")
    return value


def require_string_field(request: dict[str, Any], field: str) -> str:
    value = request.get(field)
    if not isinstance(value, str):
        raise RuntimeError(f"Request field {field} must be a string.")
    return value


def optional_positive_integer_field(request: dict[str, Any], field: str) -> int | None:
    value = request.get(field)
    if value is None:
        return None
    if not isinstance(value, int) or value <= 0:
        raise RuntimeError(f"Request field {field} must be a positive integer.")
    return value


def set_optional_env(name: str, value: Any) -> None:
    if isinstance(value, str) and value:
        os.environ[name] = value
    else:
        os.environ.pop(name, None)


def merged_source(pipeline: Any, source_override: Any) -> dict[str, Any]:
    source = camelize_mapping(pipeline_source(pipeline), SOURCE_KEY_MAP)
    if isinstance(source_override, dict):
        return {**source, **source_override}
    return source


def state_for_org(org_name: str | None) -> Any:
    state = _internal_get_global_state()
    if not org_name:
        state.login()
        return state
    if not getattr(state, "logged_in", False):
        state.login(org_name=org_name)
        return state
    if getattr(state, "org_name", None) == org_name:
        return state
    return login_to_state(org_name=org_name)


def ref_root_span_id(ref: Any) -> str:
    if not isinstance(ref, dict) or not isinstance(ref.get("root_span_id"), str):
        raise RuntimeError("Discovery ref is missing root_span_id.")
    return ref["root_span_id"]


def ref_span_row_id(ref: Any) -> str | None:
    if isinstance(ref, dict) and isinstance(ref.get("id"), str):
        return ref["id"]
    return None


def hydrate_discovery_refs(
    pipeline: Any,
    source_override: Any,
    source_project_id: str,
    refs: list[Any],
) -> list[dict[str, Any]]:
    source = merged_source(pipeline, source_override)
    state = state_for_org(source.get("orgName"))
    candidates: list[dict[str, Any]] = []
    traces_by_root_span_id: dict[str, LocalTrace] = {}
    for ref in refs:
        root_span_id = ref_root_span_id(ref)
        row_id = ref_span_row_id(ref)
        trace = traces_by_root_span_id.get(root_span_id)
        if trace is None:
            trace = LocalTrace(
                object_type="project_logs",
                object_id=source_project_id,
                root_span_id=root_span_id,
                ensure_spans_flushed=None,
                state=state,
            )
            traces_by_root_span_id[root_span_id] = trace
        candidate: dict[str, Any] = {
            "trace": trace,
        }
        origin = ref.get("origin") if isinstance(ref, dict) else None
        if isinstance(origin, dict):
            candidate["origin"] = origin
        if row_id:
            candidate["id"] = row_id
        candidates.append(candidate)
    return candidates


def span_attr(span: Any, name: str) -> Any:
    if isinstance(span, dict):
        return span.get(name)
    return getattr(span, name, None)


async def source_row_for_candidate(candidate: dict[str, Any]) -> Any | None:
    row_id = candidate.get("id")
    if not isinstance(row_id, str):
        return None

    trace = candidate["trace"]
    spans = await trace.get_spans(include_scorers=True)
    for span in spans:
        if row_id in {span_attr(span, "id"), span_attr(span, "span_id")}:
            return span
    raise RuntimeError(f"Source span row {row_id!r} was not found in hydrated trace.")


async def transform_args_for_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    row = await source_row_for_candidate(candidate)
    args = {
        "input": span_attr(row, "input"),
        "output": span_attr(row, "output"),
        "expected": span_attr(row, "expected"),
        "metadata": span_attr(row, "metadata"),
        "trace": candidate["trace"],
    }
    row_id = candidate.get("id")
    if isinstance(row_id, str):
        args["id"] = row_id
    return args


def normalize_transform_result(result: Any) -> list[Any]:
    if result is None:
        return []
    if isinstance(result, list):
        return result
    return [result]


def candidate_fallback_id(candidate: dict[str, Any]) -> str | None:
    row_id = candidate.get("id")
    if isinstance(row_id, str):
        return row_id
    trace = candidate.get("trace")
    config = trace.get_configuration() if hasattr(trace, "get_configuration") else None
    if isinstance(config, dict) and isinstance(config.get("root_span_id"), str):
        return config["root_span_id"]
    return None


def with_pipeline_defaults(
    row: Any,
    candidate: dict[str, Any],
    row_index: int | None,
) -> dict[str, Any]:
    row = normalize_deferred_attachments(row)
    if not isinstance(row, dict):
        raise RuntimeError("Dataset pipeline transform must return an object row.")
    output = {key: value for key, value in row.items() if key != "origin"}
    fallback_id = candidate_fallback_id(candidate)
    if "id" not in output and fallback_id:
        output["id"] = fallback_id if row_index is None else f"{fallback_id}:{row_index}"
    if "origin" in candidate:
        output["origin"] = candidate["origin"]
    return output


async def transform_refs(
    pipeline: Any,
    source_override: Any,
    source_project_id: str,
    refs: list[Any],
    max_concurrency: int = 16,
    sse: SseWriter | None = None,
) -> list[dict[str, Any]]:
    if max_concurrency <= 0:
        raise RuntimeError("maxConcurrency must be a positive integer.")
    transform = pipeline_transform(pipeline)
    candidates = hydrate_discovery_refs(pipeline, source_override, source_project_id, refs)
    transformed_rows: list[list[dict[str, Any]]] = [[] for _ in candidates]
    semaphore = asyncio.Semaphore(max_concurrency)

    async def run_one(index: int, candidate: dict[str, Any]) -> None:
        async with semaphore:
            transform_args = await transform_args_for_candidate(candidate)
            result = await call_user_fn(
                asyncio.get_running_loop(),
                transform,
                **transform_args,
            )
            rows = normalize_transform_result(result)
            transformed_rows[index] = [
                with_pipeline_defaults(
                    row,
                    candidate,
                    row_index if len(rows) > 1 else None,
                )
                for row_index, row in enumerate(rows)
            ]
            write_progress(sse, len(transformed_rows[index]))

    await asyncio.gather(
        *(run_one(index, candidate) for index, candidate in enumerate(candidates))
    )
    return [row for rows in transformed_rows for row in rows]


async def main() -> None:
    if len(sys.argv) < 2:
        raise RuntimeError("Pipeline file is required.")

    stage = parse_stage()
    if stage == "transform":
        install_deferred_attachment_shims()

    load_pipeline_file(sys.argv[1])
    pipeline = select_pipeline(
        collect_pipelines(),
        os.getenv("BT_DATASET_PIPELINE_NAME") or None,
    )
    sse = create_sse_writer()

    if stage == "inspect":
        write_response(
            {
                "name": object_get(pipeline, "name"),
                "source": camelize_mapping(object_get(pipeline, "source"), SOURCE_KEY_MAP),
                "target": camelize_mapping(object_get(pipeline, "target"), TARGET_KEY_MAP),
            },
            sse,
        )
    elif stage == "transform":
        request = read_request()
        attachment_dir = request.get("attachmentDir")
        if attachment_dir is not None and not isinstance(attachment_dir, str):
            raise RuntimeError("Request field attachmentDir must be a string.")
        set_deferred_attachment_dir(attachment_dir)
        refs = require_array_field(request, "refs")
        source_project_id = require_string_field(request, "sourceProjectId")
        source_override = (
            request.get("source") if isinstance(request.get("source"), dict) else None
        )
        source_for_env = (
            source_override
            if isinstance(source_override, dict)
            else camelize_mapping(object_get(pipeline, "source"), SOURCE_KEY_MAP)
        )
        set_optional_env(
            "BT_DATASET_PIPELINE_SOURCE_ORG_NAME",
            source_for_env.get("orgName") if isinstance(source_for_env, dict) else None,
        )
        rows = await transform_refs(
            pipeline,
            source_override,
            source_project_id,
            refs,
            optional_positive_integer_field(request, "maxConcurrency") or 16,
            sse,
        )
        write_response({"candidates": len(refs), "rowCount": len(rows), "rows": rows}, sse)
    else:
        raise RuntimeError(f"Unsupported dataset pipeline stage: {stage}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
