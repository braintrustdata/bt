#!/usr/bin/env python3
"""Generate, extract, and validate eval-improvement-loop JSONL records."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1
KINDS = {"baseline", "candidate", "confirmation"}
SCOPES = {"full", "sample"}
STATUSES = {"advance", "keep", "discard", "crash", "checks_failed"}
DIRECTIONS = {"higher", "lower"}
PRIMARY_KINDS = {"score", "metric"}
MODES = {"offline", "experiment", "hill_climb"}
EXPERIMENT_KEYS = {
    "project_name",
    "project_id",
    "experiment_name",
    "experiment_id",
    "experiment_url",
    "comparison_experiment_name",
}
RECORD_KEYS = {
    "schema_version",
    "run",
    "timestamp",
    "kind",
    "scope",
    "status",
    "mode",
    "evaluator",
    "experiment",
    "primary",
    "scores",
    "metrics",
    "sample",
    "base_commit",
    "commit",
    "revert_commit",
    "hypothesis",
    "changed_files",
    "reason",
    "next",
    "eval_output",
}


class ValidationError(ValueError):
    pass


def fail(message: str) -> None:
    raise ValidationError(message)


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def require_string(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        fail(f"{field} must be a non-empty string")
    return value


def validate_number_map(value: Any, field: str) -> None:
    if not isinstance(value, dict):
        fail(f"{field} must be an object")
    for name, number in value.items():
        require_string(name, f"{field} key")
        if not is_number(number):
            fail(f"{field}.{name} must be a finite number")


def validate_record(record: Any, *, line: int | None = None) -> None:
    prefix = f"line {line}: " if line is not None else ""
    try:
        if not isinstance(record, dict):
            fail("record must be a JSON object")
        unknown = sorted(set(record) - RECORD_KEYS)
        if unknown:
            fail(f"unknown field(s): {', '.join(unknown)}")
        missing = sorted(
            {
                "schema_version",
                "run",
                "timestamp",
                "kind",
                "scope",
                "status",
                "mode",
                "primary",
                "scores",
                "metrics",
                "hypothesis",
                "changed_files",
            }
            - set(record)
        )
        if missing:
            fail(f"missing field(s): {', '.join(missing)}")
        if record["schema_version"] != SCHEMA_VERSION:
            fail(f"schema_version must be {SCHEMA_VERSION}")
        if not isinstance(record["run"], int) or isinstance(record["run"], bool) or record["run"] < 1:
            fail("run must be a positive integer")
        timestamp = require_string(record["timestamp"], "timestamp")
        try:
            parsed_timestamp = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            fail("timestamp must be ISO 8601")
        if parsed_timestamp.tzinfo is None:
            fail("timestamp must include a timezone")
        if record["kind"] not in KINDS:
            fail(f"kind must be one of: {', '.join(sorted(KINDS))}")
        if record["scope"] not in SCOPES:
            fail(f"scope must be one of: {', '.join(sorted(SCOPES))}")
        if record["status"] not in STATUSES:
            fail(f"status must be one of: {', '.join(sorted(STATUSES))}")
        if record["mode"] not in MODES:
            fail(f"mode must be one of: {', '.join(sorted(MODES))}")
        if "evaluator" in record and record["evaluator"] is not None:
            require_string(record["evaluator"], "evaluator")
        if "experiment" in record:
            experiment = record["experiment"]
            if not isinstance(experiment, dict) or set(experiment) != EXPERIMENT_KEYS:
                fail("experiment must contain exactly the documented experiment identity fields")
            for field, value in experiment.items():
                if value is not None:
                    require_string(value, f"experiment.{field}")
            if record["mode"] == "offline" and any(
                experiment[field] is not None
                for field in ("experiment_id", "experiment_url")
            ):
                fail("offline records cannot reference an uploaded experiment")

        primary = record["primary"]
        if not isinstance(primary, dict) or set(primary) != {"name", "kind", "value", "direction"}:
            fail("primary must contain exactly name, kind, value, and direction")
        primary_name = require_string(primary["name"], "primary.name")
        if primary["kind"] not in PRIMARY_KINDS:
            fail(f"primary.kind must be one of: {', '.join(sorted(PRIMARY_KINDS))}")
        if primary["direction"] not in DIRECTIONS:
            fail(f"primary.direction must be one of: {', '.join(sorted(DIRECTIONS))}")
        if primary["value"] is not None and not is_number(primary["value"]):
            fail("primary.value must be a finite number or null")
        if primary["value"] is None and record["status"] not in {"crash", "checks_failed"}:
            fail("primary.value may be null only for crash or checks_failed")

        validate_number_map(record["scores"], "scores")
        validate_number_map(record["metrics"], "metrics")
        primary_values = record["scores"] if primary["kind"] == "score" else record["metrics"]
        if primary["value"] is not None:
            if primary_name not in primary_values:
                fail(f"primary metric {primary_name!r} is missing from {primary['kind']} values")
            if primary_values[primary_name] != primary["value"]:
                fail("primary.value does not match the extracted value")

        if record["scope"] == "sample":
            sample = record.get("sample")
            if not isinstance(sample, dict) or set(sample) != {"count", "seed"}:
                fail("sample scope requires sample with exactly count and seed")
            if not isinstance(sample["count"], int) or isinstance(sample["count"], bool) or sample["count"] < 1:
                fail("sample.count must be a positive integer")
            if not isinstance(sample["seed"], int) or isinstance(sample["seed"], bool) or sample["seed"] < 0:
                fail("sample.seed must be a non-negative integer")
        elif "sample" in record:
            fail("sample is only valid when scope is sample")

        for field in (
            "base_commit",
            "commit",
            "revert_commit",
            "reason",
            "next",
            "eval_output",
        ):
            if field in record and record[field] is not None:
                require_string(record[field], field)
        require_string(record["hypothesis"], "hypothesis")
        if not isinstance(record["changed_files"], list):
            fail("changed_files must be an array")
        for index, path in enumerate(record["changed_files"]):
            require_string(path, f"changed_files[{index}]")
        if len(record["changed_files"]) != len(set(record["changed_files"])):
            fail("changed_files must not contain duplicates")
        if record["kind"] == "candidate" and not record.get("commit"):
            fail("every candidate requires the evaluated commit")
        if record["status"] == "advance" and record["scope"] != "sample":
            fail("advance is only valid for a sampled candidate moving to full validation")
        if record["kind"] == "candidate" and record["status"] == "keep" and record["scope"] != "full":
            fail("a kept candidate must be validated on the full dataset")
        if record["status"] in {"keep", "advance"} and record.get("revert_commit"):
            fail("kept or advancing records cannot have revert_commit")
        if (
            record["kind"] == "candidate"
            and record["status"] in {"discard", "crash", "checks_failed"}
            and not record.get("revert_commit")
        ):
            fail("a discarded or failed candidate requires revert_commit")
    except ValidationError as error:
        raise ValidationError(prefix + str(error)) from error


def load_log(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    previous_run = 0
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw.strip():
            fail(f"line {line_number}: blank lines are not allowed")
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as error:
            fail(f"line {line_number}: invalid JSON: {error.msg}")
        validate_record(record, line=line_number)
        run = record["run"]
        expected_run = previous_run + 1
        if run != expected_run:
            fail(f"line {line_number}: run must be {expected_run}, got {run}")
        previous_run = run
        records.append(record)
    return records


def extract_values(container: Any, value_field: str, field: str) -> dict[str, float]:
    if container is None:
        return {}
    if not isinstance(container, dict):
        fail(f"eval summary {field} must be an object")
    values: dict[str, float] = {}
    for key, item in container.items():
        if not isinstance(item, dict) or not is_number(item.get(value_field)):
            fail(f"eval summary {field}.{key}.{value_field} must be a finite number")
        values[key] = item[value_field]
    return values


def extract_summaries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        fail(f"eval output does not exist: {path}")
    summaries: list[dict[str, Any]] = []
    for line_number, raw in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
        try:
            value = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(value, dict) or not isinstance(value.get("scores"), dict):
            continue
        evaluator = value.get("experimentName") or value.get("experiment_name")
        if evaluator is not None and not isinstance(evaluator, str):
            fail(f"eval output line {line_number}: experimentName must be a string")
        summaries.append(
            {
                "evaluator": evaluator,
                "experiment": {
                    "project_name": value.get("projectName", value.get("project_name")),
                    "project_id": value.get("projectId", value.get("project_id")),
                    "experiment_name": value.get("experimentName", value.get("experiment_name")),
                    "experiment_id": value.get("experimentId", value.get("experiment_id")),
                    "experiment_url": value.get("experimentUrl", value.get("experiment_url")),
                    "comparison_experiment_name": value.get(
                        "comparisonExperimentName",
                        value.get("comparison_experiment_name"),
                    ),
                },
                "scores": extract_values(value.get("scores"), "score", "scores"),
                "metrics": extract_values(value.get("metrics"), "metric", "metrics"),
                "is_final": value.get("isFinal", value.get("is_final")),
                "run_mode": value.get("runMode", value.get("run_mode")),
            }
        )
    if not summaries:
        fail(f"no eval summary objects with scores found in {path}")
    return summaries


def select_summary(summaries: list[dict[str, Any]], evaluator: str | None, primary_name: str, primary_kind: str) -> dict[str, Any]:
    candidates = summaries
    if evaluator is not None:
        candidates = [summary for summary in summaries if summary["evaluator"] == evaluator]
        if not candidates:
            available = sorted(str(summary["evaluator"]) for summary in summaries)
            fail(f"evaluator {evaluator!r} not found; available: {', '.join(available)}")
    field = "scores" if primary_kind == "score" else "metrics"
    candidates = [summary for summary in candidates if primary_name in summary[field]]
    if not candidates:
        fail(f"primary {primary_kind} {primary_name!r} was not found")
    if len(candidates) > 1:
        names = [str(summary["evaluator"]) for summary in candidates]
        fail(f"primary {primary_kind} {primary_name!r} is ambiguous across evaluators: {', '.join(names)}; pass --evaluator")
    return candidates[0]


def utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def command_extract(args: argparse.Namespace) -> None:
    summaries = extract_summaries(args.input)
    print(json.dumps(summaries, sort_keys=True, separators=(",", ":")))


def command_validate(args: argparse.Namespace) -> None:
    records = load_log(args.log)
    print(f"valid: {len(records)} record(s)")


def command_append(args: argparse.Namespace) -> None:
    records = load_log(args.log)
    expected_run = records[-1]["run"] + 1 if records else 1
    if args.run != expected_run:
        fail(f"run must be {expected_run}, got {args.run}")

    scores: dict[str, float] = {}
    metrics: dict[str, float] = {}
    evaluator = args.evaluator
    experiment: dict[str, str | None] | None = None
    primary_value = args.primary_value
    if args.eval_output is not None:
        summary = select_summary(
            extract_summaries(args.eval_output),
            args.evaluator,
            args.primary_name,
            args.primary_kind,
        )
        evaluator = summary["evaluator"]
        experiment = summary["experiment"]
        scores = summary["scores"]
        metrics = summary["metrics"]
        if args.scope == "full" and summary["is_final"] is False:
            fail("full record cannot use a non-final eval summary")
        if args.scope == "sample" and summary["is_final"] is True:
            fail("sample record cannot use a final eval summary")
        values = scores if args.primary_kind == "score" else metrics
        extracted = values[args.primary_name]
        if primary_value is not None and primary_value != extracted:
            fail("--primary-value does not match the eval output")
        primary_value = extracted
    elif primary_value is not None:
        target = scores if args.primary_kind == "score" else metrics
        target[args.primary_name] = primary_value

    record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run": args.run,
        "timestamp": args.timestamp or utc_timestamp(),
        "kind": args.kind,
        "scope": args.scope,
        "status": args.status,
        "mode": args.mode,
        "primary": {
            "name": args.primary_name,
            "kind": args.primary_kind,
            "value": primary_value,
            "direction": args.direction,
        },
        "scores": scores,
        "metrics": metrics,
        "hypothesis": args.hypothesis,
        "changed_files": sorted(set(args.changed_file)),
    }
    optional = {
        "evaluator": evaluator,
        "experiment": experiment,
        "base_commit": args.base_commit,
        "commit": args.commit,
        "revert_commit": args.revert_commit,
        "reason": args.reason,
        "next": args.next,
        "eval_output": str(args.eval_output) if args.eval_output is not None else None,
    }
    record.update({key: value for key, value in optional.items() if value is not None})
    if args.scope == "sample":
        if args.sample_count is None or args.sample_seed is None:
            fail("sample scope requires --sample-count and --sample-seed")
        record["sample"] = {"count": args.sample_count, "seed": args.sample_seed}
    elif args.sample_count is not None or args.sample_seed is not None:
        fail("sample options require --scope sample")

    validate_record(record)
    args.log.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    with args.log.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(line)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("extract", help="extract normalized summaries from bt eval --jsonl output")
    extract.add_argument("--input", required=True, type=Path)
    extract.set_defaults(func=command_extract)

    validate = subparsers.add_parser("validate", help="validate an eval-loop log")
    validate.add_argument("--log", required=True, type=Path)
    validate.set_defaults(func=command_validate)

    append = subparsers.add_parser("append", help="generate, validate, and append one compact JSONL record")
    append.add_argument("--log", required=True, type=Path)
    append.add_argument("--eval-output", type=Path)
    append.add_argument("--run", required=True, type=int)
    append.add_argument("--kind", required=True, choices=sorted(KINDS))
    append.add_argument("--scope", required=True, choices=sorted(SCOPES))
    append.add_argument("--status", required=True, choices=sorted(STATUSES))
    append.add_argument("--mode", required=True, choices=sorted(MODES))
    append.add_argument("--primary-name", required=True)
    append.add_argument("--primary-kind", choices=sorted(PRIMARY_KINDS), default="score")
    append.add_argument("--primary-value", type=float)
    append.add_argument("--direction", required=True, choices=sorted(DIRECTIONS))
    append.add_argument("--evaluator")
    append.add_argument("--hypothesis", required=True)
    append.add_argument("--changed-file", action="append", default=[])
    append.add_argument("--base-commit")
    append.add_argument("--commit")
    append.add_argument("--revert-commit")
    append.add_argument("--reason")
    append.add_argument("--next")
    append.add_argument("--sample-count", type=int)
    append.add_argument("--sample-seed", type=int)
    append.add_argument("--timestamp", help="ISO 8601 timestamp; defaults to current UTC time")
    append.set_defaults(func=command_append)
    return parser


def main() -> int:
    try:
        args = build_parser().parse_args()
        args.func(args)
        return 0
    except ValidationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
