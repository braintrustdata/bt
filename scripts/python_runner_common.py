#!/usr/bin/env python3
import importlib.util
import os
import sys
from types import ModuleType


def normalize_file_list(files: list[str]) -> list[str]:
    unique: set[str] = set()
    for file_path in files:
        unique.add(os.path.abspath(file_path))
    return sorted(unique)


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


def import_file(module_name: str, file_path: str, extra_paths: list[str]) -> ModuleType:
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
    return module


def purge_local_modules(cwd: str, preserve_modules: set[str] | None = None) -> None:
    preserved = preserve_modules or set()
    cwd_abs = os.path.abspath(cwd)
    for module_name, module in list(sys.modules.items()):
        if module_name in preserved:
            continue
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        candidate = module_file[:-1] if module_file.endswith(".pyc") else module_file
        candidate_abs = os.path.abspath(candidate)
        if not os.path.isfile(candidate_abs):
            continue
        # Skip installed packages inside virtualenvs under cwd (e.g. .venv/lib/.../site-packages).
        if os.sep + "site-packages" + os.sep in candidate_abs:
            continue
        # Skip bt runner scripts materialised under .bt/.
        if os.sep + ".bt" + os.sep in candidate_abs:
            continue
        try:
            common = os.path.commonpath([candidate_abs, cwd_abs])
        except ValueError:
            continue
        if common == cwd_abs:
            sys.modules.pop(module_name, None)


def collect_python_sources(cwd: str, input_file: str) -> list[str]:
    sources: set[str] = set()
    input_abs = os.path.abspath(input_file)
    sources.add(input_abs)

    for module in list(sys.modules.values()):
        module_file = getattr(module, "__file__", None)
        if not module_file:
            continue
        candidate = module_file[:-1] if module_file.endswith(".pyc") else module_file
        candidate_abs = os.path.abspath(candidate)
        if not os.path.isfile(candidate_abs):
            continue
        if not candidate_abs.endswith(".py"):
            continue
        # Skip installed packages inside virtualenvs under cwd (e.g. .venv/lib/.../site-packages).
        if os.sep + "site-packages" + os.sep in candidate_abs:
            continue
        # Skip bt runner scripts materialised under .bt/.
        if os.sep + ".bt" + os.sep in candidate_abs:
            continue
        try:
            common = os.path.commonpath([candidate_abs, cwd])
        except ValueError:
            continue
        if common != cwd:
            continue
        sources.add(candidate_abs)

    return sorted(sources)


def python_version() -> str:
    return f"{sys.version_info.major}.{sys.version_info.minor}"
