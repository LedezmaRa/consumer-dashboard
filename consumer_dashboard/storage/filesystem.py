"""Filesystem helpers for the local pipeline."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_project_directories(settings) -> None:
    ensure_directory(settings.data_dir)
    ensure_directory(settings.raw_dir)
    ensure_directory(settings.processed_dir)
    ensure_directory(settings.state_dir)


def write_json(path: Path, payload: Any) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def latest_json_file(path: Path) -> Path | None:
    if not path.exists():
        return None
    candidates = [item for item in path.rglob("*.json") if item.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def latest_directory(path: Path) -> Path | None:
    if not path.exists():
        return None
    candidates = [item for item in path.iterdir() if item.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.name)
