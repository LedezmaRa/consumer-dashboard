"""Ingestion orchestration."""

from __future__ import annotations

from consumer_dashboard.config.registry import get_source_definition
from consumer_dashboard.sources import build_adapter
from consumer_dashboard.storage.filesystem import ensure_project_directories
from consumer_dashboard.storage.state import StateStore


def ingest_source(source_id: str, settings) -> dict:
    ensure_project_directories(settings)
    definition = get_source_definition(settings.manifest_path, source_id)
    adapter = build_adapter(definition.source_id, settings)
    result = adapter.fetch_latest(definition)
    StateStore(settings.state_dir).update_source(definition.source_id, result.status, result.message)
    return {
        "source_id": definition.source_id,
        "status": result.status,
        "message": result.message,
    }

