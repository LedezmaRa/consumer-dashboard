"""End-to-end pipeline orchestration."""

from __future__ import annotations

from consumer_dashboard.config.registry import list_automatable_sources
from consumer_dashboard.pipeline.derive import derive_metrics
from consumer_dashboard.pipeline.ingest import ingest_source
from consumer_dashboard.pipeline.normalize import normalize_source


def refresh_pipeline(settings) -> dict:
    if settings.enabled_sources:
        source_ids = list(settings.enabled_sources)
    else:
        source_ids = [source.source_id for source in list_automatable_sources(settings.manifest_path)]

    actions = []
    for source_id in source_ids:
        actions.append(ingest_source(source_id, settings))
        actions.append(normalize_source(source_id, settings))

    derived = derive_metrics(settings)
    return {
        "status": "completed",
        "sources": source_ids,
        "actions": actions,
        "message": (
            f"Refresh ran for {len(source_ids)} source(s). "
            f"Derive step computed {derived['series_count']} derived series."
        ),
    }
