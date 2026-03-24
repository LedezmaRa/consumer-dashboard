"""Status reporting for source readiness and pipeline state."""

from __future__ import annotations

from consumer_dashboard.config.registry import list_automatable_sources, load_source_definitions
from consumer_dashboard.storage.state import StateStore


def render_status_report(settings) -> str:
    source_map = load_source_definitions(settings.manifest_path)
    state = StateStore(settings.state_dir).load()
    tracked = state.get("sources", {})
    lines = [
        "Consumer Dashboard Status",
        f"Project root: {settings.project_root}",
        f"Manifest: {settings.manifest_path}",
        "",
        "Known sources:",
    ]
    for source_id in sorted(source_map):
        source = source_map[source_id]
        current = tracked.get(source_id, {})
        status = current.get("status", "not_run")
        lines.append(
            f"- {source_id}: {status} | reports={len(source.report_slugs)} | automatable={source.is_automatable}"
        )
    lines.append("")
    lines.append(
        "Enabled sources: "
        + ", ".join(settings.enabled_sources or tuple(source.source_id for source in list_automatable_sources(settings.manifest_path)))
    )
    return "\n".join(lines)

