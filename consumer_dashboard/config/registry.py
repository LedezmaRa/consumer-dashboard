"""Load report and source definitions from the manifest."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", value.strip().lower())
    return slug.strip("_")


@dataclass(frozen=True)
class ReportDefinition:
    report_slug: str
    report_name: str
    layer: str
    frequency: str
    priority: int
    source_owner: str
    primary_source_url: str
    acquisition_mode: str
    automation_status: str
    notes: str

    @property
    def source_id(self) -> str:
        return slugify(self.source_owner)


@dataclass(frozen=True)
class SourceDefinition:
    source_id: str
    source_name: str
    acquisition_modes: tuple[str, ...]
    automation_statuses: tuple[str, ...]
    report_slugs: tuple[str, ...]
    report_names: tuple[str, ...]
    primary_urls: tuple[str, ...]

    @property
    def is_automatable(self) -> bool:
        blocked = {"manual_or_licensed", "compute_in_pipeline"}
        return any(status not in blocked for status in self.automation_statuses)


def load_report_definitions(manifest_path: Path) -> List[ReportDefinition]:
    with manifest_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        reports = []
        for row in reader:
            reports.append(
                ReportDefinition(
                    report_slug=row["report_slug"],
                    report_name=row["report_name"],
                    layer=row["layer"],
                    frequency=row["frequency"],
                    priority=int(row["priority"]),
                    source_owner=row["source_owner"],
                    primary_source_url=row["primary_source_url"],
                    acquisition_mode=row["acquisition_mode"],
                    automation_status=row["automation_status"],
                    notes=row["notes"],
                )
            )
        return reports


def build_source_definitions(reports: Iterable[ReportDefinition]) -> Dict[str, SourceDefinition]:
    grouped: Dict[str, Dict[str, object]] = {}
    for report in reports:
        bucket = grouped.setdefault(
            report.source_id,
            {
                "source_name": report.source_owner,
                "acquisition_modes": set(),
                "automation_statuses": set(),
                "report_slugs": [],
                "report_names": [],
                "primary_urls": [],
            },
        )
        bucket["acquisition_modes"].add(report.acquisition_mode)
        bucket["automation_statuses"].add(report.automation_status)
        bucket["report_slugs"].append(report.report_slug)
        bucket["report_names"].append(report.report_name)
        if report.primary_source_url:
            bucket["primary_urls"].append(report.primary_source_url)

    source_map: Dict[str, SourceDefinition] = {}
    for source_id, bucket in grouped.items():
        source_map[source_id] = SourceDefinition(
            source_id=source_id,
            source_name=str(bucket["source_name"]),
            acquisition_modes=tuple(sorted(bucket["acquisition_modes"])),
            automation_statuses=tuple(sorted(bucket["automation_statuses"])),
            report_slugs=tuple(bucket["report_slugs"]),
            report_names=tuple(bucket["report_names"]),
            primary_urls=tuple(dict.fromkeys(bucket["primary_urls"])),
        )
    return source_map


def load_source_definitions(manifest_path: Path) -> Dict[str, SourceDefinition]:
    return build_source_definitions(load_report_definitions(manifest_path))


def get_source_definition(manifest_path: Path, source_id: str) -> SourceDefinition:
    source_map = load_source_definitions(manifest_path)
    key = slugify(source_id)
    if key not in source_map:
        available = ", ".join(sorted(source_map))
        raise KeyError(f"Unknown source '{source_id}'. Available sources: {available}")
    return source_map[key]


def list_automatable_sources(manifest_path: Path) -> List[SourceDefinition]:
    source_map = load_source_definitions(manifest_path)
    return [
        source
        for source in sorted(source_map.values(), key=lambda item: item.source_id)
        if source.source_id != "derived" and source.is_automatable
    ]

