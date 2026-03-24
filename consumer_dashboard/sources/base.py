"""Base classes for source adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List

from consumer_dashboard.config.registry import SourceDefinition


@dataclass(frozen=True)
class AcquisitionResult:
    source_id: str
    status: str
    message: str
    artifacts: List[str] = field(default_factory=list)


class BaseSourceAdapter:
    source_id = "base"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        return AcquisitionResult(
            source_id=self.source_id,
            status="stub",
            message=(
                f"Adapter '{self.source_id}' is scaffolded but not connected yet. "
                f"Ready to implement {definition.source_name} ingestion next."
            ),
        )

    def backfill(self, definition: SourceDefinition, start: str, end: str) -> AcquisitionResult:
        return AcquisitionResult(
            source_id=self.source_id,
            status="stub",
            message=(
                f"Backfill for '{self.source_id}' is scaffolded but not connected yet "
                f"for range {start} -> {end}."
            ),
        )

