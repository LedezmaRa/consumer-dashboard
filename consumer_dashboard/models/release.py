"""Release metadata models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReleaseRecord:
    source_id: str
    report_slug: str
    release_date: str
    reference_period: str = ""
    artifact_path: str = ""
    status: str = "pending"
    notes: str = ""

