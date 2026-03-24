"""Dashboard snapshot models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DashboardSnapshot:
    snapshot_name: str
    as_of_date: str
    frequency: str
    metric_label: str
    latest_value: str
    prior_value: str
    trend: str
    notes: str = ""


@dataclass(frozen=True)
class RegimeAssessment:
    month: str
    regime: str
    confidence_level: str
    rationale: str


@dataclass(frozen=True)
class MemoOutput:
    period: str
    summary: str
    generated_at: str

