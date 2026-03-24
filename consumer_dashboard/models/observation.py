"""Canonical observation models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Observation:
    series_id: str
    period_date: str
    value: float
    frequency: str
    unit: str
    source: str
    report: str
    release_date: str
    reference_period: str = ""
    vintage: str = ""
    seasonal_adjustment: str = ""
    source_series_label: str = ""
    source_table_name: str = ""
    source_line_number: str = ""
    source_metric_name: str = ""
    source_unit_label: str = ""
    artifact_path: str = ""


@dataclass(frozen=True)
class DerivedObservation(Observation):
    input_series: tuple[str, ...] = ()
