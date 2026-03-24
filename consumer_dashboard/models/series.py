"""Series catalog models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SeriesDefinition:
    series_id: str
    display_name: str
    frequency: str
    unit: str
    source_id: str
    report_slug: str
    seasonal_adjustment: str = ""
    category: str = ""
    subcategory: str = ""

