"""Normalize University of Michigan SCA artifacts into canonical observations."""

from __future__ import annotations

from consumer_dashboard.models.observation import Observation

MICHIGAN_SERIES_METADATA = {
    "michigan_sentiment_index": {
        "unit": "index_1966_q1_100",
        "report": "michigan_sentiment",
        "label": "University of Michigan Index of Consumer Sentiment",
        "seasonal_adjustment": "not_seasonally_adjusted",
    },
    "michigan_inflation_expectations_1y": {
        "unit": "percent",
        "report": "michigan_sentiment",
        "label": "University of Michigan 1-Year Inflation Expectations (Median)",
        "seasonal_adjustment": "not_seasonally_adjusted",
    },
    "michigan_inflation_expectations_5y": {
        "unit": "percent",
        "report": "michigan_sentiment",
        "label": "University of Michigan 5-Year Inflation Expectations (Median)",
        "seasonal_adjustment": "not_seasonally_adjusted",
    },
}


def _parse_michigan_date(date_str: str) -> str | None:
    """Parse a date string to YYYY-MM-01. Handles FRED (YYYY-MM-DD), YYYY-MM, Mon-YYYY formats."""
    date_str = date_str.strip()
    # FRED format: YYYY-MM-DD
    if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
        try:
            year = int(date_str[:4])
            month = int(date_str[5:7])
            if 1 <= month <= 12:
                return f"{year:04d}-{month:02d}-01"
        except ValueError:
            pass
    # Try YYYY-MM
    if len(date_str) == 7 and date_str[4] == "-":
        try:
            year = int(date_str[:4])
            month = int(date_str[5:7])
            if 1 <= month <= 12:
                return f"{year:04d}-{month:02d}-01"
        except ValueError:
            pass
    # Try Mon-YYYY and other formats
    import datetime
    for fmt in ("%b-%Y", "%B-%Y", "%m/%Y", "%Y/%m"):
        try:
            parsed = datetime.datetime.strptime(date_str, fmt)
            return f"{parsed.year:04d}-{parsed.month:02d}-01"
        except ValueError:
            continue
    return None


def normalize_michigan_payload(payload: dict) -> list[Observation]:
    """Normalize a single Michigan SCA artifact (one series per file)."""
    metadata = payload.get("metadata", {})
    series_id = str(payload.get("series_id", "")).strip()
    release_date = str(metadata.get("fetched_at", ""))
    artifact_path = str(metadata.get("artifact_path", ""))

    series_meta = MICHIGAN_SERIES_METADATA.get(series_id)
    if series_meta is None:
        return []

    observations: list[Observation] = []
    for row in payload.get("data", []):
        if not isinstance(row, dict):
            continue
        date_str = str(row.get("date", "")).strip()
        val_str = str(row.get("value", "")).strip()
        period_date = _parse_michigan_date(date_str)
        if period_date is None:
            continue
        try:
            value = float(val_str)
        except ValueError:
            continue
        observations.append(
            Observation(
                series_id=series_id,
                period_date=period_date,
                value=value,
                frequency="monthly",
                unit=series_meta["unit"],
                source="michigan",
                report=series_meta["report"],
                release_date=release_date,
                reference_period=period_date,
                vintage=period_date,
                seasonal_adjustment=series_meta["seasonal_adjustment"],
                source_series_label=series_meta["label"],
                source_table_name="University of Michigan Survey of Consumers",
                source_line_number=series_id,
                source_metric_name="level",
                source_unit_label=series_meta["unit"],
                artifact_path=artifact_path,
            )
        )
    return sorted(observations, key=lambda obs: obs.period_date)
