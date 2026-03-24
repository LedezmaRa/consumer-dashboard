"""Normalize Federal Reserve Board payloads."""

from __future__ import annotations

import csv
from datetime import datetime
import io
import re

from consumer_dashboard.models.observation import Observation

G19_ROW_PATTERN = re.compile(
    r"<tr[^>]*>\s*<th[^>]*>(?P<label>[A-Z][a-z]{2} \d{4})</th>\s*"
    r"<td>(?P<total>[^<]+)</td>\s*"
    r"<td>(?P<revolving>[^<]+)</td>\s*"
    r"<td>(?P<nonrevolving>[^<]+)</td>",
    re.IGNORECASE | re.DOTALL,
)

G19_SERIES_METADATA = {
    "total": {
        "series_id": "consumer_credit_total",
        "label": "Consumer Credit Outstanding Total",
        "line": "total",
    },
    "revolving": {
        "series_id": "consumer_credit_revolving",
        "label": "Consumer Credit Outstanding Revolving",
        "line": "revolving",
    },
    "nonrevolving": {
        "series_id": "consumer_credit_nonrevolving",
        "label": "Consumer Credit Outstanding Nonrevolving",
        "line": "nonrevolving",
    },
}

Z1_SERIES_METADATA = {
    "FL152000005.Q": {
        "series_id": "household_total_assets",
        "label": "Households and Nonprofit Organizations Total Assets",
        "line": "Line 1",
    },
    "FL152010005.Q": {
        "series_id": "household_nonfinancial_assets",
        "label": "Households and Nonprofit Organizations Nonfinancial Assets",
        "line": "Line 2",
    },
    "FL154090005.Q": {
        "series_id": "household_total_financial_assets",
        "label": "Households and Nonprofit Organizations Total Financial Assets",
        "line": "Line 3",
    },
    "FL154190005.Q": {
        "series_id": "household_total_liabilities",
        "label": "Households and Nonprofit Organizations Total Liabilities",
        "line": "Line 23",
    },
    "FL152090005.Q": {
        "series_id": "household_net_worth",
        "label": "Households and Nonprofit Organizations Net Worth",
        "line": "Line 24",
    },
}


def _parse_g19_period(label: str) -> str:
    return datetime.strptime(label, "%b %Y").strftime("%Y-%m-01")


def _parse_quarter_period(label: str) -> str:
    year_text, quarter_text = label.split(":Q", maxsplit=1)
    quarter = int(quarter_text)
    if quarter == 1:
        return f"{year_text}-03-31"
    if quarter == 2:
        return f"{year_text}-06-30"
    if quarter == 3:
        return f"{year_text}-09-30"
    if quarter == 4:
        return f"{year_text}-12-31"
    raise ValueError(f"Unsupported Z.1 quarter label: {label}")


def _parse_number(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if not cleaned or cleaned.lower() in {"n.a.", "na", "n.a"}:
        return None
    return float(cleaned)


def _normalize_g19_payload(payload) -> list[Observation]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    artifact_path = metadata.get("artifact_path", "")
    release_date = metadata.get("release_date", metadata.get("fetched_at", ""))
    response_text = payload.get("response_text", "") if isinstance(payload, dict) else ""
    observations: list[Observation] = []
    for match in G19_ROW_PATTERN.finditer(response_text):
        reference_period = match.group("label")
        period_date = _parse_g19_period(reference_period)
        for source_key, series_metadata in G19_SERIES_METADATA.items():
            value = _parse_number(match.group(source_key))
            if value is None:
                continue
            observations.append(
                Observation(
                    series_id=series_metadata["series_id"],
                    period_date=period_date,
                    value=value,
                    frequency="monthly",
                    unit="millions_of_dollars",
                    source="federal_reserve_board",
                    report="consumer_credit_g19",
                    release_date=release_date,
                    reference_period=reference_period,
                    vintage=period_date,
                    seasonal_adjustment="seasonally_adjusted",
                    source_series_label=series_metadata["label"],
                    source_table_name="Consumer Credit Outstanding (Levels)",
                    source_line_number=series_metadata["line"],
                    source_metric_name="level",
                    source_unit_label="millions_of_dollars",
                    artifact_path=artifact_path,
                )
            )
    return observations


def _normalize_z1_payload(payload) -> list[Observation]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    artifact_path = metadata.get("artifact_path", "")
    release_date = metadata.get("release_date", metadata.get("fetched_at", ""))
    csv_text = payload.get("csv_text", "") if isinstance(payload, dict) else ""
    observations: list[Observation] = []
    for row in csv.DictReader(io.StringIO(csv_text)):
        reference_period = str(row.get("date", "")).strip()
        if not reference_period:
            continue
        period_date = _parse_quarter_period(reference_period)
        for column_name, series_metadata in Z1_SERIES_METADATA.items():
            value = _parse_number(str(row.get(column_name, "")))
            if value is None:
                continue
            observations.append(
                Observation(
                    series_id=series_metadata["series_id"],
                    period_date=period_date,
                    value=value,
                    frequency="quarterly",
                    unit="millions_of_dollars",
                    source="federal_reserve_board",
                    report="financial_accounts_z1",
                    release_date=release_date,
                    reference_period=reference_period,
                    vintage=reference_period,
                    seasonal_adjustment="not_seasonally_adjusted",
                    source_series_label=series_metadata["label"],
                    source_table_name=metadata.get("source_table_name", "B.101.e"),
                    source_line_number=series_metadata["line"],
                    source_metric_name="level",
                    source_unit_label="millions_of_dollars",
                    artifact_path=artifact_path,
                )
            )
    return observations


def normalize_fed_payload(payload) -> list[Observation]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    report_slug = metadata.get("report_slug", "")
    if report_slug == "consumer_credit_g19":
        observations = _normalize_g19_payload(payload)
    elif report_slug == "financial_accounts_z1":
        observations = _normalize_z1_payload(payload)
    else:
        observations = []
    return sorted(observations, key=lambda item: (item.period_date, item.series_id))
