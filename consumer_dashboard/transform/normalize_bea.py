"""Normalize BEA Personal Income and Outlays data."""

from __future__ import annotations

import re

from consumer_dashboard.models.observation import Observation
from consumer_dashboard.sources.bea import BEA_PCE_PRICE_TABLE, BEA_PERSONAL_INCOME_TABLE

SERIES_ALIASES = {
    "personal_income": (
        "personal income",
        "current dollar personal income",
    ),
    "disposable_personal_income": (
        "equals disposable personal income",
        "disposable personal income",
        "current dollar disposable personal income",
        "disposable personal income dpi",
    ),
    "personal_consumption_expenditures": (
        "personal consumption expenditures pce",
        "current dollar personal consumption expenditures pce",
        "personal consumption expenditures",
    ),
    "personal_outlays": (
        "less personal outlays",
        "personal outlays",
        "personal outlays current dollars",
    ),
    "personal_saving": ("equals personal saving", "personal saving"),
    "savings_rate": (
        "personal saving as a percentage of disposable personal income",
        "personal saving rate",
    ),
    "real_disposable_personal_income": (
        "real disposable personal income",
        "disposable personal income chained 2017 dollars",
    ),
    "real_personal_consumption_expenditures": (
        "real personal consumption expenditures",
        "real pce",
    ),
    "pce_price_index": ("pce price index",),
    "core_pce_price_index": (
        "pce price index excluding food and energy",
        "pce price index ex food and energy",
        "pce excluding food and energy",
    ),
}


def _normalize_label(value: str) -> str:
    label = value.lower().replace("&", "and")
    label = re.sub(r"[^a-z0-9]+", " ", label)
    return re.sub(r"\s+", " ", label).strip()


NORMALIZED_LABEL_TO_SERIES = {
    _normalize_label(alias): series_id
    for series_id, aliases in SERIES_ALIASES.items()
    for alias in aliases
}

LINE_NUMBER_TO_SERIES = {
    "27": "disposable_personal_income",
    "28": "personal_outlays",
    "29": "personal_consumption_expenditures",
    "34": "personal_saving",
    "35": "savings_rate",
    "37": "real_disposable_personal_income",
}

PCE_PRICE_LINE_NUMBER_TO_SERIES = {
    "1": "pce_price_index",
    "25": "core_pce_price_index",
}


def _parse_time_period(value: str) -> str:
    match = re.fullmatch(r"(\d{4})M(\d{1,2})", value.strip())
    if not match:
        raise ValueError(f"Unsupported BEA time period '{value}'.")
    year, month = match.groups()
    return f"{year}-{int(month):02d}-01"


def _parse_data_value(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if cleaned in {"", "(NA)", "NA", "--"}:
        return None
    return float(cleaned)


def _build_unit(row: dict) -> str:
    metric_name = str(row.get("METRIC_NAME") or row.get("Metric_Name") or "").strip()
    cl_unit = str(row.get("CL_UNIT") or "").strip()
    if metric_name and cl_unit:
        return f"{metric_name}; {cl_unit}"
    if metric_name:
        return metric_name
    if cl_unit:
        return cl_unit
    return "value"


def _get_rows(payload: dict) -> list[dict]:
    if "response" in payload:
        payload = payload["response"]
    return payload.get("BEAAPI", {}).get("Results", {}).get("Data", [])


def normalize_bea_payload(payload) -> list[Observation]:
    envelope_metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    release_date = envelope_metadata.get("fetched_at", "")
    artifact_path = envelope_metadata.get("artifact_path", "")
    source_table_name = envelope_metadata.get("table_name", "T20600")
    rows = _get_rows(payload)
    observations: list[Observation] = []
    for row in rows:
        line_description = str(row.get("LineDescription", "")).strip()
        line_number = str(row.get("LineNumber", "")).strip()
        metric_name = str(row.get("METRIC_NAME") or row.get("Metric_Name") or "").strip()
        cl_unit = str(row.get("CL_UNIT") or "").strip()
        if cl_unit.lower() == "percent change":
            continue
        series_id = None
        if source_table_name == BEA_PERSONAL_INCOME_TABLE:
            series_id = LINE_NUMBER_TO_SERIES.get(line_number)
        elif source_table_name == BEA_PCE_PRICE_TABLE:
            series_id = PCE_PRICE_LINE_NUMBER_TO_SERIES.get(line_number)
        if not series_id:
            series_id = NORMALIZED_LABEL_TO_SERIES.get(_normalize_label(line_description))
        if not series_id:
            continue
        if source_table_name == BEA_PCE_PRICE_TABLE and series_id not in {
            "pce_price_index",
            "core_pce_price_index",
        }:
            continue
        value = _parse_data_value(str(row.get("DataValue", "")))
        if value is None:
            continue
        period = _parse_time_period(str(row.get("TimePeriod", "")))
        observations.append(
            Observation(
                series_id=series_id,
                period_date=period,
                value=value,
                frequency="monthly",
                unit=_build_unit(row),
                source="bea",
                report="personal_income_outlays",
                release_date=release_date,
                reference_period=str(row.get("TimePeriod", "")),
                vintage=line_number,
                seasonal_adjustment="",
                source_series_label=line_description,
                source_table_name=source_table_name,
                source_line_number=line_number,
                source_metric_name=metric_name,
                source_unit_label=cl_unit,
                artifact_path=artifact_path,
            )
        )
    return sorted(observations, key=lambda item: (item.period_date, item.series_id))
