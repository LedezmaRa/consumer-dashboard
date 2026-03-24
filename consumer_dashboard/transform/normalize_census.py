"""Normalize Census economic-indicators data."""

from __future__ import annotations

from consumer_dashboard.models.observation import Observation

CENSUS_SERIES_RULES = {
    "mrts": [
        {
            "series_id": "retail_sales",
            "category_code": "44X72",
            "data_type_code": "SM",
            "seasonally_adj": "yes",
            "unit": "millions_of_dollars",
            "report": "retail_sales",
            "label": "Retail and Food Services Sales",
        },
    ],
    "resconst": [
        {
            "series_id": "building_permits",
            "category_code": "APERMITS",
            "data_type_code": "TOTAL",
            "seasonally_adj": "yes",
            "unit": "annual_rate_thousands_units",
            "report": "housing_starts_permits",
            "label": "Building Permits, Total",
        },
        {
            "series_id": "housing_starts",
            "category_code": "ASTARTS",
            "data_type_code": "TOTAL",
            "seasonally_adj": "yes",
            "unit": "annual_rate_thousands_units",
            "report": "housing_starts_permits",
            "label": "Housing Starts, Total",
        },
    ],
    "ressales": [
        {
            "series_id": "new_home_sales",
            "category_code": "ASOLD",
            "data_type_code": "TOTAL",
            "seasonally_adj": "yes",
            "unit": "annual_rate_thousands_units",
            "report": "new_home_sales",
            "label": "New Home Sales, Total",
        },
    ],
}


def _parse_census_time(value: str) -> str:
    year, month = value.split("-")
    return f"{year}-{int(month):02d}-01"


def _parse_census_value(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if cleaned in {"", "-", "(S)"}:
        return None
    return float(cleaned)


def _rows_from_payload(payload: dict) -> tuple[list[str], list[list[str]]]:
    rows = payload.get("response", []) if isinstance(payload, dict) else []
    if not rows:
        return [], []
    header, *data_rows = rows
    return header, data_rows


def normalize_census_payload(payload) -> list[Observation]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    dataset_key = metadata.get("dataset_key", "")
    release_date = metadata.get("fetched_at", "")
    artifact_path = metadata.get("artifact_path", "")
    rules = CENSUS_SERIES_RULES.get(dataset_key, [])
    if not rules:
        return []

    header, rows = _rows_from_payload(payload)
    if not header:
        return []
    records = [dict(zip(header, row)) for row in rows]

    observations: list[Observation] = []
    for rule in rules:
        for record in records:
            if record.get("category_code") != rule["category_code"]:
                continue
            if record.get("data_type_code") != rule["data_type_code"]:
                continue
            if record.get("seasonally_adj") != rule["seasonally_adj"]:
                continue
            if record.get("error_data") != "no":
                continue
            geo_level_code = record.get("geo_level_code", "")
            if geo_level_code and geo_level_code != "US":
                continue
            value = _parse_census_value(record.get("cell_value", ""))
            if value is None:
                continue
            reference_period = record.get("time", "")
            observations.append(
                Observation(
                    series_id=rule["series_id"],
                    period_date=_parse_census_time(reference_period),
                    value=value,
                    frequency="monthly",
                    unit=rule["unit"],
                    source="census",
                    report=rule["report"],
                    release_date=release_date,
                    reference_period=reference_period,
                    vintage=reference_period,
                    seasonal_adjustment="seasonally_adjusted",
                    source_series_label=rule["label"],
                    source_table_name=metadata.get("dataset_label", dataset_key),
                    source_line_number=f"{rule['category_code']}:{rule['data_type_code']}",
                    source_metric_name="level",
                    source_unit_label=rule["unit"],
                    artifact_path=artifact_path,
                )
            )
    return sorted(observations, key=lambda item: (item.period_date, item.series_id))
