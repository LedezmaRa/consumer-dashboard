"""Normalize core BLS API series into canonical observations."""

from __future__ import annotations

from consumer_dashboard.models.observation import Observation

BLS_SERIES_METADATA = {
    "LNS14000000": {
        "series_id": "unemployment_rate",
        "unit": "percent",
        "report": "jobs_report",
        "label": "Unemployment Rate",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CES0000000001": {
        "series_id": "nonfarm_payrolls",
        "unit": "thousands_of_persons",
        "report": "jobs_report",
        "label": "All Employees, Total Nonfarm",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CES0500000003": {
        "series_id": "average_hourly_earnings",
        "unit": "dollars_per_hour",
        "report": "jobs_report",
        "label": "Average Hourly Earnings of All Employees, Total Private",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CUSR0000SA0": {
        "series_id": "cpi_headline",
        "unit": "index_1982_84_100",
        "report": "cpi",
        "label": "CPI All Items, U.S. City Average",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CUSR0000SA0L1E": {
        "series_id": "cpi_core",
        "unit": "index_1982_84_100",
        "report": "cpi",
        "label": "CPI All Items Less Food and Energy, U.S. City Average",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CUSR0000SAH1": {
        "series_id": "cpi_shelter",
        "unit": "index_1982_84_100",
        "report": "cpi",
        "label": "CPI Shelter, U.S. City Average",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CUSR0000SAH21": {
        "series_id": "cpi_owners_equivalent_rent",
        "unit": "index_1982_84_100",
        "report": "cpi",
        "label": "CPI Owners' Equivalent Rent of Residences",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "CUSR0000SASLE": {
        "series_id": "cpi_services_ex_energy",
        "unit": "index_1982_84_100",
        "report": "cpi",
        "label": "CPI Services Less Energy Services, U.S. City Average",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "JTS000000000000000JOL": {
        "series_id": "jolts_job_openings",
        "unit": "thousands_of_jobs",
        "report": "jolts",
        "label": "Job Openings Level, Total Nonfarm",
        "seasonal_adjustment": "seasonally_adjusted",
    },
    "JTS000000000000000QUR": {
        "series_id": "jolts_quits_rate",
        "unit": "percent",
        "report": "jolts",
        "label": "Quits Rate, Total Nonfarm",
        "seasonal_adjustment": "seasonally_adjusted",
    },
}


def _parse_bls_period(year: str, period: str) -> str | None:
    if not period.startswith("M") or period == "M13":
        return None
    month = int(period[1:])
    return f"{year}-{month:02d}-01"


def _parse_bls_value(value: str) -> float | None:
    cleaned = value.strip().replace(",", "")
    if cleaned in {"", "-"}:
        return None
    return float(cleaned)


def _get_series(payload: dict) -> list[dict]:
    if "response" in payload:
        payload = payload["response"]
    return payload.get("Results", {}).get("series", [])


def normalize_bls_payload(payload) -> list[Observation]:
    envelope_metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    release_date = envelope_metadata.get("fetched_at", "")
    artifact_path = envelope_metadata.get("artifact_path", "")
    observations: list[Observation] = []
    for series in _get_series(payload):
        series_code = str(series.get("seriesID", "")).strip()
        metadata = BLS_SERIES_METADATA.get(series_code)
        if not metadata:
            continue
        for row in series.get("data", []):
            period_date = _parse_bls_period(str(row.get("year", "")), str(row.get("period", "")))
            if not period_date:
                continue
            value = _parse_bls_value(str(row.get("value", "")))
            if value is None:
                continue
            reference_period = f"{row.get('year', '')}{row.get('period', '')}"
            observations.append(
                Observation(
                    series_id=metadata["series_id"],
                    period_date=period_date,
                    value=value,
                    frequency="monthly",
                    unit=metadata["unit"],
                    source="bls",
                    report=metadata["report"],
                    release_date=release_date,
                    reference_period=reference_period,
                    vintage=reference_period,
                    seasonal_adjustment=metadata["seasonal_adjustment"],
                    source_series_label=metadata["label"],
                    source_table_name="BLS Public Data API",
                    source_line_number=series_code,
                    source_metric_name="level",
                    source_unit_label=metadata["unit"],
                    artifact_path=artifact_path,
                )
            )
    return sorted(observations, key=lambda item: (item.period_date, item.series_id))
