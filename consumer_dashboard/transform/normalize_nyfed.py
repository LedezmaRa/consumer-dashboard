"""Normalize New York Fed payloads."""

from __future__ import annotations

from consumer_dashboard.models.observation import Observation

TABLE_SERIES_METADATA = {
    "debt_balance_composition": {
        "frequency": "quarterly",
        "metric_name": "balance",
        "series": {
            "Mortgage": ("household_mortgage_balance", "Mortgage Balance"),
            "HE Revolving": ("household_heloc_balance", "HELOC Balance"),
            "Auto Loan": ("household_auto_loan_balance", "Auto Loan Balance"),
            "Credit Card": ("household_credit_card_balance", "Credit Card Balance"),
            "Student Loan": ("household_student_loan_balance", "Student Loan Balance"),
            "Other": ("household_other_balance", "Other Balance"),
            "Total": ("household_debt_total", "Total Household Debt Balance"),
        },
    },
    "balance_by_delinquency_status": {
        "frequency": "quarterly",
        "metric_name": "share",
        "series": {
            "Current": ("household_debt_current_share", "Current Balance Share"),
            "30 days late": ("household_debt_30_days_late_share", "30 Days Late Balance Share"),
            "60 days late": ("household_debt_60_days_late_share", "60 Days Late Balance Share"),
            "90 days late": ("household_debt_90_days_late_share", "90 Days Late Balance Share"),
            "120+ days late": ("household_debt_120_plus_days_late_share", "120+ Days Late Balance Share"),
            "Severely Derogatory": (
                "household_debt_severely_derogatory_share",
                "Severely Derogatory Balance Share",
            ),
        },
    },
    "ninety_plus_delinquent_balance_rate": {
        "frequency": "quarterly",
        "metric_name": "rate",
        "series": {
            "MORTGAGE": ("household_mortgage_90_plus_delinquent_rate", "Mortgage 90+ Days Delinquent Rate"),
            "HELOC": ("household_heloc_90_plus_delinquent_rate", "HELOC 90+ Days Delinquent Rate"),
            "AUTO": ("household_auto_loan_90_plus_delinquent_rate", "Auto Loan 90+ Days Delinquent Rate"),
            "CC": ("household_credit_card_90_plus_delinquent_rate", "Credit Card 90+ Days Delinquent Rate"),
            "STUDENT LOAN": (
                "household_student_loan_90_plus_delinquent_rate",
                "Student Loan 90+ Days Delinquent Rate",
            ),
            "OTHER": ("household_other_90_plus_delinquent_rate", "Other 90+ Days Delinquent Rate"),
            "ALL": ("household_debt_90_plus_delinquent_rate", "All Household Debt 90+ Days Delinquent Rate"),
        },
    },
    "new_delinquent_balances": {
        "frequency": "quarterly",
        "metric_name": "rate",
        "series": {
            "AUTO": ("new_delinquent_auto_loan_rate", "New Delinquent Auto Loan Rate"),
            "CC": ("new_delinquent_credit_card_rate", "New Delinquent Credit Card Rate"),
            "MORTGAGE": ("new_delinquent_mortgage_rate", "New Delinquent Mortgage Rate"),
            "HELOC": ("new_delinquent_heloc_rate", "New Delinquent HELOC Rate"),
            "STUDENT LOAN": ("new_delinquent_student_loan_rate", "New Delinquent Student Loan Rate"),
            "OTHER": ("new_delinquent_other_rate", "New Delinquent Other Rate"),
            "Total": ("new_delinquent_total_rate", "New Delinquent Total Rate"),
        },
    },
    "new_serious_delinquent_balances": {
        "frequency": "quarterly",
        "metric_name": "rate",
        "series": {
            "AUTO": (
                "new_serious_delinquent_auto_loan_rate",
                "New Seriously Delinquent Auto Loan Rate",
            ),
            "CC": (
                "new_serious_delinquent_credit_card_rate",
                "New Seriously Delinquent Credit Card Rate",
            ),
            "MORTGAGE": (
                "new_serious_delinquent_mortgage_rate",
                "New Seriously Delinquent Mortgage Rate",
            ),
            "HELOC": (
                "new_serious_delinquent_heloc_rate",
                "New Seriously Delinquent HELOC Rate",
            ),
            "STUDENT LOAN": (
                "new_serious_delinquent_student_loan_rate",
                "New Seriously Delinquent Student Loan Rate",
            ),
            "OTHER": (
                "new_serious_delinquent_other_rate",
                "New Seriously Delinquent Other Rate",
            ),
            "ALL": (
                "new_serious_delinquent_total_rate",
                "New Seriously Delinquent Total Rate",
            ),
        },
    },
}


def _parse_quarter_period(label: str) -> str:
    year_text, quarter_text = label.split(":Q", maxsplit=1)
    year = int(year_text)
    if len(year_text) == 2:
        year += 2000 if year < 80 else 1900
    quarter = int(quarter_text)
    if quarter == 1:
        return f"{year:04d}-03-31"
    if quarter == 2:
        return f"{year:04d}-06-30"
    if quarter == 3:
        return f"{year:04d}-09-30"
    if quarter == 4:
        return f"{year:04d}-12-31"
    raise ValueError(f"Unsupported quarter label: {label}")


def _normalize_unit(unit_label: str) -> str:
    normalized = unit_label.strip().lower()
    if "trillion" in normalized:
        return "trillions_of_dollars"
    if "billion" in normalized:
        return "billions_of_dollars"
    if "percent" in normalized:
        return "percent"
    return "index"


def _parse_number(value) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    cleaned = str(value).strip().replace(",", "").replace("%", "")
    if not cleaned or cleaned.lower() in {"n.a.", "na", "n.a"}:
        return None
    return float(cleaned)


def _normalize_table(table_key: str, payload, metadata: dict[str, object]) -> list[Observation]:
    table = payload.get("tables", {}).get(table_key, {}) if isinstance(payload, dict) else {}
    if not isinstance(table, dict):
        return []

    table_metadata = TABLE_SERIES_METADATA[table_key]
    release_date = str(metadata.get("release_date", metadata.get("fetched_at", "")))
    artifact_path = str(metadata.get("artifact_path", ""))
    report_period = str(metadata.get("report_period", ""))
    table_name = str(table.get("table_name", table_key))
    unit_label = str(table.get("unit_label", ""))
    unit = _normalize_unit(unit_label)
    observations: list[Observation] = []

    for row in table.get("rows", []):
        if not isinstance(row, dict):
            continue
        reference_period = str(row.get("reference_period", "")).strip()
        if not reference_period:
            continue
        period_date = _parse_quarter_period(reference_period)
        for source_label, (series_id, source_series_label) in table_metadata["series"].items():
            value = _parse_number(row.get(source_label))
            if value is None:
                continue
            observations.append(
                Observation(
                    series_id=series_id,
                    period_date=period_date,
                    value=value,
                    frequency=str(table_metadata["frequency"]),
                    unit=unit,
                    source="new_york_fed",
                    report="household_debt_credit",
                    release_date=release_date,
                    reference_period=reference_period,
                    vintage=report_period or reference_period,
                    seasonal_adjustment="not_seasonally_adjusted",
                    source_series_label=source_series_label,
                    source_table_name=table_name,
                    source_line_number=source_label,
                    source_metric_name=str(table_metadata["metric_name"]),
                    source_unit_label=unit_label,
                    artifact_path=artifact_path,
                )
            )
    return observations


def normalize_nyfed_payload(payload) -> list[Observation]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    report_slug = metadata.get("report_slug", "")
    if report_slug != "household_debt_credit":
        return []

    observations: list[Observation] = []
    for table_key in TABLE_SERIES_METADATA:
        observations.extend(_normalize_table(table_key, payload, metadata))
    return sorted(observations, key=lambda item: (item.period_date, item.series_id))
