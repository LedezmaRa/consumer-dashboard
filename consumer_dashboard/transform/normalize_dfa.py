"""Normalize Federal Reserve Distributional Financial Accounts (DFA) artifacts.

The DFA data is released quarterly alongside the Z.1 Financial Accounts.
It publishes household wealth broken down by wealth percentile group, income
quintile, generation, race/ethnicity, and education level.

This normalizer extracts the wealth-by-percentile breakdowns from the
dfa-level-detail.csv or dfa-shares.csv files. The series codes used:

  Level detail CSV series names for net worth by wealth group:
    Net worth -- Top 1%
    Net worth -- Next 9% (90th-99th percentile)
    Net worth -- Next 40% (50th-90th percentile)
    Net worth -- Bottom 50%

These map to series IDs:
    dfa_net_worth_top1pct
    dfa_net_worth_next9pct
    dfa_net_worth_next40pct
    dfa_net_worth_bottom50pct

Values are in millions of dollars (unadjusted seasonally, quarterly frequency).
"""

from __future__ import annotations

import csv
import io

from consumer_dashboard.models.observation import Observation


# Mapping from DFA CSV column header substrings to canonical series IDs
DFA_WEALTH_SERIES = {
    "Top 1%": "dfa_net_worth_top1pct",
    "Next 9%": "dfa_net_worth_next9pct",
    "Next 40%": "dfa_net_worth_next40pct",
    "Bottom 50%": "dfa_net_worth_bottom50pct",
}

DFA_WEALTH_LABELS = {
    "dfa_net_worth_top1pct": "Household Net Worth — Top 1% (DFA)",
    "dfa_net_worth_next9pct": "Household Net Worth — Next 9% (DFA)",
    "dfa_net_worth_next40pct": "Household Net Worth — Next 40% (DFA)",
    "dfa_net_worth_bottom50pct": "Household Net Worth — Bottom 50% (DFA)",
}


def _parse_dfa_date(date_str: str) -> str | None:
    """Parse a DFA quarterly date string (e.g. '2024:Q3' or '2024Q3') to YYYY-MM-01."""
    import re
    date_str = date_str.strip()
    match = re.match(r"(\d{4})[:\s]?Q(\d)", date_str, re.IGNORECASE)
    if not match:
        return None
    year = int(match.group(1))
    quarter = int(match.group(2))
    month = (quarter - 1) * 3 + 1
    return f"{year:04d}-{month:02d}-01"


def normalize_dfa_payload(payload: dict) -> list[Observation]:
    """Normalize a DFA artifact into canonical observations.

    Attempts to parse the dfa-level-detail.csv for net worth by wealth group.
    Returns an empty list gracefully if the CSV structure has changed or is unavailable.
    """
    metadata = payload.get("metadata", {})
    release_date = str(metadata.get("release_date", ""))
    artifact_path = str(metadata.get("artifact_path", ""))
    csv_texts: dict[str, str] = payload.get("csv_texts", {})

    observations: list[Observation] = []

    # Try level detail first, then shares as fallback
    csv_text = csv_texts.get("csv/dfa-level-detail.csv", "")
    if not csv_text:
        return []

    try:
        reader = csv.DictReader(io.StringIO(csv_text))
        rows = list(reader)
    except Exception:
        return []

    if not rows:
        return []

    # The DFA CSV has a 'Date' column and then named columns per wealth group.
    # Column names contain the wealth group label somewhere in the header.
    header = list(rows[0].keys()) if rows else []

    # Build mapping from column name -> series_id for net worth columns
    col_to_series: dict[str, str] = {}
    for col in header:
        for keyword, series_id in DFA_WEALTH_SERIES.items():
            if keyword in col and "Net worth" in col:
                col_to_series[col] = series_id
                break

    if not col_to_series:
        # Column structure doesn't match expectations — return empty gracefully
        return []

    # Find the date column
    date_col = next((c for c in header if c.lower() in {"date", "period", "quarter"}), None)
    if date_col is None and header:
        date_col = header[0]  # Assume first column is date

    for row in rows:
        if date_col is None:
            continue
        date_str = str(row.get(date_col, "")).strip()
        period_date = _parse_dfa_date(date_str)
        if period_date is None:
            continue

        for col, series_id in col_to_series.items():
            val_str = str(row.get(col, "")).strip().replace(",", "")
            if not val_str or val_str in {".", "N/A", ""}:
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
                    frequency="quarterly",
                    unit="millions_of_dollars",
                    source="federal_reserve_board",
                    report="distributional_financial_accounts",
                    release_date=release_date,
                    reference_period=period_date,
                    vintage=period_date,
                    seasonal_adjustment="not_seasonally_adjusted",
                    source_series_label=DFA_WEALTH_LABELS.get(series_id, series_id),
                    source_table_name="Distributional Financial Accounts (DFA)",
                    source_line_number=col,
                    source_metric_name="level",
                    source_unit_label="millions of dollars",
                    artifact_path=artifact_path,
                )
            )

    return sorted(observations, key=lambda obs: (obs.period_date, obs.series_id))
