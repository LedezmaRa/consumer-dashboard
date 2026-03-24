"""Helpers for derived metric calculations."""

from __future__ import annotations

from calendar import monthrange
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from consumer_dashboard.models.observation import DerivedObservation, Observation
from consumer_dashboard.storage.filesystem import read_json


def load_normalized_observations(processed_dir: Path) -> list[Observation]:
    observations: list[Observation] = []
    for path in sorted(processed_dir.glob("*_observations.json")):
        if path.name == "derived_observations.json":
            continue
        payload = read_json(path, default={})
        for item in payload.get("observations", []):
            if not isinstance(item, dict):
                continue
            observations.append(Observation(**item))
    return sorted(observations, key=lambda item: (item.period_date, item.series_id))


def build_series_map(observations: list[Observation]) -> dict[str, list[Observation]]:
    series_map: dict[str, list[Observation]] = {}
    for observation in observations:
        series_map.setdefault(observation.series_id, []).append(observation)
    for series_id in series_map:
        series_map[series_id] = sorted(series_map[series_id], key=lambda item: item.period_date)
    return series_map


def shift_months(period_date: str, months: int) -> str:
    current = datetime.strptime(period_date, "%Y-%m-%d").date()
    month_index = current.month - 1 - months
    year = current.year + month_index // 12
    month = month_index % 12 + 1
    day = min(current.day, monthrange(year, month)[1])
    return f"{year:04d}-{month:02d}-{day:02d}"


def _combine_release_dates(*values: str) -> str:
    candidates = [value for value in values if value]
    if not candidates:
        return ""
    return max(candidates)


def _derived_from_base(
    base: Observation,
    *,
    series_id: str,
    value: float,
    report: str,
    unit: str,
    source_series_label: str,
    source_metric_name: str,
    source_unit_label: str,
    input_series: tuple[str, ...],
) -> DerivedObservation:
    return DerivedObservation(
        series_id=series_id,
        period_date=base.period_date,
        value=value,
        frequency=base.frequency,
        unit=unit,
        source="derived",
        report=report,
        release_date=base.release_date,
        reference_period=base.reference_period or base.period_date,
        vintage=base.vintage or base.period_date,
        seasonal_adjustment=base.seasonal_adjustment,
        source_series_label=source_series_label,
        source_table_name=report,
        source_line_number="",
        source_metric_name=source_metric_name,
        source_unit_label=source_unit_label,
        artifact_path="",
        input_series=input_series,
    )


def compute_pct_change(
    series_map: dict[str, list[Observation]],
    *,
    input_series_id: str,
    output_series_id: str,
    output_label: str,
    months_lag: int,
    report: str,
    metric_name: str,
) -> list[DerivedObservation]:
    observations = series_map.get(input_series_id, [])
    if not observations:
        return []

    by_period = {item.period_date: item for item in observations}
    derived: list[DerivedObservation] = []
    for current in observations:
        prior = by_period.get(shift_months(current.period_date, months_lag))
        if prior is None or prior.value == 0:
            continue
        value = ((current.value / prior.value) - 1.0) * 100.0
        derived.append(
            _derived_from_base(
                current,
                series_id=output_series_id,
                value=value,
                report=report,
                unit="percent",
                source_series_label=output_label,
                source_metric_name=metric_name,
                source_unit_label="percent",
                input_series=(input_series_id,),
            )
        )
    return derived


def compute_annualized_change(
    series_map: dict[str, list[Observation]],
    *,
    input_series_id: str,
    output_series_id: str,
    output_label: str,
    months_lag: int,
    report: str,
    metric_name: str,
) -> list[DerivedObservation]:
    observations = series_map.get(input_series_id, [])
    if not observations:
        return []

    by_period = {item.period_date: item for item in observations}
    derived: list[DerivedObservation] = []
    annualization_factor = 12 / months_lag
    for current in observations:
        prior = by_period.get(shift_months(current.period_date, months_lag))
        if prior is None or prior.value <= 0 or current.value <= 0:
            continue
        value = (((current.value / prior.value) ** annualization_factor) - 1.0) * 100.0
        derived.append(
            _derived_from_base(
                current,
                series_id=output_series_id,
                value=value,
                report=report,
                unit="percent",
                source_series_label=output_label,
                source_metric_name=metric_name,
                source_unit_label="percent",
                input_series=(input_series_id,),
            )
        )
    return derived


def compute_deflated_level_proxy(
    series_map: dict[str, list[Observation]],
    *,
    nominal_series_id: str,
    price_series_id: str,
    output_series_id: str,
    output_label: str,
    report: str,
) -> list[DerivedObservation]:
    nominal_observations = series_map.get(nominal_series_id, [])
    price_observations = series_map.get(price_series_id, [])
    if not nominal_observations or not price_observations:
        return []

    price_by_period = {item.period_date: item for item in price_observations}
    derived: list[DerivedObservation] = []
    for nominal in nominal_observations:
        price = price_by_period.get(nominal.period_date)
        if price is None or price.value == 0:
            continue
        value = (nominal.value / price.value) * 100.0
        derived.append(
            DerivedObservation(
                series_id=output_series_id,
                period_date=nominal.period_date,
                value=value,
                frequency=nominal.frequency,
                unit="real_proxy_index",
                source="derived",
                report=report,
                release_date=_combine_release_dates(nominal.release_date, price.release_date),
                reference_period=nominal.reference_period or nominal.period_date,
                vintage=nominal.vintage or nominal.period_date,
                seasonal_adjustment=nominal.seasonal_adjustment,
                source_series_label=output_label,
                source_table_name=report,
                source_line_number="",
                source_metric_name="deflated_level_proxy",
                source_unit_label="index",
                artifact_path="",
                input_series=(nominal_series_id, price_series_id),
            )
        )
    return derived


def serialize_derived_observations(observations: list[DerivedObservation]) -> list[dict[str, object]]:
    return [asdict(item) for item in observations]
