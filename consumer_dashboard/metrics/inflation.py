"""Inflation metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import (
    _derived_from_base,
    build_series_map,
    compute_annualized_change,
    compute_pct_change,
)
from consumer_dashboard.models.observation import DerivedObservation, Observation


SERIES_SPECS = {
    "cpi_headline": "CPI Headline",
    "cpi_core": "CPI Core",
    "pce_price_index": "PCE Price Index",
    "core_pce_price_index": "Core PCE Price Index",
    # Inflation detail series (Phase 4)
    "cpi_shelter": "CPI Shelter",
    "cpi_owners_equivalent_rent": "CPI Owners' Equivalent Rent",
    "cpi_services_ex_energy": "CPI Services Ex-Energy",
}


def compute_inflation_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    derived: list[DerivedObservation] = []
    for source_series_id, label in SERIES_SPECS.items():
        if source_series_id not in series_map:
            continue
        derived.extend(
            compute_pct_change(
                series_map,
                input_series_id=source_series_id,
                output_series_id=f"{source_series_id}_mom_pct",
                output_label=f"{label} Month-over-Month Inflation",
                months_lag=1,
                report="inflation_metrics",
                metric_name="month_over_month_pct_change",
            )
        )
        derived.extend(
            compute_pct_change(
                series_map,
                input_series_id=source_series_id,
                output_series_id=f"{source_series_id}_yoy_pct",
                output_label=f"{label} Year-over-Year Inflation",
                months_lag=12,
                report="inflation_metrics",
                metric_name="year_over_year_pct_change",
            )
        )
        derived.extend(
            compute_annualized_change(
                series_map,
                input_series_id=source_series_id,
                output_series_id=f"{source_series_id}_3m_annualized_pct",
                output_label=f"{label} 3-Month Annualized Inflation",
                months_lag=3,
                report="inflation_metrics",
                metric_name="three_month_annualized_pct_change",
            )
        )

    # Compute shelter-vs-services spread from derived YoY series
    augmented = dict(series_map)
    augmented.update(build_series_map(derived))
    derived.extend(_compute_shelter_vs_services_gap(augmented))

    return sorted(derived, key=lambda item: (item.period_date, item.series_id))


def _compute_shelter_vs_services_gap(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Shelter YoY minus services-ex-energy YoY.

    When positive and falling it signals that shelter's lagged mean-reversion
    is underway and the inflation problem is becoming more services-driven.
    When negative, services inflation is outpacing shelter — a different risk.
    """
    shelter_obs = series_map.get("cpi_shelter_yoy_pct", [])
    services_obs = series_map.get("cpi_services_ex_energy_yoy_pct", [])
    if not shelter_obs or not services_obs:
        return []

    services_by_period = {obs.period_date: obs for obs in services_obs}
    derived: list[DerivedObservation] = []
    for shelter in shelter_obs:
        services = services_by_period.get(shelter.period_date)
        if services is None:
            continue
        spread = shelter.value - services.value
        derived.append(
            _derived_from_base(
                shelter,
                series_id="cpi_shelter_vs_services_spread",
                value=spread,
                report="inflation_metrics",
                unit="percent",
                source_series_label="CPI Shelter vs Services Ex-Energy Spread (YoY)",
                source_metric_name="shelter_vs_services_spread",
                source_unit_label="percentage points",
                input_series=("cpi_shelter_yoy_pct", "cpi_services_ex_energy_yoy_pct"),
            )
        )
    return derived
