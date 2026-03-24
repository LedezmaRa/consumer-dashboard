"""Housing channel metric calculations.

Produces three derived series that expose housing as a first-class analytical
pillar rather than a subset of the spending section:

  shelter_affordability_squeeze
    Shelter CPI YoY minus real disposable income YoY (percentage points).
    When positive, shelter costs are outrunning household income — the housing
    channel is a headwind. When negative, real income is outpacing shelter
    cost growth, which relieves affordability pressure.
    Depends on Phase 4 (cpi_shelter_yoy_pct) being computed first.

  home_equity_extraction_proxy
    YoY change in household nonfinancial assets minus YoY change in total
    liabilities (from Z.1 data). Rising nonfinancial assets relative to flat
    liabilities = equity accumulation. When nonfinancial assets flatten while
    liabilities rise, households may be extracting equity to fund spending —
    a late-cycle signal.

  housing_starts_to_permits_ratio
    New residential construction starts divided by building permits at each
    common period. A ratio persistently below 0.85 means permitted supply is
    still in the pipeline (more homes coming). A ratio above 1.05 is unusual
    and can signal permit cancellations or financing stress reducing the
    conversion of permits to actual construction.
"""

from __future__ import annotations

from consumer_dashboard.metrics.common import _derived_from_base, compute_pct_change, build_series_map
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_housing_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    derived: list[DerivedObservation] = []
    derived.extend(_compute_shelter_affordability_squeeze(series_map))
    derived.extend(_compute_home_equity_extraction_proxy(series_map))
    derived.extend(_compute_starts_to_permits_ratio(series_map))
    return sorted(derived, key=lambda item: (item.period_date, item.series_id))


def _compute_shelter_affordability_squeeze(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Shelter CPI YoY minus real DPI YoY — positive = shelter outrunning income."""
    shelter_obs = series_map.get("cpi_shelter_yoy_pct", [])
    dpi_obs = series_map.get("real_disposable_personal_income_yoy_pct", [])
    if not shelter_obs or not dpi_obs:
        return []

    dpi_by_period = {obs.period_date: obs for obs in dpi_obs}
    derived: list[DerivedObservation] = []
    for shelter in shelter_obs:
        dpi = dpi_by_period.get(shelter.period_date)
        if dpi is None:
            continue
        squeeze = shelter.value - dpi.value
        derived.append(
            _derived_from_base(
                shelter,
                series_id="shelter_affordability_squeeze",
                value=round(squeeze, 2),
                report="housing_metrics",
                unit="percent",
                source_series_label="Shelter Affordability Squeeze (Shelter CPI YoY minus Real DPI YoY)",
                source_metric_name="shelter_affordability_squeeze",
                source_unit_label="percentage points; positive=shelter outrunning income",
                input_series=("cpi_shelter_yoy_pct", "real_disposable_personal_income_yoy_pct"),
            )
        )
    return derived


def _compute_home_equity_extraction_proxy(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """YoY change in nonfinancial assets minus YoY change in total liabilities (Z.1).

    Positive = net equity accumulation. Negative = extraction or rising debt
    outpacing asset growth, which can signal households are using home equity
    to fund consumption — a late-cycle pattern.
    """
    # Build YoY change series for both asset and liability components
    augmented = dict(series_map)

    nfa_yoy = compute_pct_change(
        series_map,
        input_series_id="household_nonfinancial_assets",
        output_series_id="_nfa_yoy_pct_tmp",
        output_label="Household Nonfinancial Assets YoY (temp)",
        months_lag=4,  # quarterly Z.1 data: 4 quarters = 1 year
        report="housing_metrics",
        metric_name="year_over_year_pct_change",
    )
    liab_yoy = compute_pct_change(
        series_map,
        input_series_id="household_total_liabilities",
        output_series_id="_liab_yoy_pct_tmp",
        output_label="Household Total Liabilities YoY (temp)",
        months_lag=4,
        report="housing_metrics",
        metric_name="year_over_year_pct_change",
    )
    if not nfa_yoy or not liab_yoy:
        return []

    augmented.update(build_series_map(nfa_yoy + liab_yoy))
    nfa_by_period = {obs.period_date: obs for obs in augmented.get("_nfa_yoy_pct_tmp", [])}
    liab_by_period = {obs.period_date: obs for obs in augmented.get("_liab_yoy_pct_tmp", [])}

    derived: list[DerivedObservation] = []
    for period, nfa in nfa_by_period.items():
        liab = liab_by_period.get(period)
        if liab is None:
            continue
        proxy = nfa.value - liab.value
        derived.append(
            _derived_from_base(
                nfa,
                series_id="home_equity_extraction_proxy",
                value=round(proxy, 2),
                report="housing_metrics",
                unit="percent",
                source_series_label="Home Equity Extraction Proxy (NFA YoY minus Liabilities YoY)",
                source_metric_name="home_equity_extraction_proxy",
                source_unit_label="percentage points; positive=accumulation, negative=extraction/stress",
                input_series=("household_nonfinancial_assets", "household_total_liabilities"),
            )
        )
    return derived


def _compute_starts_to_permits_ratio(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Housing starts divided by building permits.

    Below 0.85: pipeline supply still coming. Above 1.05: anomalous — possible
    permit cancellations or financing stress cutting conversion rates.
    """
    starts_obs = series_map.get("housing_starts", [])
    permits_obs = series_map.get("building_permits", [])
    if not starts_obs or not permits_obs:
        return []

    permits_by_period = {obs.period_date: obs for obs in permits_obs}
    derived: list[DerivedObservation] = []
    for starts in starts_obs:
        permits = permits_by_period.get(starts.period_date)
        if permits is None or permits.value == 0:
            continue
        ratio = starts.value / permits.value
        derived.append(
            _derived_from_base(
                starts,
                series_id="housing_starts_to_permits_ratio",
                value=round(ratio, 3),
                report="housing_metrics",
                unit="ratio",
                source_series_label="Housing Starts to Building Permits Ratio",
                source_metric_name="housing_starts_to_permits_ratio",
                source_unit_label="ratio; <0.85=pipeline supply pending, >1.05=anomalous",
                input_series=("housing_starts", "building_permits"),
            )
        )
    return derived
