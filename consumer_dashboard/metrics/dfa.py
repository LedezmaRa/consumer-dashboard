"""Distributional Financial Accounts (DFA) metric calculations.

Produces two derived series that reveal the distributional dimension of
household wealth — the single most important missing lens in pure aggregate
consumer analysis:

  dfa_wealth_concentration_ratio
    Top-1% net worth share divided by bottom-50% net worth share.
    Rising ratio = increasing wealth concentration. This contextualizes
    why aggregate spending can look healthy while large portions of the
    population face increasing financial stress.

  dfa_bottom50_net_worth_yoy_pct
    Year-over-year percentage change in bottom-50% household net worth.
    The most sensitive indicator of whether the median household is gaining
    or losing financial resilience. This group has almost no financial buffer
    and is the first to reduce spending when wealth declines.
"""

from __future__ import annotations

from consumer_dashboard.metrics.common import _derived_from_base, compute_pct_change, build_series_map
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_dfa_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    derived: list[DerivedObservation] = []
    derived.extend(_compute_wealth_concentration_ratio(series_map))
    derived.extend(_compute_bottom50_yoy(series_map))
    derived.extend(_compute_liabilities_to_assets_ratio(series_map))
    return sorted(derived, key=lambda item: (item.period_date, item.series_id))


def _compute_wealth_concentration_ratio(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Top-1% net worth divided by bottom-50% net worth (raw dollar levels).

    This ratio shows how many times larger the top-1%'s wealth stock is relative
    to the entire bottom half of the wealth distribution. A rising ratio over
    time signals concentration is intensifying.
    """
    top1_obs = series_map.get("dfa_net_worth_top1pct", [])
    bot50_obs = series_map.get("dfa_net_worth_bottom50pct", [])
    if not top1_obs or not bot50_obs:
        return []

    bot50_by_period = {obs.period_date: obs for obs in bot50_obs}
    derived: list[DerivedObservation] = []
    for top1 in top1_obs:
        bot50 = bot50_by_period.get(top1.period_date)
        if bot50 is None or bot50.value == 0:
            continue
        ratio = top1.value / bot50.value
        derived.append(
            _derived_from_base(
                top1,
                series_id="dfa_wealth_concentration_ratio",
                value=round(ratio, 2),
                report="dfa_metrics",
                unit="ratio",
                source_series_label="Wealth Concentration Ratio (Top 1% / Bottom 50% Net Worth)",
                source_metric_name="wealth_concentration_ratio",
                source_unit_label="ratio; higher=more concentrated",
                input_series=("dfa_net_worth_top1pct", "dfa_net_worth_bottom50pct"),
            )
        )
    return derived


def _compute_bottom50_yoy(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Year-over-year percent change in bottom-50% net worth (quarterly: 4 periods = 1 year)."""
    return compute_pct_change(
        series_map,
        input_series_id="dfa_net_worth_bottom50pct",
        output_series_id="dfa_bottom50_net_worth_yoy_pct",
        output_label="Bottom 50% Household Net Worth Year-over-Year Growth",
        months_lag=4,  # quarterly data: 4 quarters = 1 year
        report="dfa_metrics",
        metric_name="year_over_year_pct_change",
    )


def _compute_liabilities_to_assets_ratio(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Household total liabilities / total assets ratio.

    Rising ratio = deteriorating balance sheet even if net worth rises nominally.
    """
    liab_obs = series_map.get("household_total_liabilities", [])
    assets_obs = series_map.get("household_total_assets", [])
    if not liab_obs or not assets_obs:
        return []

    assets_by_period = {obs.period_date: float(obs.value) for obs in assets_obs}
    results: list[DerivedObservation] = []
    for obs in liab_obs:
        assets_val = assets_by_period.get(obs.period_date)
        if assets_val is None or assets_val == 0:
            continue
        ratio = float(obs.value) / assets_val
        results.append(
            _derived_from_base(
                obs,
                series_id="household_liabilities_to_assets_ratio",
                value=round(ratio, 4),
                unit="ratio; level",
                report="financial_accounts_z1",
                source_series_label="Household Liabilities to Assets Ratio",
                source_metric_name="liabilities_to_assets_ratio",
                source_unit_label="ratio",
                input_series=("household_total_liabilities", "household_total_assets"),
            )
        )
    return results
