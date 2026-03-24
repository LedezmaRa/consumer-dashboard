"""Cohort stress metrics — bottom-40% household fragility composite."""
from __future__ import annotations
import statistics
from consumer_dashboard.metrics.common import _derived_from_base, build_series_map
from consumer_dashboard.models.observation import DerivedObservation


def compute_cohort_stress_metrics(series_map: dict) -> list[DerivedObservation]:
    results: list[DerivedObservation] = []
    results.extend(_compute_wealth_divergence_ratio(series_map))
    results.extend(_compute_cohort_stress_index(series_map))
    return results


def _compute_wealth_divergence_ratio(series_map: dict) -> list[DerivedObservation]:
    """Top-1% net worth level / Bottom-50% net worth level — rising ratio means diverging fortunes."""
    top_obs_list = series_map.get("dfa_net_worth_top1pct", [])
    bot_obs_list = series_map.get("dfa_net_worth_bottom50pct", [])
    if not top_obs_list or not bot_obs_list:
        return []

    top_period_map = {str(o.period_date): float(o.value) for o in top_obs_list}
    bot_period_map = {str(o.period_date): float(o.value) for o in bot_obs_list}

    results: list[DerivedObservation] = []
    for obs in bot_obs_list:
        period = str(obs.period_date)
        top_val = top_period_map.get(period)
        bot_val = bot_period_map.get(period)
        if top_val is None or bot_val is None or bot_val == 0:
            continue
        ratio = top_val / bot_val
        results.append(_derived_from_base(
            obs,
            series_id="dfa_top1_to_bottom50_ratio",
            value=round(ratio, 2),
            unit="ratio; level",
            report="dfa_metrics",
            source_series_label="Top 1% to Bottom 50% Wealth Ratio",
            source_metric_name="ratio",
            source_unit_label="ratio",
            input_series=("dfa_net_worth_top1pct", "dfa_net_worth_bottom50pct"),
        ))
    return results


def _compute_cohort_stress_index(series_map: dict) -> list[DerivedObservation]:
    """Z-score composite of bottom-50% wealth direction + card delinquency."""
    bot50_obs = series_map.get("dfa_bottom50_net_worth_yoy_pct", [])
    card_del_obs = series_map.get("household_credit_card_90_plus_delinquent_rate", [])
    if not bot50_obs or not card_del_obs:
        return []

    bot50_map = {str(o.period_date): float(o.value) for o in bot50_obs}
    card_map = {str(o.period_date): float(o.value) for o in card_del_obs}

    def z_scores(vals: list[float]) -> list[float]:
        if len(vals) < 3:
            return [0.0] * len(vals)
        mean = statistics.mean(vals)
        stdev = statistics.stdev(vals) or 1.0
        return [(v - mean) / stdev for v in vals]

    bot50_vals = list(bot50_map.values())
    card_vals = list(card_map.values())
    bot50_z = dict(zip(bot50_map.keys(), z_scores(bot50_vals)))
    card_z = dict(zip(card_map.keys(), z_scores(card_vals)))

    results: list[DerivedObservation] = []
    for obs in bot50_obs:
        period = str(obs.period_date)
        b_z = bot50_z.get(period, 0.0)
        c_z = card_z.get(period, 0.0)
        # Bottom-50% wealth falling (negative) is bad -> flip sign for stress index
        # Card delinquency rising (positive) is bad -> keep sign
        index_val = (-b_z + c_z) / 2.0
        results.append(_derived_from_base(
            obs,
            series_id="cohort_stress_index",
            value=round(index_val, 4),
            unit="score",
            report="dfa_metrics",
            source_series_label="Cohort Stress Index (Bottom 50% + Card Delinquency Composite)",
            source_metric_name="composite_index",
            source_unit_label="z-score composite",
            input_series=("dfa_bottom50_net_worth_yoy_pct", "household_credit_card_90_plus_delinquent_rate"),
        ))
    return results
