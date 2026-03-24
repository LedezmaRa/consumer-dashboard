"""Real spending metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import _derived_from_base, build_series_map, compute_deflated_level_proxy, compute_pct_change
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_real_spending_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    derived = compute_deflated_level_proxy(
        series_map,
        nominal_series_id="personal_consumption_expenditures",
        price_series_id="pce_price_index",
        output_series_id="real_personal_spending",
        output_label="Real Personal Spending Proxy",
        report="real_spending_metrics",
    )
    derived.extend(
        compute_deflated_level_proxy(
            series_map,
            nominal_series_id="retail_sales",
            price_series_id="cpi_headline",
            output_series_id="real_retail_sales_proxy",
            output_label="Real Retail Sales Proxy",
            report="real_spending_metrics",
        )
    )

    augmented_series_map = dict(series_map)
    augmented_series_map.update(build_series_map(derived))
    derived.extend(
        compute_pct_change(
            augmented_series_map,
            input_series_id="real_personal_spending",
            output_series_id="real_personal_spending_yoy_pct",
            output_label="Real Personal Spending Year-over-Year Growth",
            months_lag=12,
            report="real_spending_metrics",
            metric_name="year_over_year_pct_change",
        )
    )
    derived.extend(
        compute_pct_change(
            augmented_series_map,
            input_series_id="real_retail_sales_proxy",
            output_series_id="real_retail_sales_proxy_yoy_pct",
            output_label="Real Retail Sales Proxy Year-over-Year Growth",
            months_lag=12,
            report="real_spending_metrics",
            metric_name="year_over_year_pct_change",
        )
    )

    # Second-pass: spending_income_gap depends on real_personal_spending_yoy_pct
    # and real_disposable_personal_income_yoy_pct (may already be in series_map
    # if called from second pass in derive.py, or computed here if augmented)
    derived.extend(_compute_spending_income_gap(augmented_series_map))

    return sorted(derived, key=lambda item: (item.period_date, item.series_id))


def _compute_spending_income_gap(series_map: dict) -> list[DerivedObservation]:
    """Spending YoY minus income YoY — positive gap signals late-cycle behavior."""
    spending_obs = series_map.get("real_personal_spending_yoy_pct", [])
    income_obs = series_map.get("real_disposable_personal_income_yoy_pct", [])
    if not spending_obs or not income_obs:
        return []

    income_by_period = {obs.period_date: float(obs.value) for obs in income_obs}
    results: list[DerivedObservation] = []
    for obs in spending_obs:
        income_val = income_by_period.get(obs.period_date)
        if income_val is None:
            continue
        gap_value = float(obs.value) - income_val
        results.append(
            _derived_from_base(
                obs,
                series_id="spending_income_gap",
                value=round(gap_value, 3),
                unit="percent",
                report="real_spending_metrics",
                source_series_label="Real Spending vs Income YoY Gap",
                source_metric_name="spread",
                source_unit_label="percentage points",
                input_series=("real_personal_spending_yoy_pct", "real_disposable_personal_income_yoy_pct"),
            )
        )
    return results
