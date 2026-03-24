"""Real spending metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import build_series_map, compute_deflated_level_proxy, compute_pct_change
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
    return sorted(derived, key=lambda item: (item.period_date, item.series_id))
