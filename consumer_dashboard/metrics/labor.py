"""Labor metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import build_series_map, compute_deflated_level_proxy, compute_pct_change
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_labor_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    derived = compute_deflated_level_proxy(
        series_map,
        nominal_series_id="average_hourly_earnings",
        price_series_id="cpi_headline",
        output_series_id="real_average_hourly_earnings_proxy",
        output_label="Real Average Hourly Earnings Proxy",
        report="labor_metrics",
    )
    augmented_series_map = dict(series_map)
    augmented_series_map.update(build_series_map(derived))
    derived.extend(
        compute_pct_change(
            augmented_series_map,
            input_series_id="real_average_hourly_earnings_proxy",
            output_series_id="real_wage_growth",
            output_label="Real Wage Growth Year-over-Year",
            months_lag=12,
            report="labor_metrics",
            metric_name="year_over_year_pct_change",
        )
    )
    return sorted(derived, key=lambda item: (item.period_date, item.series_id))
