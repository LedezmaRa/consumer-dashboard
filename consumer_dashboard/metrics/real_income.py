"""Real income metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import compute_pct_change
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_real_income_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    return compute_pct_change(
        series_map,
        input_series_id="real_disposable_personal_income",
        output_series_id="real_disposable_personal_income_yoy_pct",
        output_label="Real Disposable Personal Income Year-over-Year Growth",
        months_lag=12,
        report="real_income_metrics",
        metric_name="year_over_year_pct_change",
    )
