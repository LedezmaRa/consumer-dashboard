"""Credit and stress metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import compute_pct_change
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_credit_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Compute credit growth rates and stress signals from G.19 and NY Fed data.

    Derived series produced:
      - consumer_credit_revolving_yoy_pct    Revolving credit YoY growth (borrowing pressure)
      - consumer_credit_total_yoy_pct        Total credit outstanding YoY growth
      - household_credit_card_balance_yoy_pct Card balance YoY growth vs income
    """
    derived: list[DerivedObservation] = []

    # Revolving credit YoY growth — the most fragile, expensive segment
    derived.extend(
        compute_pct_change(
            series_map,
            input_series_id="consumer_credit_revolving",
            output_series_id="consumer_credit_revolving_yoy_pct",
            output_label="Revolving Consumer Credit Year-over-Year Growth",
            months_lag=12,
            report="credit_metrics",
            metric_name="year_over_year_pct_change",
        )
    )

    # Total consumer credit YoY growth
    derived.extend(
        compute_pct_change(
            series_map,
            input_series_id="consumer_credit_total",
            output_series_id="consumer_credit_total_yoy_pct",
            output_label="Total Consumer Credit Year-over-Year Growth",
            months_lag=12,
            report="credit_metrics",
            metric_name="year_over_year_pct_change",
        )
    )

    # Credit card balance YoY growth (from NY Fed household debt data)
    derived.extend(
        compute_pct_change(
            series_map,
            input_series_id="household_credit_card_balance",
            output_series_id="household_credit_card_balance_yoy_pct",
            output_label="Household Credit Card Balance Year-over-Year Growth",
            months_lag=4,  # NY Fed is quarterly; 4 quarters = 1 year
            report="credit_metrics",
            metric_name="year_over_year_pct_change",
        )
    )

    return sorted(derived, key=lambda item: (item.period_date, item.series_id))
