"""Credit and stress metric calculations."""

from __future__ import annotations

from consumer_dashboard.metrics.common import _derived_from_base, compute_pct_change
from consumer_dashboard.models.observation import DerivedObservation, Observation


def compute_credit_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Compute credit growth rates and stress signals from G.19 and NY Fed data.

    Derived series produced:
      - consumer_credit_revolving_yoy_pct    Revolving credit YoY growth (borrowing pressure)
      - consumer_credit_total_yoy_pct        Total credit outstanding YoY growth
      - household_credit_card_balance_yoy_pct Card balance YoY growth vs income
      - delinquency_acceleration             Second derivative of 90+ day delinquency
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

    derived.extend(_compute_delinquency_acceleration(series_map))

    return sorted(derived, key=lambda item: (item.period_date, item.series_id))


def _compute_delinquency_acceleration(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Second derivative of the 90+ day delinquency rate.

    Catches rising momentum before the level looks alarming.
    """
    del_obs = series_map.get("household_debt_90_plus_delinquent_rate", [])
    if len(del_obs) < 3:
        return []

    # Sort by period_date
    del_obs_sorted = sorted(del_obs, key=lambda o: o.period_date)
    period_map = {obs.period_date: float(obs.value) for obs in del_obs_sorted}
    periods = [obs.period_date for obs in del_obs_sorted]

    # First derivative: level change period-to-period
    first_deriv: dict[str, float] = {}
    for i in range(1, len(periods)):
        first_deriv[periods[i]] = period_map[periods[i]] - period_map[periods[i - 1]]

    # Second derivative: change in first derivative
    first_deriv_keys = sorted(first_deriv.keys())
    results: list[DerivedObservation] = []
    obs_by_period = {obs.period_date: obs for obs in del_obs_sorted}
    for i in range(1, len(first_deriv_keys)):
        p_curr = first_deriv_keys[i]
        p_prev = first_deriv_keys[i - 1]
        accel = first_deriv[p_curr] - first_deriv[p_prev]
        base_obs = obs_by_period[p_curr]
        results.append(
            _derived_from_base(
                base_obs,
                series_id="delinquency_acceleration",
                value=round(accel, 4),
                unit="percent",
                report="household_debt_credit",
                source_series_label="Delinquency Rate Acceleration (2nd Derivative)",
                source_metric_name="delinquency_acceleration",
                source_unit_label="percentage points",
                input_series=("household_debt_90_plus_delinquent_rate",),
            )
        )
    return results
