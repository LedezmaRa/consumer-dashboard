"""Savings rate depth metrics.

Produces three derived series that give the dashboard a multi-period lens on
the savings rate rather than treating it as a single snapshot value:

  savings_rate_3m_avg
    3-month moving average of the savings rate. Smooths seasonal noise and
    gives a cleaner read on whether the buffer is growing or shrinking.

  savings_rate_yoy_chg
    Absolute percentage-point change year-over-year. Captures direction in
    a way that the level alone cannot: a savings rate of 3.5% down from 5.5%
    a year ago signals very different household behavior than 3.5% up from 2.0%.

  excess_savings_cumulative_proxy
    Cumulative savings above (positive) or below (negative) the long-run
    pre-pandemic median savings rate, denominated in billions of dollars.
    When this turns deeply negative, the pandemic buffer is fully exhausted
    and spending must be funded from current income or new debt — a structural
    inflection point for consumer sustainability.
"""

from __future__ import annotations

import statistics

from consumer_dashboard.metrics.common import _derived_from_base
from consumer_dashboard.models.observation import DerivedObservation, Observation

# Pre-pandemic baseline window: use observations before 2020-01-01 to establish
# the long-run trend savings rate.
_BASELINE_CUTOFF = "2020-01-01"


def compute_savings_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    derived: list[DerivedObservation] = []
    derived.extend(_compute_savings_3m_avg(series_map))
    derived.extend(_compute_savings_yoy_chg(series_map))
    derived.extend(_compute_excess_savings(series_map))
    derived.extend(_compute_savings_runway(series_map))
    return sorted(derived, key=lambda item: (item.period_date, item.series_id))


def _compute_savings_3m_avg(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    observations = series_map.get("savings_rate", [])
    if len(observations) < 3:
        return []
    derived: list[DerivedObservation] = []
    for i in range(2, len(observations)):
        window = [observations[i - 2].value, observations[i - 1].value, observations[i].value]
        avg = sum(window) / 3.0
        derived.append(
            _derived_from_base(
                observations[i],
                series_id="savings_rate_3m_avg",
                value=round(avg, 3),
                report="savings_metrics",
                unit="percent",
                source_series_label="Personal Savings Rate 3-Month Moving Average",
                source_metric_name="savings_rate_3m_avg",
                source_unit_label="percent",
                input_series=("savings_rate",),
            )
        )
    return derived


def _compute_savings_yoy_chg(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Absolute percentage-point change vs 12 months prior."""
    observations = series_map.get("savings_rate", [])
    if not observations:
        return []
    by_period = {obs.period_date: obs for obs in observations}
    derived: list[DerivedObservation] = []
    from consumer_dashboard.metrics.common import shift_months
    for current in observations:
        prior_date = shift_months(current.period_date, 12)
        prior = by_period.get(prior_date)
        if prior is None:
            continue
        chg = current.value - prior.value
        derived.append(
            _derived_from_base(
                current,
                series_id="savings_rate_yoy_chg",
                value=round(chg, 3),
                report="savings_metrics",
                unit="percent",
                source_series_label="Personal Savings Rate Year-over-Year Change (pp)",
                source_metric_name="savings_rate_yoy_chg",
                source_unit_label="percentage points",
                input_series=("savings_rate",),
            )
        )
    return derived


def _compute_excess_savings(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Cumulative excess (or deficit) savings relative to pre-pandemic trend.

    Methodology:
    1. Compute the median savings rate from all observations prior to _BASELINE_CUTOFF
       as the long-run trend rate.
    2. For each month, excess_savings_gap = savings_rate - trend_rate (pp).
    3. Multiply the gap by disposable_personal_income / 1200 to convert to a
       monthly dollar flow in billions (DPI is annual; divide by 12 for monthly;
       divide by 100 for percent to fraction; net result is /1200).
    4. Cumulate the dollar flows to produce a running stock of excess (or deficit)
       savings in billions of dollars.
    """
    savings_obs = series_map.get("savings_rate", [])
    dpi_obs = series_map.get("disposable_personal_income", [])
    if not savings_obs or not dpi_obs:
        return []

    # Establish baseline from pre-pandemic observations
    baseline_values = [obs.value for obs in savings_obs if obs.period_date < _BASELINE_CUTOFF]
    if len(baseline_values) < 6:
        # Fallback: use long-run historical median of ~7% if insufficient history
        trend_rate = 7.0
    else:
        trend_rate = statistics.median(baseline_values)

    # Build DPI lookup (annual rate, billions of dollars)
    dpi_by_period = {obs.period_date: obs for obs in dpi_obs}

    cumulative_billions: float = 0.0
    derived: list[DerivedObservation] = []
    for obs in savings_obs:
        dpi = dpi_by_period.get(obs.period_date)
        if dpi is None:
            continue
        gap_pct = obs.value - trend_rate  # percentage points above/below trend
        # Monthly excess savings in billions: DPI (annual billions) * gap / 1200
        monthly_excess = dpi.value * gap_pct / 1200.0
        cumulative_billions += monthly_excess
        derived.append(
            _derived_from_base(
                obs,
                series_id="excess_savings_cumulative_proxy",
                value=round(cumulative_billions, 1),
                report="savings_metrics",
                unit="billions_of_dollars",
                source_series_label="Cumulative Excess Savings Proxy (vs Pre-Pandemic Trend)",
                source_metric_name="excess_savings_cumulative_proxy",
                source_unit_label="billions of dollars; positive=above trend",
                input_series=("savings_rate", "disposable_personal_income"),
            )
        )
    return derived


def _compute_savings_runway(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Savings runway metrics from excess_savings_cumulative_proxy.

    Produces:
      - excess_savings_monthly_burn_rate   3-month trailing average burn (change per month)
      - excess_savings_runway_months       months remaining at current burn rate
    """
    excess_obs = series_map.get("excess_savings_cumulative_proxy", [])
    if len(excess_obs) < 4:
        return []

    sorted_obs = sorted(excess_obs, key=lambda o: o.period_date)
    levels = [(obs.period_date, float(obs.value)) for obs in sorted_obs]

    # Monthly changes
    changes = [levels[i][1] - levels[i - 1][1] for i in range(1, len(levels))]

    results: list[DerivedObservation] = []
    # Start from index 2 in changes (0-based) to have a 3-month window
    for i in range(2, len(changes)):
        burn_3m = statistics.mean(changes[i - 2: i + 1])
        current_level = levels[i + 1][1]
        base_obs = sorted_obs[i + 1]

        results.append(
            _derived_from_base(
                base_obs,
                series_id="excess_savings_monthly_burn_rate",
                value=round(burn_3m, 2),
                report="savings_metrics",
                unit="billions_of_dollars",
                source_series_label="Excess Savings Monthly Burn Rate (3M Avg)",
                source_metric_name="excess_savings_monthly_burn_rate",
                source_unit_label="billions of dollars per month",
                input_series=("excess_savings_cumulative_proxy",),
            )
        )

        if current_level > 0 and burn_3m < 0:
            runway = min(60.0, current_level / abs(burn_3m))
        else:
            runway = 0.0

        results.append(
            _derived_from_base(
                base_obs,
                series_id="excess_savings_runway_months",
                value=round(runway, 1),
                report="savings_metrics",
                unit="ratio; level",
                source_series_label="Excess Savings Runway (Months at Current Burn Rate)",
                source_metric_name="excess_savings_runway_months",
                source_unit_label="months",
                input_series=("excess_savings_cumulative_proxy",),
            )
        )

    return results
