"""Derived metric orchestration."""

from __future__ import annotations

from dataclasses import asdict

from consumer_dashboard.metrics.cohort import compute_cohort_stress_metrics
from consumer_dashboard.metrics.credit import compute_credit_metrics
from consumer_dashboard.metrics.common import build_series_map, load_normalized_observations
from consumer_dashboard.metrics.dfa import compute_dfa_metrics
from consumer_dashboard.metrics.inflation import compute_inflation_metrics
from consumer_dashboard.metrics.labor import compute_labor_metrics
from consumer_dashboard.metrics.real_income import compute_real_income_metrics
from consumer_dashboard.metrics.real_spending import compute_real_spending_metrics, _compute_spending_income_gap
from consumer_dashboard.metrics.housing import compute_housing_metrics
from consumer_dashboard.metrics.regime import compute_regime_metrics
from consumer_dashboard.metrics.savings import compute_savings_metrics, _compute_savings_runway
from consumer_dashboard.storage.filesystem import ensure_project_directories, write_json
from consumer_dashboard.storage.state import StateStore


def derive_metrics(settings) -> dict:
    ensure_project_directories(settings)
    normalized_observations = load_normalized_observations(settings.processed_dir)
    series_map = build_series_map(normalized_observations)

    # First pass: compute base derived series
    inflation_derived = compute_inflation_metrics(series_map)
    income_derived = compute_real_income_metrics(series_map)
    spending_derived = compute_real_spending_metrics(series_map)
    labor_derived = compute_labor_metrics(series_map)
    credit_derived = compute_credit_metrics(series_map)
    savings_derived = compute_savings_metrics(series_map)

    # Build augmented series map that includes first-pass derived series so that
    # housing and regime can consume cpi_shelter_yoy_pct, real_disposable_personal_income_yoy_pct, etc.
    augmented_series_map = dict(series_map)
    augmented_series_map.update(
        build_series_map(
            inflation_derived + income_derived + spending_derived
            + labor_derived + credit_derived + savings_derived
        )
    )

    # Second pass: metrics that depend on first-pass derived series
    housing_derived = compute_housing_metrics(augmented_series_map)
    augmented_series_map.update(build_series_map(housing_derived))

    dfa_derived = compute_dfa_metrics(augmented_series_map)
    augmented_series_map.update(build_series_map(dfa_derived))

    # Second-pass spending gap: depends on real_personal_spending_yoy_pct and
    # real_disposable_personal_income_yoy_pct from the first pass.
    spending_gap_derived = _compute_spending_income_gap(augmented_series_map)
    # Filter to only the gap series (avoid duplicating other spending series)
    spending_gap_derived = [o for o in spending_gap_derived if o.series_id == "spending_income_gap"]
    augmented_series_map.update(build_series_map(spending_gap_derived))

    # Second-pass savings runway: depends on excess_savings_cumulative_proxy from first pass.
    savings_runway_derived = _compute_savings_runway(augmented_series_map)
    augmented_series_map.update(build_series_map(savings_runway_derived))

    regime_derived = compute_regime_metrics(augmented_series_map)

    # Third pass: cohort stress metrics depend on dfa derived series and card delinquency
    full_series_map = dict(augmented_series_map)
    cohort_derived = compute_cohort_stress_metrics(full_series_map)

    # Deduplicate spending_income_gap: remove duplicates from spending_derived that
    # were already computed in first pass (they'll be empty because augmented map
    # wasn't available), and use second-pass results
    spending_derived_filtered = [o for o in spending_derived if o.series_id != "spending_income_gap"]
    savings_derived_filtered = [
        o for o in savings_derived
        if o.series_id not in ("excess_savings_monthly_burn_rate", "excess_savings_runway_months")
    ]

    derived_observations = (
        inflation_derived + income_derived + spending_derived_filtered
        + labor_derived + credit_derived + savings_derived_filtered
        + housing_derived + dfa_derived + regime_derived
        + spending_gap_derived + savings_runway_derived + cohort_derived
    )
    serialized = [asdict(observation) for observation in derived_observations]
    series_ids = sorted({observation["series_id"] for observation in serialized})
    observations_path = settings.processed_dir / "derived_observations.json"
    write_json(
        observations_path,
        {
            "source_id": "derived",
            "observation_count": len(serialized),
            "series_count": len(series_ids),
            "series_ids": series_ids,
            "observations": serialized,
        },
    )
    payload = {
        "source_id": "derived",
        "status": "derived",
        "observation_count": len(serialized),
        "series_count": len(series_ids),
        "derived_series": series_ids,
        "message": f"Computed {len(serialized)} derived observations across {len(series_ids)} series.",
        "output_path": str(observations_path),
    }
    write_json(settings.processed_dir / "derived_metrics_status.json", payload)
    StateStore(settings.state_dir).update_source("derived", payload["status"], payload["message"])
    return payload
