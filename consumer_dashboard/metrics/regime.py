"""Consumer regime classification.

Classifies the overall consumer health state into one of four regimes:
  - expansion: labor firm, inflation contained, spending healthy, stress low
  - slowing: mixed signals, one or more pillars softening
  - stressed: multiple pillars flashing caution, spending fading or credit deteriorating
  - recessionary: broad deterioration across labor, spending, and stress indicators

The regime is derived from a composite score across the four pillar dimensions
(labor, inflation, spending power, stress) using the same thresholds the
dashboard cards already define for tone classification.
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime

from consumer_dashboard.metrics.common import _derived_from_base
from consumer_dashboard.models.observation import DerivedObservation, Observation


# ---------------------------------------------------------------------------
# Pillar scoring helpers
# ---------------------------------------------------------------------------

def _latest(series_map: dict[str, list[Observation]], series_id: str) -> Observation | None:
    observations = series_map.get(series_id, [])
    return observations[-1] if observations else None


def _momentum_adjustment(
    series_map: dict[str, list[Observation]],
    series_id: str,
    periods: int = 3,
    low_is_good: bool = True,
) -> float:
    """Compute a signed momentum adjustment in the range [-0.5, +0.5].

    Looks at the average delta over the last `periods` observations.
    For low_is_good series (e.g. unemployment): falling trend -> +0.5 (good),
    rising trend -> -0.5 (bad).
    For high_is_good series (e.g. real wages): rising trend -> +0.5 (good),
    falling trend -> -0.5 (bad).
    """
    observations = series_map.get(series_id, [])
    if len(observations) < periods + 1:
        return 0.0
    recent = observations[-(periods + 1):]
    deltas = [recent[i].value - recent[i - 1].value for i in range(1, len(recent))]
    avg_delta = sum(deltas) / len(deltas)
    # Normalise: clamp to [-0.5, +0.5]
    reference = abs(recent[-1].value) * 0.01 if recent[-1].value != 0 else 0.1
    if reference == 0:
        return 0.0
    raw = avg_delta / (reference * 10)  # 10% move per period = full ±0.5
    clamped = max(-0.5, min(0.5, raw))
    return -clamped if low_is_good else clamped


def _score_low_is_good(value: float, good_threshold: float, neutral_threshold: float) -> int:
    """Return +1 (positive), 0 (neutral), or -1 (caution)."""
    if value <= good_threshold:
        return 1
    if value <= neutral_threshold:
        return 0
    return -1


def _score_high_is_good(value: float, good_threshold: float, neutral_threshold: float) -> int:
    if value >= good_threshold:
        return 1
    if value >= neutral_threshold:
        return 0
    return -1


def _pillar_labor(series_map: dict[str, list[Observation]]) -> int | None:
    """Score labor pillar: unemployment, claims, and real wages.

    Each level score is blended with a 25% momentum weight so that a rising
    unemployment rate at 4.0% scores differently from a falling one at 4.0%.
    """
    scores: list[float] = []
    unemp = _latest(series_map, "unemployment_rate")
    if unemp is not None:
        base = _score_low_is_good(unemp.value, 4.2, 4.8)
        adj = _momentum_adjustment(series_map, "unemployment_rate", low_is_good=True)
        scores.append(base + adj * 0.25)
    claims = _latest(series_map, "initial_jobless_claims_4_week_average")
    if claims is not None:
        base = _score_low_is_good(claims.value, 220_000, 260_000)
        adj = _momentum_adjustment(series_map, "initial_jobless_claims_4_week_average", low_is_good=True)
        scores.append(base + adj * 0.25)
    real_wages = _latest(series_map, "real_wage_growth")
    if real_wages is not None:
        base = _score_high_is_good(real_wages.value, 1.0, 0.0)
        adj = _momentum_adjustment(series_map, "real_wage_growth", low_is_good=False)
        scores.append(base + adj * 0.25)
    return round(sum(scores) / len(scores) * 100) if scores else None


def _pillar_inflation(series_map: dict[str, list[Observation]]) -> int | None:
    """Score inflation pillar: CPI, core PCE, and inflation expectations anchoring."""
    scores: list[float] = []
    cpi = _latest(series_map, "cpi_headline_yoy_pct")
    if cpi is not None:
        base = _score_low_is_good(cpi.value, 2.5, 3.25)
        adj = _momentum_adjustment(series_map, "cpi_headline_yoy_pct", low_is_good=True)
        scores.append(base + adj * 0.25)
    core_pce = _latest(series_map, "core_pce_price_index_yoy_pct")
    if core_pce is not None:
        base = _score_low_is_good(core_pce.value, 2.7, 3.2)
        adj = _momentum_adjustment(series_map, "core_pce_price_index_yoy_pct", low_is_good=True)
        scores.append(base + adj * 0.25)
    # Phase 5 hook: inflation expectations anchoring. If 5Y expectations are
    # unanchored (>3.5%), subtract from the pillar even if current CPI looks OK.
    exp_5y = _latest(series_map, "michigan_inflation_expectations_5y")
    if exp_5y is not None:
        scores.append(_score_low_is_good(exp_5y.value, 3.0, 3.5))
    return round(sum(scores) / len(scores) * 100) if scores else None


def _pillar_spending(series_map: dict[str, list[Observation]]) -> int | None:
    """Score spending pillar: real spending, real DPI, savings, and shelter squeeze."""
    scores: list[float] = []
    spending = _latest(series_map, "real_personal_spending_yoy_pct")
    if spending is not None:
        base = _score_high_is_good(spending.value, 1.0, 0.0)
        adj = _momentum_adjustment(series_map, "real_personal_spending_yoy_pct", low_is_good=False)
        scores.append(base + adj * 0.25)
    dpi = _latest(series_map, "real_disposable_personal_income_yoy_pct")
    if dpi is not None:
        base = _score_high_is_good(dpi.value, 1.0, 0.0)
        adj = _momentum_adjustment(series_map, "real_disposable_personal_income_yoy_pct", low_is_good=False)
        scores.append(base + adj * 0.25)
    savings = _latest(series_map, "savings_rate")
    if savings is not None:
        base = _score_high_is_good(savings.value, 4.5, 3.5)
        adj = _momentum_adjustment(series_map, "savings_rate", low_is_good=False)
        scores.append(base + adj * 0.25)
    # Phase 6 hook: shelter affordability squeeze depresses spending pillar
    shelter_squeeze = _latest(series_map, "shelter_affordability_squeeze")
    if shelter_squeeze is not None:
        scores.append(_score_low_is_good(shelter_squeeze.value, 0.5, 2.0))
    return round(sum(scores) / len(scores) * 100) if scores else None


def _pillar_stress(series_map: dict[str, list[Observation]]) -> int | None:
    """Score stress pillar: delinquency rates and credit growth.

    Now includes credit card delinquency, auto loan delinquency, and revolving
    credit growth rate so the pillar is no longer limited to NY Fed quarterly data.
    """
    scores: list[int] = []

    # Broad household delinquency (NY Fed quarterly)
    delinq = _latest(series_map, "household_debt_90_plus_delinquent_rate")
    if delinq is not None:
        scores.append(_score_low_is_good(delinq.value, 2.5, 3.5))

    new_delinq = _latest(series_map, "new_serious_delinquent_total_rate")
    if new_delinq is not None:
        scores.append(_score_low_is_good(new_delinq.value, 2.8, 4.0))

    # Credit card delinquency (NY Fed quarterly — most sensitive to lower-income stress)
    card_delinq = _latest(series_map, "household_credit_card_90_plus_delinquent_rate")
    if card_delinq is not None:
        scores.append(_score_low_is_good(card_delinq.value, 7.5, 10.0))

    # Auto loan delinquency (NY Fed quarterly — early-warning for subprime stress)
    auto_delinq = _latest(series_map, "household_auto_loan_90_plus_delinquent_rate")
    if auto_delinq is not None:
        scores.append(_score_low_is_good(auto_delinq.value, 3.0, 4.5))

    # Revolving credit growth — rapid growth relative to income is a precursor (G.19 monthly)
    revolving_yoy = _latest(series_map, "consumer_credit_revolving_yoy_pct")
    if revolving_yoy is not None:
        scores.append(_score_low_is_good(revolving_yoy.value, 5.0, 10.0))

    return round(sum(scores) / len(scores) * 100) if scores else None


# ---------------------------------------------------------------------------
# Regime classification
# ---------------------------------------------------------------------------

REGIME_EXPANSION = "expansion"
REGIME_SLOWING = "slowing"
REGIME_STRESSED = "stressed"
REGIME_RECESSIONARY = "recessionary"


def classify_regime(
    series_map: dict[str, list[Observation]],
) -> tuple[str, float, dict[str, int | None]]:
    """Return (regime_label, composite_score, pillar_scores).

    Composite score ranges from -100 (all pillars caution) to +100 (all positive).
    Regime thresholds:
      >=  34  -> expansion
      >=   0  -> slowing
      >= -50  -> stressed
      <  -50  -> recessionary
    """
    pillar_scores = {
        "labor": _pillar_labor(series_map),
        "inflation": _pillar_inflation(series_map),
        "spending": _pillar_spending(series_map),
        "stress": _pillar_stress(series_map),
    }
    valid = [v for v in pillar_scores.values() if v is not None]
    if not valid:
        return REGIME_SLOWING, 0.0, pillar_scores

    composite = sum(valid) / len(valid)

    if composite >= 34:
        regime = REGIME_EXPANSION
    elif composite >= 0:
        regime = REGIME_SLOWING
    elif composite >= -50:
        regime = REGIME_STRESSED
    else:
        regime = REGIME_RECESSIONARY

    return regime, composite, pillar_scores


def compute_regime_metrics(
    series_map: dict[str, list[Observation]],
) -> list[DerivedObservation]:
    """Compute regime classification as derived observations.

    Produces three series:
      - consumer_regime_composite: the composite score (-100 to +100)
      - consumer_regime_label: encoded as 1=expansion, 0.5=slowing, -0.5=stressed, -1=recessionary
      - consumer_regime_pillar_*: individual pillar scores
    """
    regime, composite, pillar_scores = classify_regime(series_map)

    # Find the most recent period date from any input series we used
    candidate_dates: list[str] = []
    for sid in (
        "unemployment_rate", "initial_jobless_claims_4_week_average",
        "real_wage_growth", "cpi_headline_yoy_pct", "core_pce_price_index_yoy_pct",
        "real_personal_spending_yoy_pct", "real_disposable_personal_income_yoy_pct",
        "savings_rate", "household_debt_90_plus_delinquent_rate",
        "new_serious_delinquent_total_rate",
        # Phase 2: credit stress additions
        "household_credit_card_90_plus_delinquent_rate",
        "household_auto_loan_90_plus_delinquent_rate",
        "consumer_credit_revolving_yoy_pct",
    ):
        obs = _latest(series_map, sid)
        if obs is not None:
            candidate_dates.append(obs.period_date)

    if not candidate_dates:
        return []

    latest_date = max(candidate_dates)
    # Use the observation from unemployment_rate as a base if available
    base = _latest(series_map, "unemployment_rate")
    if base is None:
        # Fallback: pick any available base
        for sid in ("cpi_headline_yoy_pct", "real_personal_spending_yoy_pct"):
            base = _latest(series_map, sid)
            if base is not None:
                break
    if base is None:
        return []

    regime_label_value = {
        REGIME_EXPANSION: 1.0,
        REGIME_SLOWING: 0.5,
        REGIME_STRESSED: -0.5,
        REGIME_RECESSIONARY: -1.0,
    }

    derived: list[DerivedObservation] = []

    derived.append(_derived_from_base(
        base,
        series_id="consumer_regime_composite",
        value=composite,
        report="regime_metrics",
        unit="score",
        source_series_label="Consumer Regime Composite Score",
        source_metric_name="regime_composite",
        source_unit_label="score (-100 to +100)",
        input_series=tuple(sorted(
            sid for sid in (
                "unemployment_rate", "initial_jobless_claims_4_week_average",
                "real_wage_growth", "cpi_headline_yoy_pct",
                "core_pce_price_index_yoy_pct",
                "real_personal_spending_yoy_pct",
                "real_disposable_personal_income_yoy_pct",
                "savings_rate", "household_debt_90_plus_delinquent_rate",
                "new_serious_delinquent_total_rate",
                # Phase 2: credit stress additions
                "household_credit_card_90_plus_delinquent_rate",
                "household_auto_loan_90_plus_delinquent_rate",
                "consumer_credit_revolving_yoy_pct",
            ) if sid in series_map
        )),
    ))

    derived.append(_derived_from_base(
        base,
        series_id="consumer_regime_label",
        value=regime_label_value.get(regime, 0.0),
        report="regime_metrics",
        unit="regime_code",
        source_series_label=f"Consumer Regime: {regime.title()}",
        source_metric_name="regime_label",
        source_unit_label="1=expansion, 0.5=slowing, -0.5=stressed, -1=recessionary",
        input_series=("consumer_regime_composite",),
    ))

    for pillar_name, pillar_score in pillar_scores.items():
        if pillar_score is not None:
            derived.append(_derived_from_base(
                base,
                series_id=f"consumer_regime_pillar_{pillar_name}",
                value=float(pillar_score),
                report="regime_metrics",
                unit="score",
                source_series_label=f"Regime Pillar: {pillar_name.title()}",
                source_metric_name=f"regime_pillar_{pillar_name}",
                source_unit_label="score (-100 to +100)",
                input_series=("consumer_regime_composite",),
            ))

    # Phase 1: Regime momentum — aggregate directional signal across anchor series.
    # Positive = conditions improving on balance; negative = deteriorating.
    momentum_scores: list[float] = []
    for sid, lig in (
        ("unemployment_rate", True),
        ("cpi_headline_yoy_pct", True),
        ("real_personal_spending_yoy_pct", False),
        ("household_debt_90_plus_delinquent_rate", True),
    ):
        adj = _momentum_adjustment(series_map, sid, low_is_good=lig)
        if adj != 0.0:
            momentum_scores.append(adj)
    if momentum_scores:
        momentum_value = sum(momentum_scores) / len(momentum_scores)  # -0.5 to +0.5
        derived.append(_derived_from_base(
            base,
            series_id="consumer_regime_momentum",
            value=round(momentum_value * 100, 1),  # scale to -50 to +50 for readability
            report="regime_metrics",
            unit="score",
            source_series_label="Consumer Regime Momentum (Directional)",
            source_metric_name="regime_momentum",
            source_unit_label="score (-50 to +50; positive=improving)",
            input_series=("consumer_regime_composite",),
        ))

    return derived
