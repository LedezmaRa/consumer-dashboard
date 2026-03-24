"""Monthly consumer memo generator.

Produces a structured, deterministic narrative from the live dashboard payload.
The narrative is template-based (not LLM-generated) so it is fully auditable
and reproducible — the same data always produces the same text.

Output: data/processed/monthly_memo_YYYY_MM.json

Structure:
  executive_summary    3-4 sentence plain-English read on the regime
  pillar_reads         One paragraph per pillar (stance + data + educational consequence)
  key_risks            Top watchlist items + extreme-caution series
  what_to_watch        Key data releases in the coming 30 days
  data_vintage         Sources that are stale or aging
"""

from __future__ import annotations

from datetime import datetime, timezone

from consumer_dashboard.dashboard.datasets import build_dashboard_data
from consumer_dashboard.storage.filesystem import ensure_project_directories, write_json


# ---------------------------------------------------------------------------
# Narrative templates by regime
# ---------------------------------------------------------------------------

_REGIME_INTROS = {
    "expansion": (
        "The U.S. consumer is operating in expansion mode. Labor markets remain firm, "
        "inflation is running at a level that is not yet materially eroding purchasing "
        "power, and household spending continues to expand in real terms. Stress indicators "
        "remain contained, though they warrant ongoing monitoring."
    ),
    "slowing": (
        "The U.S. consumer backdrop is in a slowing phase with mixed signals across the "
        "four pillars. At least one dimension — labor, inflation, spending, or stress — "
        "is showing meaningful softness while others hold up. This is not yet a stress "
        "scenario, but the balance of evidence has shifted toward caution."
    ),
    "stressed": (
        "Multiple consumer pillars are flashing caution simultaneously. Spending momentum "
        "is fading, credit conditions are tightening, or labor is deteriorating faster than "
        "the headline narrative suggests. A stressed regime does not mean recession is "
        "imminent, but it does mean the probability distribution has shifted materially."
    ),
    "recessionary": (
        "The consumer backdrop has deteriorated to a recessionary configuration. Broad "
        "deterioration is visible across labor, spending, and stress simultaneously. Historical "
        "patterns suggest that when this regime classification is active, subsequent quarters "
        "of negative real spending growth become significantly more probable."
    ),
}

_PILLAR_EDUCATIONAL_CONSEQUENCES = {
    "labor": {
        "positive": (
            "A firm labor market is the foundation of consumer spending. When employment is "
            "stable and wages are growing, households can sustain spending from income rather "
            "than credit. This reduces systemic vulnerability even if other pressures emerge."
        ),
        "neutral": (
            "Labor is cooling but not cracking. Historically, a cooling labor market in this "
            "range is consistent with continued spending growth, but it removes the cushion "
            "that absorbs other shocks. Watch for claims acceleration as the leading signal "
            "of a transition from cooling to deteriorating."
        ),
        "caution": (
            "Labor stress is the most dangerous consumer signal because it attacks income — "
            "the primary funding source for spending. When labor cracks, spending, credit, and "
            "sentiment typically follow in sequence. The key question is speed: a slow "
            "deterioration allows adjustment; a rapid one does not."
        ),
    },
    "inflation": {
        "positive": (
            "Cooling inflation is a net positive for household purchasing power even if it "
            "happens slowly. When inflation runs below wage growth, real incomes improve and "
            "households regain the spending headroom they lost during the inflation surge. "
            "This is the structural support behind any durable consumer recovery."
        ),
        "neutral": (
            "Sticky inflation means the cost-of-living squeeze has not fully resolved. "
            "Households are still paying more for the same basket of goods and services, "
            "which constrains the amount of income available for discretionary spending. "
            "The risk is that inflation becomes entrenched in expectations, which would "
            "make the policy response — and the consumer adjustment — more painful."
        ),
        "caution": (
            "Re-accelerating inflation is among the most damaging scenarios for the consumer "
            "because it erodes purchasing power faster than incomes can adjust. Combined with "
            "a Fed that responds by tightening, it produces the double-hit of higher prices "
            "and higher borrowing costs simultaneously."
        ),
    },
    "spending": {
        "positive": (
            "Real spending growth confirms that household demand is expanding in volume terms, "
            "not just nominal terms. This is the most direct validation that income and "
            "confidence are translating into actual economic activity. Sustained real spending "
            "growth supports corporate revenues, employment, and credit quality."
        ),
        "neutral": (
            "Mixed spending signals mean households are making tradeoffs — likely shifting "
            "from discretionary to essential, or sustaining spending in services while "
            "cutting goods. This pattern is consistent with a mid-cycle slowdown rather "
            "than outright contraction, but it requires monitoring to determine whether "
            "the mix shift leads to aggregate deceleration."
        ),
        "caution": (
            "Fading real spending is the clearest sign that consumer fundamentals are "
            "under pressure. When spending contracts in real terms, it typically follows "
            "a sequence of deteriorating income, rising costs, or depleted savings buffers. "
            "Spending contraction tends to be self-reinforcing through the employment channel: "
            "less spending means less revenue, which leads to less hiring."
        ),
    },
    "stress": {
        "positive": (
            "Contained household stress means the balance-sheet dimension of the consumer "
            "story is not yet adding to cyclical risk. Low delinquency and moderate credit "
            "growth signal that households are managing their debt obligations without "
            "strain, which gives the cycle more durability even when income or spending "
            "pressures emerge."
        ),
        "neutral": (
            "Slowly building stress is a yellow flag, not a red one. Historically, "
            "delinquency rates rise gradually before they spike. The current trajectory "
            "requires monitoring, particularly in credit cards and auto loans — the "
            "segments most sensitive to lower-income household cash flow and the first "
            "to show deterioration in previous cycles."
        ),
        "caution": (
            "Rising household stress signals that the credit channel is beginning to "
            "constrain consumer behavior. When delinquencies rise, lenders tighten "
            "standards, which reduces credit availability. This creates a feedback loop: "
            "higher stress leads to tighter credit, which reduces spending, which weakens "
            "employment, which causes more stress. Breaking this loop typically requires "
            "either policy intervention or time."
        ),
    },
}

# Key release calendar (simplified; month/day windows)
_RELEASE_CALENDAR = [
    {"name": "CPI (BLS)", "typical_window": "Second week of the month", "section": "inflation"},
    {"name": "Jobs Report (BLS)", "typical_window": "First Friday of the month", "section": "labor"},
    {"name": "PCE / Personal Income (BEA)", "typical_window": "Last Friday of the month", "section": "spending"},
    {"name": "Retail Sales (Census)", "typical_window": "Mid-month, ~15th", "section": "spending"},
    {"name": "Initial Jobless Claims (DOL)", "typical_window": "Every Thursday", "section": "labor"},
    {"name": "Michigan Sentiment (U of M)", "typical_window": "Second and fourth Friday", "section": "psychology"},
    {"name": "Consumer Credit G.19 (Fed)", "typical_window": "~7th of following month", "section": "stress"},
]


# ---------------------------------------------------------------------------
# Narrative builders
# ---------------------------------------------------------------------------

def _build_executive_summary(regime: str, composite_score: float, pillars: list[dict]) -> str:
    intro = _REGIME_INTROS.get(regime, _REGIME_INTROS["slowing"])
    pillar_tones = {p["id"]: p["tone"] for p in pillars}
    caution_pillars = [p["title"] for p in pillars if p["tone"] == "caution"]
    positive_pillars = [p["title"] for p in pillars if p["tone"] == "positive"]

    if positive_pillars:
        strength_line = f"Key strengths: {', '.join(positive_pillars)}."
    else:
        strength_line = "No pillar is currently registering as outright positive."

    if caution_pillars:
        risk_line = f"Areas requiring attention: {', '.join(caution_pillars)}."
    else:
        risk_line = "No pillar is currently in caution territory."

    return f"{intro} {strength_line} {risk_line} Composite regime score: {composite_score:+.0f} (scale: +100 all positive, -100 all caution)."


def _build_pillar_reads(pillars: list[dict], snapshots: dict) -> list[dict]:
    reads = []
    for pillar in pillars:
        pillar_id = pillar["id"]
        tone = pillar["tone"]
        stance = pillar["stance"]
        detail = pillar["detail"]
        consequence = _PILLAR_EDUCATIONAL_CONSEQUENCES.get(pillar_id, {}).get(tone, "")
        paragraph = f"{pillar['title']} is {stance.lower()}. {detail} {consequence}"
        reads.append({
            "pillar_id": pillar_id,
            "pillar_title": pillar["title"],
            "tone": tone,
            "stance": stance,
            "narrative": paragraph.strip(),
        })
    return reads


def _build_key_risks(watchlist: list[str], extreme_cautions: list[dict]) -> list[str]:
    risks = list(watchlist)
    for item in extreme_cautions:
        line = (
            f"{item['title']} is at {item['value_display']} "
            f"(at the {item['percentile_rank']:.0f}th percentile of its historical range — elevated)."
        )
        if line not in risks:
            risks.append(line)
    return risks[:6]


def _build_data_vintage_notes(stale_sources: list[dict]) -> list[str]:
    if not stale_sources:
        return ["All data sources are current."]
    notes = []
    for source in stale_sources:
        notes.append(
            f"{source['source'].replace('_', ' ').title()}: latest available data is "
            f"{source['latest_period']} (as of {source['latest_release']}). "
            f"Consider re-running ingest before drawing conclusions from this source."
        )
    return notes


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def generate_monthly_memo(settings) -> dict:
    """Generate and write the monthly consumer narrative memo."""
    ensure_project_directories(settings)

    # Load the live dashboard payload
    dashboard = build_dashboard_data(settings)
    memo_ready = dashboard.get("memo_ready", {})

    regime_info = memo_ready.get("regime", {})
    regime_label = str(regime_info.get("regime", "slowing"))
    composite_score = float(regime_info.get("composite_score", 0.0))
    pillars = memo_ready.get("pillars", [])
    positives = memo_ready.get("positives", [])
    watchlist = memo_ready.get("watchlist", [])
    snapshots = memo_ready.get("key_snapshots", {})
    extreme_cautions = memo_ready.get("extreme_cautions", [])
    stale_sources = memo_ready.get("stale_sources", [])

    now = datetime.now(timezone.utc)
    period_label = now.strftime("%B %Y")

    memo = {
        "generated_at": now.isoformat(),
        "period": period_label,
        "regime": regime_label,
        "composite_score": composite_score,
        "executive_summary": _build_executive_summary(regime_label, composite_score, pillars),
        "pillar_reads": _build_pillar_reads(pillars, snapshots),
        "positives": positives,
        "key_risks": _build_key_risks(watchlist, extreme_cautions),
        "what_to_watch": _RELEASE_CALENDAR,
        "data_vintage": _build_data_vintage_notes(stale_sources),
        "key_snapshots": snapshots,
    }

    month_slug = now.strftime("%Y_%m")
    output_path = settings.processed_dir / f"monthly_memo_{month_slug}.json"
    write_json(output_path, memo)
    write_json(settings.processed_dir / "monthly_memo_status.json", {
        "generated_at": memo["generated_at"],
        "period": period_label,
        "regime": regime_label,
        "output_path": str(output_path),
        "message": f"Monthly memo generated for {period_label}. Regime: {regime_label.title()}.",
    })
    return memo
