"""Generate AI-written narrative reports for each dashboard section using Claude."""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from datetime import date
from typing import Any


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _build_system_prompt() -> str:
    today = date.today().strftime("%B %d, %Y")
    return (
        f"Today's date is {today}. "
        "You are a senior macro-economic analyst writing concise investor briefings. "
        "Write in clear, confident prose. No bullet lists — flowing paragraphs only. "
        "3–4 paragraphs max. Do not use markdown headers. "
        "\n\n"
        "GROUNDING RULES — follow these without exception:\n"
        "1. DATA FIRST: Every factual claim must begin by citing the exact metric name, "
        "its value, and its reference period as provided in the data. State the number, "
        "then interpret it. Never state a conclusion before citing the supporting number.\n"
        "2. USE COMPUTED METRICS: The data includes pre-computed derived metrics "
        "(e.g., real_wage_growth, cpi_headline_yoy_pct). Always use these computed values "
        "directly. Do not attempt to re-derive or re-calculate them from components — "
        "your arithmetic may conflict with the authoritative computed figure.\n"
        "3. SEPARATE CURRENT FROM FORWARD: Distinguish sharply between (a) what the data "
        "currently shows — stated in present tense with the metric value — and (b) what "
        "might happen in the future — stated in conditional language (e.g., 'if X continues, "
        "Y may follow'). Never present a forward risk as a current fact.\n"
        "4. NO CONTRADICTION: Before drawing any conclusion, check that it is consistent "
        "with all metrics provided. If two metrics appear to pull in different directions, "
        "acknowledge both explicitly and resolve the tension rather than ignoring one.\n"
        "5. PERCENTILE CONTEXT: Where a percentile rank is provided, use it to characterize "
        "severity (e.g., 91st percentile = historically elevated). Do not characterize a "
        "metric as extreme without this support.\n"
        "6. TIME ANCHORING: All forward-looking statements must be anchored to today's date. "
        "Do not reference past dates as if they are in the future."
    )


_SECTION_PROMPTS: dict[str, str] = {
    "fast-read": (
        "Write an executive-level snapshot of overall US consumer health based on the "
        "key indicators below. For each claim, cite the metric value and period first, "
        "then state what it means. Summarize the most important signal across labor, "
        "inflation, spending, and stress in 2–3 punchy paragraphs. Use the regime "
        "classification and pillar scores as your organizing framework."
    ),
    "labor": (
        "Write an investor briefing on the US labor market using the metrics below. "
        "Structure your analysis as follows: (1) State each metric's current value and "
        "period before interpreting it. (2) Use the pre-computed real_wage_growth figure "
        "directly — do not re-derive purchasing power from nominal wages and CPI separately. "
        "(3) Address employment trends, wage dynamics, and jobless claims in sequence. "
        "(4) Conclude with a forward-looking paragraph that uses conditional language and "
        "identifies specific threshold values to watch (e.g., the exact claims level or "
        "unemployment rate that would signal deterioration)."
    ),
    "inflation": (
        "Write an investor briefing on US inflation using the metrics below. "
        "Structure your analysis as follows: (1) Cite each inflation metric's current "
        "value and period before interpreting its direction. (2) Distinguish between "
        "headline CPI, core CPI, PCE headline, and core PCE — they measure different "
        "things and may diverge. (3) Use the pre-computed real_wage_growth and "
        "real_disposable_personal_income_yoy_pct values directly to assess purchasing "
        "power — do not re-derive these from components. (4) Assess shelter and services "
        "inflation separately. (5) Conclude with what the trajectory means for Fed policy "
        "and consumer purchasing power, using conditional language for any forward view."
    ),
    "spending": (
        "Write an investor briefing on US consumer spending using the metrics below. "
        "Structure your analysis as follows: (1) State each metric's value and period "
        "before interpreting it. (2) Distinguish between nominal and real spending — "
        "use the real figures as the authoritative measure of volume. (3) Connect income, "
        "savings rate, and outlays: if spending is growing faster than income, identify "
        "whether savings or credit is funding the gap. (4) Assess retail sales and housing "
        "activity. (5) Conclude with a conditional forward view tied to specific metrics "
        "the reader should monitor."
    ),
    "stress": (
        "Write an investor briefing on US consumer financial stress using the metrics below. "
        "Structure your analysis as follows: (1) State each delinquency and credit metric's "
        "value, period, and percentile rank (if provided) before interpreting it. "
        "(2) Distinguish between revolving credit (credit cards) and nonrevolving credit "
        "(auto, student). (3) Use the 90+ day delinquency rate as the primary stress "
        "indicator, with credit card and auto delinquency as sub-components. "
        "(4) Identify which cohorts are most stressed if the data supports it. "
        "(5) Flag only systemic risks that are supported by specific metric values — "
        "do not generalize from isolated data points."
    ),
    "distribution": (
        "Write an investor briefing on US wealth and income distribution using the metrics "
        "below. Structure your analysis as follows: (1) State each distributional metric's "
        "value and period before interpreting it. (2) Use the wealth concentration ratio "
        "and bottom-50% net worth figures as the primary lens. (3) Connect distribution "
        "to aggregate demand: explain how the wealth split affects spending resilience "
        "at the macro level. (4) Distinguish between top-end wealth effects (asset prices, "
        "equities) and bottom-end income effects (wages, transfers). (5) Use conditional "
        "language for any forward-looking claim about consumption."
    ),
    "housing": (
        "Write an investor briefing on the US housing market using the metrics below. "
        "Structure your analysis as follows: (1) State each metric's value and period "
        "before interpreting it. (2) Separate supply indicators (starts, permits) from "
        "demand indicators (new home sales). (3) Connect housing to consumer balance "
        "sheets — home equity is a key wealth and borrowing variable. (4) Assess "
        "affordability using the shelter and OER inflation data. (5) Conclude with a "
        "conditional forward view tied to specific housing metrics the reader should watch."
    ),
    "psychology": (
        "Write an investor briefing on US consumer sentiment and inflation expectations "
        "using the metrics below. Structure your analysis as follows: (1) State the "
        "Michigan Sentiment index value, its period, and its percentile rank before "
        "interpreting it. (2) State the 1-year inflation expectations value and period — "
        "this is the short-run signal that drives near-term spending behavior and wage "
        "demands. (3) Explain the gap between sentiment and hard spending data if one "
        "exists, using both sets of values. (4) Conclude with what the expectation level "
        "implies for Fed policy credibility and consumer behavior, using conditional language."
    ),
    "investor-guide": (
        "Write a strategic investor guide based on the consumer health framework below. "
        "Structure your analysis as follows: (1) Open by stating the current regime "
        "classification and composite score. (2) For each active playbook, state the "
        "trigger conditions and current metric values that validate or invalidate the "
        "playbook. (3) Synthesize into actionable investor takeaways — sector positioning, "
        "risk appetite, and timing signals. (4) Use conditional language throughout: "
        "state what data levels would confirm or reverse each view."
    ),
}

_DEFAULT_PROMPT = (
    "Write an investor briefing for the following consumer economic data. "
    "State each metric's value and period before interpreting it. "
    "Distinguish current conditions from forward risks. "
    "Summarize key trends and investment implications in 3 paragraphs."
)


def _build_section_summary(section: dict[str, Any]) -> str:
    """Flatten a dashboard section into a rich text summary for the AI prompt."""
    lines: list[str] = []
    title = section.get("title") or section.get("label") or section.get("id", "")
    lines.append(f"Section: {title}")
    lines.append("")

    # --- Cards: current value, delta, period, tone, percentile, trend, why ---
    cards = section.get("cards", [])
    if cards:
        lines.append("CURRENT METRICS:")
        for card in cards:
            if not card:
                continue
            label = card.get("title") or card.get("series_id", "")
            value = card.get("value_display") or str(card.get("value", ""))
            period = card.get("period_label", "")
            delta = card.get("delta_display", "")
            tone = card.get("tone", "")
            momentum = card.get("trend_momentum", "")
            pct_rank = card.get("percentile_rank")
            why = card.get("why_it_matters", "")

            line = f"  {label}: {value}"
            if period:
                line += f" ({period})"
            if delta:
                line += f" | change: {delta}"
            if tone:
                line += f" | signal: {tone}"
            if momentum:
                line += f" | trend: {momentum}"
            if pct_rank is not None:
                line += f" | historical percentile: {pct_rank:.0f}th"
            lines.append(line)
            if why:
                lines.append(f"    → {why}")

            # 3-period history for trend context
            history = card.get("history", [])
            if history and len(history) >= 2:
                recent = history[-3:] if len(history) >= 3 else history[-2:]
                hist_str = ", ".join(
                    f"{h.get('period_label', h.get('period_date',''))}: {h.get('value','')}"
                    for h in recent
                )
                lines.append(f"    recent trend: {hist_str}")

    # --- Chart series: labeled with series_id and last 3 values ---
    chart = section.get("chart", {})
    chart_series = chart.get("series", [])
    if chart_series:
        lines.append("")
        lines.append("CHART SERIES (computed metrics):")
        for series in chart_series:
            if not series:
                continue
            # Use title then series_id — the bug was using "label" which doesn't exist
            s_label = series.get("title") or series.get("series_id", "")
            s_id = series.get("series_id", "")
            unit = series.get("unit", "")
            pts = series.get("raw_points") or []
            if not pts:
                continue
            last3 = pts[-3:]
            pts_str = ", ".join(
                f"{p.get('label', '')}: {round(p['value'], 4) if isinstance(p.get('value'), float) else p.get('value', '')}"
                for p in last3
            )
            line = f"  {s_label}"
            if s_id and s_id != s_label:
                line += f" [{s_id}]"
            if unit:
                line += f" ({unit})"
            line += f": {pts_str}"
            lines.append(line)

    return "\n".join(lines)


def _build_investor_guide_summary(guide: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"Section: {guide.get('title', 'Investor Guide')}")
    setup = guide.get("current_setup", "")
    if setup:
        lines.append(f"Current Setup: {setup}")
    lines.append("")
    lines.append("PLAYBOOKS:")
    for pb in guide.get("playbooks", []):
        if not pb:
            continue
        lines.append(f"  Playbook: {pb.get('title', '')}")
        lines.append(f"    Summary: {pb.get('summary', '')}")
        triggers = pb.get("trigger_signals", [])
        if triggers:
            lines.append(f"    Trigger signals: {'; '.join(str(t) for t in triggers)}")
    return "\n".join(lines)


def _build_fast_read_summary(executive: dict[str, Any]) -> str:
    lines: list[str] = ["Section: Fast Read — Executive Snapshot"]
    lines.append("")

    headline = executive.get("headline", "")
    if headline:
        lines.append(f"Headline: {headline}")

    regime = executive.get("regime", {})
    if regime:
        lines.append(
            f"Regime: {regime.get('regime_display', '')} "
            f"(composite score={regime.get('composite_score', '')})"
        )

    lines.append("")
    lines.append("PILLAR SCORES:")
    for pillar in executive.get("pillars", []):
        if pillar:
            lines.append(
                f"  [{pillar.get('tone', '')}] {pillar.get('title', '')}: "
                f"{pillar.get('stance', '')} — {pillar.get('detail', '')}"
            )

    lines.append("")
    lines.append("KEY METRICS:")
    for card in executive.get("cards", []):
        if not card:
            continue
        label = card.get("title") or card.get("series_id", "")
        value = card.get("value_display") or str(card.get("value", ""))
        period = card.get("period_label", "")
        delta = card.get("delta_display", "")
        tone = card.get("tone", "")
        momentum = card.get("trend_momentum", "")
        line = f"  {label}: {value}"
        if period:
            line += f" ({period})"
        if delta:
            line += f" | change: {delta}"
        if tone:
            line += f" | signal: {tone}"
        if momentum:
            line += f" | trend: {momentum}"
        lines.append(line)

    positives = executive.get("positives", [])
    watchlist = executive.get("watchlist", [])
    if positives:
        lines.append("")
        lines.append("What improved: " + "; ".join(positives))
    if watchlist:
        lines.append("Watch: " + "; ".join(watchlist))

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Claude API call
# ---------------------------------------------------------------------------

def _call_claude(api_key: str, system: str, user_message: str) -> str:
    """Call the Claude API synchronously via urllib (no extra dependencies)."""
    url = "https://api.anthropic.com/v1/messages"
    payload = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 1024,
        "system": system,
        "messages": [{"role": "user", "content": user_message}],
    }).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read().decode("utf-8"))

    return body["content"][0]["text"].strip()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_ai_reports(payload: dict[str, Any], api_key: str) -> dict[str, str]:
    """
    Generate one AI narrative per dashboard section.

    Returns a dict mapping section_id -> report_text.
    Returns error strings for sections that fail.
    """
    reports: dict[str, str] = {}

    executive = payload.get("executive_snapshot", {})
    sections = payload.get("sections", [])
    investor_guide = payload.get("investor_guide", {})

    # Fast read uses the executive snapshot
    _generate_one(
        section_id="fast-read",
        summary=_build_fast_read_summary(executive),
        api_key=api_key,
        reports=reports,
    )

    # Each data section
    for section in sections:
        sid = section.get("id", "")
        if not sid or sid == "fast-read":
            continue
        _generate_one(
            section_id=sid,
            summary=_build_section_summary(section),
            api_key=api_key,
            reports=reports,
        )

    # Investor guide
    _generate_one(
        section_id="investor-guide",
        summary=_build_investor_guide_summary(investor_guide),
        api_key=api_key,
        reports=reports,
    )

    return reports


def _generate_one(
    section_id: str,
    summary: str,
    api_key: str,
    reports: dict[str, str],
) -> None:
    today = date.today().strftime("%B %d, %Y")
    prompt = _SECTION_PROMPTS.get(section_id, _DEFAULT_PROMPT)
    user_message = (
        f"Report date: {today}\n\n"
        f"{prompt}\n\n"
        "IMPORTANT: Cite each metric's exact value and period before interpreting it. "
        "Use pre-computed derived metrics (e.g., real_wage_growth) directly — do not "
        "re-derive them from components. Distinguish current conditions from forward risks.\n\n"
        f"Data:\n{summary}"
    )
    try:
        text = _call_claude(api_key, _build_system_prompt(), user_message)
        reports[section_id] = text
    except Exception as exc:  # noqa: BLE001
        reports[section_id] = f"[Report unavailable: {exc}]"
