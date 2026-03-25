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
        "Be direct about what the data says, what it means for consumers, and what "
        "investors should watch. 3–4 paragraphs max. Do not use markdown headers. "
        "All forward-looking statements must be anchored to the current date above — "
        "do not reference past dates as if they are in the future."
    )

_SECTION_PROMPTS: dict[str, str] = {
    "fast-read": (
        "Write a brief executive-level snapshot of the overall US consumer health "
        "based on the following key indicators. Summarize the most important signal "
        "across labor, inflation, spending, and stress in 2–3 punchy paragraphs."
    ),
    "labor": (
        "Write an investor briefing on the US labor market based on the following "
        "metrics. Cover employment trends, wage dynamics, and what the data implies "
        "for consumer income and spending power going forward."
    ),
    "inflation": (
        "Write an investor briefing on US inflation based on the following metrics. "
        "Assess the trajectory of consumer prices, shelter costs, and services inflation. "
        "Explain what this means for purchasing power and Fed policy expectations."
    ),
    "spending": (
        "Write an investor briefing on US consumer spending based on the following metrics. "
        "Assess the real spending trend, the relationship between income and outlays, "
        "and what sector-level data reveals about consumer priorities."
    ),
    "stress": (
        "Write an investor briefing on US consumer financial stress based on the following "
        "metrics. Cover credit card and auto delinquency, revolving credit growth, and "
        "household debt dynamics. Flag any systemic risks or early warning signs."
    ),
    "distribution": (
        "Write an investor briefing on US wealth and income distribution based on the "
        "following metrics. Analyze how wealth is spread across income groups, the "
        "implications of concentration at the top, and what this means for aggregate demand."
    ),
    "housing": (
        "Write an investor briefing on the US housing market based on the following metrics. "
        "Cover affordability, new home sales, housing starts and permits, and what current "
        "conditions mean for consumer balance sheets and the broader economy."
    ),
    "psychology": (
        "Write an investor briefing on US consumer sentiment and expectations based on the "
        "following metrics. Interpret the Michigan Sentiment index and inflation expectations "
        "data, and explain how consumer psychology might affect near-term spending behavior."
    ),
    "investor-guide": (
        "Write a strategic investor guide based on the following consumer health framework. "
        "Synthesize the current regime, playbooks, and setup into actionable investor takeaways. "
        "What does the full picture say about risk appetite, sector positioning, and timing?"
    ),
}

_DEFAULT_PROMPT = (
    "Write an investor briefing for the following consumer economic data. "
    "Summarize the key trends and their investment implications in 3 paragraphs."
)


def _build_section_summary(section: dict[str, Any]) -> str:
    """Flatten a dashboard section into a compact text summary for the prompt."""
    lines: list[str] = []
    title = section.get("title") or section.get("label") or section.get("id", "")
    lines.append(f"Section: {title}")

    for card in section.get("cards", []):
        if not card:
            continue
        label = card.get("title") or card.get("label") or card.get("series_id", "")
        value = card.get("value_display") or card.get("value", "")
        delta = card.get("delta_display", "")
        tone = card.get("tone", "")
        trend = card.get("trend_direction", "")
        why = card.get("why_it_matters", "")
        line = f"  - {label}: {value}"
        if delta:
            line += f" ({delta})"
        if tone:
            line += f" [{tone}]"
        if trend:
            line += f" trend={trend}"
        if why:
            line += f" | {why}"
        lines.append(line)

    # Include chart series summary if present
    chart = section.get("chart", {})
    if chart:
        for series in chart.get("series", []):
            if not series:
                continue
            pts = series.get("raw_points") or series.get("rebased_points") or []
            if pts:
                last = pts[-1] if pts else {}
                s_label = series.get("label", "")
                s_val = last.get("y", "")
                lines.append(f"  - Chart series '{s_label}': latest={s_val}")

    return "\n".join(lines)


def _build_investor_guide_summary(guide: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append(f"Section: {guide.get('title', 'Investor Guide')}")
    lines.append(f"Setup: {guide.get('current_setup', '')}")
    for pb in guide.get("playbooks", []):
        if not pb:
            continue
        lines.append(f"  Playbook: {pb.get('title', '')} — {pb.get('summary', '')}")
    return "\n".join(lines)


def _build_fast_read_summary(executive: dict[str, Any]) -> str:
    lines: list[str] = ["Section: Fast Read — Executive Snapshot"]
    lines.append(f"Headline: {executive.get('headline', '')}")
    regime = executive.get("regime", {})
    lines.append(
        f"Regime: {regime.get('regime_display', '')} (score={regime.get('composite_score', '')})"
    )
    for pillar in executive.get("pillars", []):
        if pillar:
            lines.append(
                f"  Pillar [{pillar.get('tone', '')}]: {pillar.get('title', '')} — {pillar.get('stance', '')} {pillar.get('detail', '')}"
            )
    for card in executive.get("cards", []):
        if not card:
            continue
        label = card.get("title") or card.get("series_id", "")
        value = card.get("value_display") or card.get("value", "")
        delta = card.get("delta_display", "")
        tone = card.get("tone", "")
        line = f"  - {label}: {value}"
        if delta:
            line += f" ({delta})"
        if tone:
            line += f" [{tone}]"
        lines.append(line)
    positives = executive.get("positives", [])
    watchlist = executive.get("watchlist", [])
    if positives:
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
    Returns empty strings for sections that fail.
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
    user_message = f"Report date: {today}\n\n{prompt}\n\nData:\n{summary}"
    try:
        text = _call_claude(api_key, _build_system_prompt(), user_message)
        reports[section_id] = text
    except Exception as exc:  # noqa: BLE001
        reports[section_id] = f"[Report unavailable: {exc}]"
