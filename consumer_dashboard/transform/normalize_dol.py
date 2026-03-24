"""Normalize DOL weekly claims page content."""

from __future__ import annotations

from datetime import datetime
import html
import re

from consumer_dashboard.models.observation import Observation

CLAIMS_PATTERN = re.compile(
    r"In the week ending (?P<week_ending>[A-Za-z]+ \d{1,2}), "
    r"the advance figure for seasonally adjusted initial claims was "
    r"(?P<initial>[\d,]+).*?The 4-week moving average was "
    r"(?P<average>[\d,]+)",
    re.IGNORECASE | re.DOTALL,
)


def _strip_html(value: str) -> str:
    text = re.sub(r"<script.*?</script>", " ", value, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_release_date(value: str) -> datetime | None:
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%SZ", "%B %d, %Y"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _infer_week_ending_date(label: str, release_date: str) -> str:
    release = _parse_release_date(release_date)
    if release is None:
        raise ValueError(f"Unsupported DOL release date: {release_date}")
    return datetime.strptime(f"{label} {release.year}", "%B %d %Y").date().isoformat()


def normalize_dol_payload(payload) -> list[Observation]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    release_date = metadata.get("release_date", metadata.get("fetched_at", ""))
    artifact_path = metadata.get("artifact_path", "")
    text_source = ""
    if isinstance(payload, dict):
        text_source = str(payload.get("summary_text", "")) or str(payload.get("response_text", ""))
    cleaned = _strip_html(text_source)
    match = CLAIMS_PATTERN.search(cleaned)
    if not match:
        return []

    week_ending_label = match.group("week_ending")
    period_date = _infer_week_ending_date(week_ending_label, release_date)
    initial_claims = float(match.group("initial").replace(",", ""))
    moving_average = float(match.group("average").replace(",", ""))

    return [
        Observation(
            series_id="initial_jobless_claims",
            period_date=period_date,
            value=initial_claims,
            frequency="weekly",
            unit="claims",
            source="dol",
            report="initial_jobless_claims",
            release_date=release_date,
            reference_period=week_ending_label,
            vintage=period_date,
            seasonal_adjustment="seasonally_adjusted",
            source_series_label="Initial Jobless Claims",
            source_table_name="Unemployment Insurance Weekly Claims Report",
            source_line_number="initial_claims",
            source_metric_name="level",
            source_unit_label="claims",
            artifact_path=artifact_path,
        ),
        Observation(
            series_id="initial_jobless_claims_4_week_average",
            period_date=period_date,
            value=moving_average,
            frequency="weekly",
            unit="claims",
            source="dol",
            report="initial_jobless_claims",
            release_date=release_date,
            reference_period=week_ending_label,
            vintage=period_date,
            seasonal_adjustment="seasonally_adjusted",
            source_series_label="Initial Jobless Claims 4-Week Average",
            source_table_name="Unemployment Insurance Weekly Claims Report",
            source_line_number="initial_claims_4_week_average",
            source_metric_name="level",
            source_unit_label="claims",
            artifact_path=artifact_path,
        ),
    ]
