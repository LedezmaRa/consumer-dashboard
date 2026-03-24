"""Department of Labor source adapter."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
import html
import re

import httpx

from consumer_dashboard import __version__
from consumer_dashboard.config.registry import SourceDefinition
from consumer_dashboard.sources.base import AcquisitionResult, BaseSourceAdapter
from consumer_dashboard.storage.filesystem import ensure_directory, write_json

DOL_WEEKLY_CLAIMS_RELEASES_URL = "https://www.dol.gov/newsroom/releases?agency=39&page=0&state=All&topic=132&year=all"
LATEST_WEEKLY_CLAIMS_ENTRY_PATTERN = re.compile(
    r'<p class="dol-date-text">\s*(?P<release_date>[A-Za-z]+ \d{1,2}, \d{4})\s*</p>\s*'
    r'<a href="\s*(?P<release_path>/newsroom/releases/eta/eta\d+)\s*">\s*<h3>\s*<span>\s*'
    r"Unemployment Insurance Weekly Claims Report\s*</span>\s*</h3>\s*</a>.*?"
    r'<div class="field field--name-field-press-body[^"]*">\s*<p>(?P<summary_html>.*?)</p>',
    re.IGNORECASE | re.DOTALL,
)


def _extract_latest_claims_entry(content: str) -> dict[str, str] | None:
    match = LATEST_WEEKLY_CLAIMS_ENTRY_PATTERN.search(content)
    if not match:
        return None
    release_date = datetime.strptime(match.group("release_date"), "%B %d, %Y").date().isoformat()
    release_path = match.group("release_path").strip()
    summary_text = html.unescape(re.sub(r"<[^>]+>", " ", match.group("summary_html")))
    summary_text = re.sub(r"\s+", " ", summary_text).strip()
    return {
        "release_date": release_date,
        "release_path": release_path,
        "release_url": f"https://www.dol.gov{release_path}",
        "summary_text": summary_text,
    }


class DolSourceAdapter(BaseSourceAdapter):
    source_id = "dol"

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        artifact_dir = ensure_directory(self.settings.raw_dir / self.source_id / run_id)
        artifact_path = artifact_dir / "weekly_claims_latest.json"

        try:
            with httpx.Client(timeout=self.settings.http_timeout_seconds) as client:
                response = client.get(DOL_WEEKLY_CLAIMS_RELEASES_URL)
                response.raise_for_status()
                content = response.text
        except httpx.HTTPError as exc:
            return AcquisitionResult(
                source_id=self.source_id,
                status="request_failed",
                message=f"DOL request failed: {exc}",
            )

        latest_entry = _extract_latest_claims_entry(content)
        if latest_entry is None:
            return AcquisitionResult(
                source_id=self.source_id,
                status="parse_failed",
                message="DOL releases page did not contain a parsable weekly claims entry.",
            )

        envelope = {
            "metadata": {
                "source_id": self.source_id,
                "source_name": definition.source_name,
                "fetched_at": fetched_at,
                "artifact_type": "html_page",
                "adapter_version": __version__,
                "endpoint": DOL_WEEKLY_CLAIMS_RELEASES_URL,
                "http_status": response.status_code,
                "release_date": latest_entry["release_date"],
                "release_path": latest_entry["release_path"],
                "release_url": latest_entry["release_url"],
            },
            "summary_text": latest_entry["summary_text"],
            "response_text": content,
        }
        envelope["metadata"]["response_sha256"] = hashlib.sha256(content.encode("utf-8")).hexdigest()
        write_json(artifact_path, envelope)

        return AcquisitionResult(
            source_id=self.source_id,
            status="ingested",
            message=(
                "Downloaded the latest DOL Unemployment Insurance Weekly Claims Report "
                f"released on {latest_entry['release_date']}."
            ),
            artifacts=[str(artifact_path)],
        )
