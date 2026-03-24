"""University of Michigan Consumer Sentiment source adapter.

Uses FRED public CSV endpoints (no API key required) as a reliable
alternative to the Michigan SCA fetchchart.php endpoint which returns HTML.

FRED series used:
  UMCSENT  = University of Michigan: Consumer Sentiment (overall index)
  MICH     = University of Michigan: Inflation Expectation (5-10 Year ahead)
  MICH1YR  = University of Michigan: Inflation Expectation (1 Year ahead)

Data is made available by the University of Michigan Survey of Consumers
via FRED (Federal Reserve Bank of St. Louis). Non-commercial research use.
"""

from __future__ import annotations

import hashlib
import io
from datetime import datetime, timezone

import httpx

from consumer_dashboard import __version__
from consumer_dashboard.config.registry import SourceDefinition
from consumer_dashboard.sources.base import AcquisitionResult, BaseSourceAdapter
from consumer_dashboard.storage.filesystem import ensure_directory, write_json

FRED_CSV_BASE = "https://fred.stlouisfed.org/graph/fredgraph.csv"

# FRED series ID → canonical series_id used by the normalize / metrics layers
FRED_SERIES = {
    "UMCSENT": "michigan_sentiment_index",
    "MICH": "michigan_inflation_expectations_5y",
}


class MichiganSourceAdapter(BaseSourceAdapter):
    source_id = "university_of_michigan"

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        now = datetime.now(timezone.utc)
        run_id = now.strftime("%Y%m%dT%H%M%SZ")
        fetched_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        artifact_dir = ensure_directory(self.settings.raw_dir / self.source_id / run_id)

        artifacts: list[str] = []
        total_rows = 0

        for fred_id, series_id in FRED_SERIES.items():
            params = {"id": fred_id}
            try:
                with httpx.Client(timeout=self.settings.http_timeout_seconds) as client:
                    response = client.get(FRED_CSV_BASE, params=params)
                    response.raise_for_status()
                    raw_text = response.text
            except httpx.HTTPError as exc:
                return AcquisitionResult(
                    source_id=self.source_id,
                    status="request_failed",
                    message=f"FRED request failed for {fred_id}: {exc}",
                )

            # FRED CSV format: header line "DATE,<SERIES_ID>" then data rows
            parsed_rows: list[dict[str, object]] = []
            for line in io.StringIO(raw_text):
                line = line.strip()
                if not line or line.lower().startswith("date") or line.lower().startswith("observation_date"):
                    continue
                parts = line.split(",")
                if len(parts) < 2:
                    continue
                date_str = parts[0].strip()
                val_str = parts[1].strip()
                if not date_str or not val_str or val_str in {".", "N/A", ""}:
                    continue
                parsed_rows.append({"date": date_str, "value": val_str})

            total_rows += len(parsed_rows)
            artifact_path = artifact_dir / f"michigan_{series_id}.json"
            envelope = {
                "metadata": {
                    "source_id": self.source_id,
                    "source_name": definition.source_name,
                    "fetched_at": fetched_at,
                    "artifact_type": "fred_csv",
                    "adapter_version": __version__,
                    "endpoint": FRED_CSV_BASE,
                    "fred_series_id": fred_id,
                    "series_id": series_id,
                    "row_count": len(parsed_rows),
                    "response_sha256": hashlib.sha256(raw_text.encode()).hexdigest(),
                },
                "series_id": series_id,
                "data": parsed_rows,
            }
            write_json(artifact_path, envelope)
            artifacts.append(str(artifact_path))

        return AcquisitionResult(
            source_id=self.source_id,
            status="ingested",
            message=(
                f"Downloaded {total_rows} Michigan SCA observations across "
                f"{len(FRED_SERIES)} series via FRED."
            ),
            artifacts=artifacts,
        )

    def backfill(self, definition: SourceDefinition, start: str, end: str) -> AcquisitionResult:
        # FRED CSV returns full history; reuse fetch_latest
        return self.fetch_latest(definition)
