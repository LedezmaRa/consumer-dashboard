"""BEA source adapter."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from datetime import datetime, timezone
from urllib.parse import urlencode
from typing import Any

import httpx

from consumer_dashboard import __version__
from consumer_dashboard.config.registry import SourceDefinition
from consumer_dashboard.sources.base import AcquisitionResult, BaseSourceAdapter
from consumer_dashboard.storage.filesystem import ensure_directory, write_json

BEA_API_URL = "https://apps.bea.gov/api/data"
BEA_DATASET = "NIPA"
BEA_PERSONAL_INCOME_TABLE = "T20600"
BEA_PERSONAL_INCOME_TABLE_LABEL = "Personal Income and Its Disposition"
BEA_PCE_PRICE_TABLE = "T20804"
BEA_PCE_PRICE_TABLE_LABEL = "Price Indexes for Personal Consumption Expenditures"
BEA_RESULT_FORMAT = "JSON"


def default_bea_years(window: int = 5) -> str:
    current_year = datetime.now(timezone.utc).year
    start_year = current_year - window + 1
    return ",".join(str(year) for year in range(start_year, current_year + 1))


def build_bea_request_params(user_id: str, years: str | None = None) -> dict[str, str]:
    return {
        "UserID": user_id,
        "method": "GetData",
        "datasetname": BEA_DATASET,
        "TableName": BEA_PERSONAL_INCOME_TABLE,
        "Frequency": "M",
        "Year": years or default_bea_years(),
        "ResultFormat": BEA_RESULT_FORMAT,
    }


def summarize_bea_response(payload: dict[str, Any]) -> dict[str, Any]:
    bea_api = payload.get("BEAAPI", {})
    error = bea_api.get("Error") or bea_api.get("Results", {}).get("Error")
    if error:
        return {
            "ok": False,
            "row_count": 0,
            "latest_period": "",
            "error": (
                error.get("APIErrorDescription", "Unknown BEA API error.")
                + (
                    f" {error.get('ErrorDetail', {}).get('Description', '')}".rstrip()
                    if isinstance(error.get("ErrorDetail"), dict)
                    else ""
                )
            ).strip(),
        }
    results = bea_api.get("Results", {})
    rows = results.get("Data", [])
    periods = sorted({row.get("TimePeriod", "") for row in rows if row.get("TimePeriod")})
    return {
        "ok": True,
        "row_count": len(rows),
        "latest_period": periods[-1] if periods else "",
        "error": "",
    }


def build_bea_artifact_filename(table_name: str, label: str) -> str:
    if table_name == BEA_PERSONAL_INCOME_TABLE:
        prefix = "personal_income_outlays"
    elif table_name == BEA_PCE_PRICE_TABLE:
        prefix = "pce_price_indexes"
    else:
        prefix = table_name.lower()
    return f"{prefix}_{label}.json"


class BeaSourceAdapter(BaseSourceAdapter):
    source_id = "bea"

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        if not self.settings.bea_api_key:
            return AcquisitionResult(
                source_id=self.source_id,
                status="needs_api_key",
                message=(
                    "BEA ingestion requires a BEA API key. Set BEA_API_KEY in "
                    ".env after registering at https://apps.bea.gov/api/signup/."
                ),
            )

        return self._fetch_and_store(definition, years=default_bea_years(), label="latest")

    def backfill(self, definition: SourceDefinition, start: str, end: str) -> AcquisitionResult:
        if not self.settings.bea_api_key:
            return AcquisitionResult(
                source_id=self.source_id,
                status="needs_api_key",
                message=(
                    "BEA backfill requires a BEA API key. Set BEA_API_KEY in "
                    ".env after registering at https://apps.bea.gov/api/signup/."
                ),
            )

        start_year = int(start[:4])
        end_year = int(end[:4])
        years = ",".join(str(year) for year in range(start_year, end_year + 1))
        return self._fetch_and_store(definition, years=years, label=f"backfill_{start}_{end}")

    def _fetch_and_store(
        self,
        definition: SourceDefinition,
        years: str,
        label: str,
    ) -> AcquisitionResult:
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        artifact_dir = ensure_directory(self.settings.raw_dir / self.source_id / run_id)
        tables = [
            (BEA_PERSONAL_INCOME_TABLE, BEA_PERSONAL_INCOME_TABLE_LABEL),
            (BEA_PCE_PRICE_TABLE, BEA_PCE_PRICE_TABLE_LABEL),
        ]
        artifacts: list[str] = []
        total_rows = 0
        latest_period = ""

        try:
            with httpx.Client(timeout=self.settings.http_timeout_seconds) as client:
                for table_name, table_label in tables:
                    params = build_bea_request_params(self.settings.bea_api_key, years=years)
                    params["TableName"] = table_name
                    response = client.get(BEA_API_URL, params=params)
                    response.raise_for_status()
                    api_payload = response.json()
                    summary = summarize_bea_response(api_payload)
                    if not summary["ok"]:
                        return AcquisitionResult(
                            source_id=self.source_id,
                            status="api_error",
                            message=f"BEA API error for {table_name}: {summary['error']}",
                        )
                    artifact_path = artifact_dir / build_bea_artifact_filename(table_name, label)
                    envelope = {
                        "metadata": {
                            "source_id": self.source_id,
                            "source_name": definition.source_name,
                            "fetched_at": fetched_at,
                            "artifact_type": "api_response",
                            "adapter_version": __version__,
                            "endpoint": BEA_API_URL,
                            "dataset": BEA_DATASET,
                            "table_name": table_name,
                            "table_label": table_label,
                            "years": years,
                            "frequency": "M",
                            "result_format": BEA_RESULT_FORMAT,
                            "request_params": {**params, "UserID": "***redacted***"},
                            "request_url": f"{BEA_API_URL}?{urlencode({**params, 'UserID': '***redacted***'})}",
                            "http_status": response.status_code,
                        },
                        "response": api_payload,
                    }
                    envelope["metadata"]["response_sha256"] = hashlib.sha256(
                        response.content
                    ).hexdigest()
                    write_json(artifact_path, envelope)
                    artifacts.append(str(artifact_path))
                    total_rows += summary["row_count"]
                    latest_period = max(latest_period, summary["latest_period"] or "")
        except httpx.HTTPError as exc:
            return AcquisitionResult(
                source_id=self.source_id,
                status="request_failed",
                message=f"BEA request failed: {exc}",
            )

        return AcquisitionResult(
            source_id=self.source_id,
            status="ingested",
            message=(
                f"Downloaded {total_rows} BEA rows across {len(tables)} tables "
                f"through {latest_period or 'unknown period'}."
            ),
            artifacts=artifacts,
        )
