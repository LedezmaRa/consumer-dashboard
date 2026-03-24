"""BLS source adapter."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone

import httpx

from consumer_dashboard import __version__
from consumer_dashboard.config.registry import SourceDefinition
from consumer_dashboard.sources.base import AcquisitionResult, BaseSourceAdapter
from consumer_dashboard.storage.filesystem import ensure_directory, write_json

BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_SERIES_IDS = (
    "LNS14000000",
    "CES0000000001",
    "CES0500000003",
    "CUSR0000SA0",
    "CUSR0000SA0L1E",
    # Inflation detail: shelter, owners' equivalent rent, services ex-energy
    "CUSR0000SAH1",
    "CUSR0000SAH21",
    "CUSR0000SASLE",
    "JTS000000000000000JOL",
    "JTS000000000000000QUR",
)


def default_bls_year_range(window: int = 5) -> tuple[str, str]:
    current_year = datetime.now(timezone.utc).year
    start_year = current_year - window + 1
    return str(start_year), str(current_year)


def build_bls_request_payload(
    series_ids: list[str] | tuple[str, ...],
    startyear: str,
    endyear: str,
    registration_key: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "seriesid": list(series_ids),
        "startyear": startyear,
        "endyear": endyear,
    }
    if registration_key:
        payload["registrationkey"] = registration_key
    return payload


def summarize_bls_response(payload: dict) -> dict[str, object]:
    if payload.get("status") != "REQUEST_SUCCEEDED":
        return {
            "ok": False,
            "row_count": 0,
            "latest_period": "",
            "error": "; ".join(payload.get("message", []) or ["Unknown BLS API error."]),
        }

    series = payload.get("Results", {}).get("series", [])
    row_count = sum(len(item.get("data", [])) for item in series)
    latest = ""
    for item in series:
        for row in item.get("data", []):
            period = row.get("period", "")
            if not period.startswith("M") or period == "M13":
                continue
            candidate = f"{row.get('year', '')}{period}"
            latest = max(latest, candidate)
    return {
        "ok": True,
        "row_count": row_count,
        "latest_period": latest,
        "error": "",
    }


class BlsSourceAdapter(BaseSourceAdapter):
    source_id = "bls"

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        startyear, endyear = default_bls_year_range()
        return self._fetch_and_store(definition, startyear=startyear, endyear=endyear, label="latest")

    def backfill(self, definition: SourceDefinition, start: str, end: str) -> AcquisitionResult:
        return self._fetch_and_store(
            definition,
            startyear=start[:4],
            endyear=end[:4],
            label=f"backfill_{start}_{end}",
        )

    def _fetch_and_store(
        self,
        definition: SourceDefinition,
        startyear: str,
        endyear: str,
        label: str,
    ) -> AcquisitionResult:
        payload = build_bls_request_payload(
            BLS_SERIES_IDS,
            startyear=startyear,
            endyear=endyear,
            registration_key=self.settings.bls_api_key,
        )
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        try:
            with httpx.Client(timeout=self.settings.http_timeout_seconds) as client:
                response = client.post(BLS_API_URL, json=payload)
                response.raise_for_status()
                api_payload = response.json()
        except httpx.HTTPError as exc:
            return AcquisitionResult(
                source_id=self.source_id,
                status="request_failed",
                message=f"BLS request failed: {exc}",
            )

        summary = summarize_bls_response(api_payload)
        if not summary["ok"]:
            return AcquisitionResult(
                source_id=self.source_id,
                status="api_error",
                message=f"BLS API error: {summary['error']}",
            )

        artifact_dir = ensure_directory(self.settings.raw_dir / self.source_id / run_id)
        artifact_path = artifact_dir / f"bls_core_{label}.json"
        envelope = {
            "metadata": {
                "source_id": self.source_id,
                "source_name": definition.source_name,
                "fetched_at": fetched_at,
                "artifact_type": "api_response",
                "adapter_version": __version__,
                "endpoint": BLS_API_URL,
                "startyear": startyear,
                "endyear": endyear,
                "series_ids": list(BLS_SERIES_IDS),
                "request_payload": {
                    **payload,
                    **({"registrationkey": "***redacted***"} if self.settings.bls_api_key else {}),
                },
                "http_status": response.status_code,
            },
            "response": api_payload,
        }
        envelope["metadata"]["response_sha256"] = hashlib.sha256(response.content).hexdigest()
        write_json(artifact_path, envelope)

        return AcquisitionResult(
            source_id=self.source_id,
            status="ingested",
            message=(
                f"Downloaded {summary['row_count']} BLS observations across "
                f"{len(BLS_SERIES_IDS)} series through {summary['latest_period'] or 'unknown period'}."
            ),
            artifacts=[str(artifact_path)],
        )
