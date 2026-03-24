"""Census source adapter."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from urllib.parse import quote_plus
import urllib.error
import urllib.request

from consumer_dashboard import __version__
from consumer_dashboard.config.registry import SourceDefinition
from consumer_dashboard.sources.base import AcquisitionResult, BaseSourceAdapter
from consumer_dashboard.storage.filesystem import ensure_directory, write_json

CENSUS_DATASETS = {
    "mrts": {
        "label": "Monthly Retail Trade",
        "path": "https://api.census.gov/data/timeseries/eits/mrts",
        "get": "cell_value,data_type_code,time_slot_id,error_data,category_code,seasonally_adj,geo_level_code",
    },
    "resconst": {
        "label": "New Residential Construction",
        "path": "https://api.census.gov/data/timeseries/eits/resconst",
        "get": "cell_value,data_type_code,time_slot_id,error_data,category_code,seasonally_adj,geo_level_code",
    },
    "ressales": {
        "label": "New Home Sales",
        "path": "https://api.census.gov/data/timeseries/eits/ressales",
        "get": "cell_value,data_type_code,time_slot_id,error_data,category_code,seasonally_adj,geo_level_code",
    },
}


def default_census_from_time() -> str:
    # The MRTS endpoint is stricter than the housing endpoints on wide history pulls.
    # Start with a recent window that the live API accepts reliably.
    return "from 2025"


def build_census_query(dataset_key: str, time_selector: str) -> dict[str, str]:
    definition = CENSUS_DATASETS[dataset_key]
    return {
        "get": definition["get"],
        "time": time_selector,
    }


def build_census_request_url(dataset_key: str, time_selector: str) -> str:
    definition = CENSUS_DATASETS[dataset_key]
    return f"{definition['path']}?get={definition['get']}&time={quote_plus(time_selector)}"


def summarize_census_payload(rows: list[list[str]]) -> dict[str, object]:
    if not rows:
        return {"row_count": 0, "latest_period": ""}
    header, *data_rows = rows
    latest = ""
    if "time" in header:
        time_index = header.index("time")
        for row in data_rows:
            if time_index < len(row):
                latest = max(latest, row[time_index])
    return {
        "row_count": len(data_rows),
        "latest_period": latest,
    }


class CensusSourceAdapter(BaseSourceAdapter):
    source_id = "census"

    def fetch_latest(self, definition: SourceDefinition) -> AcquisitionResult:
        return self._fetch_and_store(definition, time_selector=default_census_from_time(), label="latest")

    def backfill(self, definition: SourceDefinition, start: str, end: str) -> AcquisitionResult:
        return self._fetch_and_store(definition, time_selector=f"from {start} to {end}", label=f"backfill_{start}_{end}")

    def _fetch_and_store(
        self,
        definition: SourceDefinition,
        time_selector: str,
        label: str,
    ) -> AcquisitionResult:
        fetched_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        artifact_dir = ensure_directory(self.settings.raw_dir / self.source_id / run_id)
        total_rows = 0
        latest_period = ""
        artifacts: list[str] = []

        try:
            for dataset_key, dataset in CENSUS_DATASETS.items():
                params = build_census_query(dataset_key, time_selector)
                request_url = build_census_request_url(dataset_key, time_selector)
                try:
                    response = urllib.request.urlopen(request_url, timeout=self.settings.http_timeout_seconds)
                    content = response.read()
                    status_code = getattr(response, "status", 200)
                    rows = [] if status_code == 204 else json.loads(content.decode("utf-8"))
                except urllib.error.HTTPError as exc:
                    return AcquisitionResult(
                        source_id=self.source_id,
                        status="request_failed",
                        message=f"Census request failed: HTTP {exc.code} for {dataset_key} ({exc.read().decode('utf-8')[:200]})",
                    )

                summary = summarize_census_payload(rows)
                artifact_path = artifact_dir / f"{dataset_key}_{label}.json"
                envelope = {
                    "metadata": {
                        "source_id": self.source_id,
                        "source_name": definition.source_name,
                        "dataset_key": dataset_key,
                        "dataset_label": dataset["label"],
                        "fetched_at": fetched_at,
                        "artifact_type": "api_response",
                        "adapter_version": __version__,
                        "endpoint": dataset["path"],
                        "time_selector": time_selector,
                        "request_params": params,
                        "request_url": request_url,
                        "http_status": status_code,
                    },
                    "response": rows,
                }
                envelope["metadata"]["response_sha256"] = hashlib.sha256(content).hexdigest()
                write_json(artifact_path, envelope)
                artifacts.append(str(artifact_path))
                total_rows += int(summary["row_count"])
                latest_period = max(latest_period, str(summary["latest_period"]))
        except OSError as exc:
            return AcquisitionResult(
                source_id=self.source_id,
                status="request_failed",
                message=f"Census request failed: {exc}",
            )

        return AcquisitionResult(
            source_id=self.source_id,
            status="ingested",
            message=(
                f"Downloaded {total_rows} Census observations across "
                f"{len(CENSUS_DATASETS)} datasets through {latest_period or 'unknown period'}."
            ),
            artifacts=artifacts,
        )
