"""Tests for the BEA adapter and normalizer."""

from dataclasses import replace
from pathlib import Path
import unittest

from consumer_dashboard.config.registry import get_source_definition
from consumer_dashboard.config.settings import Settings
from consumer_dashboard.sources.bea import (
    BEA_PCE_PRICE_TABLE,
    BEA_PERSONAL_INCOME_TABLE,
    BeaSourceAdapter,
    build_bea_request_params,
)
from consumer_dashboard.storage.filesystem import read_json
from consumer_dashboard.transform.normalize_bea import normalize_bea_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "consumer_reports_manifest.csv"
FIXTURE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "bea_personal_income_sample.json"


class BeaTests(unittest.TestCase):
    def test_build_bea_request_params_uses_expected_table(self) -> None:
        params = build_bea_request_params("demo-key", years="2024,2025")
        self.assertEqual(params["datasetname"], "NIPA")
        self.assertEqual(params["TableName"], BEA_PERSONAL_INCOME_TABLE)
        self.assertEqual(params["Year"], "2024,2025")

    def test_normalize_bea_payload_extracts_expected_series(self) -> None:
        payload = read_json(FIXTURE_PATH, default={})
        observations = normalize_bea_payload(payload)
        series_ids = [observation.series_id for observation in observations]
        self.assertEqual(
            series_ids,
            [
                "disposable_personal_income",
                "pce_price_index",
                "personal_consumption_expenditures",
                "personal_income",
                "real_disposable_personal_income",
                "savings_rate",
            ],
        )
        self.assertEqual(observations[0].period_date, "2025-11-01")
        self.assertEqual(observations[-1].release_date, "2026-03-23T17:45:00Z")
        self.assertEqual(observations[0].source_table_name, "T20600")

    def test_normalize_bea_price_fixture_extracts_pce_indexes(self) -> None:
        payload = read_json(PROJECT_ROOT / "tests" / "fixtures" / "bea_pce_price_sample.json", default={})
        observations = normalize_bea_payload(payload)
        series_ids = [observation.series_id for observation in observations]
        self.assertEqual(series_ids, ["core_pce_price_index", "pce_price_index"])
        self.assertTrue(all(observation.source_table_name == BEA_PCE_PRICE_TABLE for observation in observations))

    def test_bea_adapter_requires_api_key(self) -> None:
        settings = replace(Settings.from_env(project_root=PROJECT_ROOT), bea_api_key="")
        adapter = BeaSourceAdapter(settings)
        definition = get_source_definition(MANIFEST_PATH, "bea")
        result = adapter.fetch_latest(definition)
        self.assertEqual(result.status, "needs_api_key")


if __name__ == "__main__":
    unittest.main()
