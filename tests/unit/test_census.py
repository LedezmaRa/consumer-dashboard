"""Tests for the Census adapter and normalizer."""

from pathlib import Path
import unittest

from consumer_dashboard.sources.census import CENSUS_DATASETS, build_census_query
from consumer_dashboard.storage.filesystem import read_json
from consumer_dashboard.transform.normalize_census import normalize_census_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class CensusTests(unittest.TestCase):
    def test_build_census_query_sets_time_and_fields(self) -> None:
        query = build_census_query("mrts", "from 2021")
        self.assertIn("cell_value", query["get"])
        self.assertEqual(query["time"], "from 2021")

    def test_normalize_census_payload_extracts_expected_series(self) -> None:
        payloads = [
            read_json(PROJECT_ROOT / "tests" / "fixtures" / "census_mrts_sample.json", default={}),
            read_json(PROJECT_ROOT / "tests" / "fixtures" / "census_resconst_sample.json", default={}),
            read_json(PROJECT_ROOT / "tests" / "fixtures" / "census_ressales_sample.json", default={}),
        ]
        observations = []
        for payload in payloads:
            observations.extend(normalize_census_payload(payload))
        series_ids = [observation.series_id for observation in observations]
        self.assertEqual(
            series_ids,
            [
                "retail_sales",
                "building_permits",
                "housing_starts",
                "new_home_sales",
            ],
        )
        self.assertTrue(all(observation.source == "census" for observation in observations))


if __name__ == "__main__":
    unittest.main()
