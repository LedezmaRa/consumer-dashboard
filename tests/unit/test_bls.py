"""Tests for the BLS adapter and normalizer."""

from pathlib import Path
import unittest

from consumer_dashboard.sources.bls import BLS_SERIES_IDS, build_bls_request_payload
from consumer_dashboard.storage.filesystem import read_json
from consumer_dashboard.transform.normalize_bls import normalize_bls_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class BlsTests(unittest.TestCase):
    def test_build_bls_request_payload_sets_series_and_years(self) -> None:
        payload = build_bls_request_payload(BLS_SERIES_IDS, startyear="2025", endyear="2026")
        self.assertEqual(payload["seriesid"][0], "LNS14000000")
        self.assertEqual(payload["startyear"], "2025")
        self.assertEqual(payload["endyear"], "2026")

    def test_normalize_bls_payload_extracts_expected_series(self) -> None:
        payload = read_json(PROJECT_ROOT / "tests" / "fixtures" / "bls_core_sample.json", default={})
        observations = normalize_bls_payload(payload)
        series_ids = [observation.series_id for observation in observations]
        self.assertEqual(
            series_ids,
            [
                "jolts_job_openings",
                "jolts_quits_rate",
                "average_hourly_earnings",
                "cpi_core",
                "cpi_headline",
                "nonfarm_payrolls",
                "unemployment_rate",
            ],
        )
        self.assertTrue(all(observation.source == "bls" for observation in observations))
        self.assertEqual(observations[0].release_date, "2026-03-23T18:10:00Z")


if __name__ == "__main__":
    unittest.main()
