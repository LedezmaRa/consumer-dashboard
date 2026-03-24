"""Tests for Federal Reserve Board adapters and normalizers."""

from pathlib import Path
import unittest

from consumer_dashboard.sources.fed import _extract_last_update, _extract_z1_release_metadata
from consumer_dashboard.storage.filesystem import read_json
from consumer_dashboard.transform.normalize_fed import normalize_fed_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class FederalReserveBoardTests(unittest.TestCase):
    def test_extract_z1_release_metadata(self) -> None:
        page = """
        <html><body>
        <p>Release Date: March 19, 2026 (2025:Q4 Release)</p>
        <a href="/releases/z1/20260319/z1_csv_files.zip">CSV</a>
        </body></html>
        """
        metadata = _extract_z1_release_metadata(page)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata["release_date"], "2026-03-19")
        self.assertEqual(metadata["csv_path"], "/releases/z1/20260319/z1_csv_files.zip")

    def test_extract_last_update(self) -> None:
        page = "<html><body><p>Last Update: March 06, 2026</p></body></html>"
        self.assertEqual(_extract_last_update(page), "2026-03-06")

    def test_normalize_g19_payload(self) -> None:
        payload = read_json(PROJECT_ROOT / "tests" / "fixtures" / "fed_g19_sample.json", default={})
        observations = normalize_fed_payload(payload)
        self.assertEqual(len(observations), 6)
        latest_total = [item for item in observations if item.series_id == "consumer_credit_total"][-1]
        self.assertEqual(latest_total.period_date, "2026-02-01")
        self.assertEqual(latest_total.value, 5032.2)
        self.assertEqual(latest_total.release_date, "2026-03-06")

    def test_normalize_z1_payload(self) -> None:
        payload = read_json(PROJECT_ROOT / "tests" / "fixtures" / "fed_z1_sample.json", default={})
        observations = normalize_fed_payload(payload)
        self.assertEqual(len(observations), 10)
        latest_net_worth = [item for item in observations if item.series_id == "household_net_worth"][-1]
        self.assertEqual(latest_net_worth.period_date, "2025-12-31")
        self.assertEqual(latest_net_worth.value, 186823600.0)
        self.assertEqual(latest_net_worth.release_date, "2026-03-19")


if __name__ == "__main__":
    unittest.main()
