"""Tests for New York Fed adapters and normalizers."""

from pathlib import Path
import unittest

from consumer_dashboard.sources.nyfed import (
    _extract_household_debt_release_metadata,
    _extract_table_from_rows,
)
from consumer_dashboard.storage.filesystem import read_json
from consumer_dashboard.transform.normalize_nyfed import normalize_nyfed_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class NewYorkFedTests(unittest.TestCase):
    def test_extract_household_debt_release_metadata(self) -> None:
        page = """
        <html><body>
        <h3>Household Debt and Credit</h3>
        <p>February 10, 2026</p>
        <a href="/medialibrary/interactives/householdcredit/data/xls/hhd_c_report_2025q4.xlsx">
            Data
        </a>
        </body></html>
        """
        metadata = _extract_household_debt_release_metadata(page)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata["release_date"], "2026-02-10")
        self.assertEqual(metadata["report_period"], "2025q4")

    def test_extract_table_from_rows(self) -> None:
        rows = [
            ["Total Debt Balance and Its Composition", None, None],
            ["Trillions of $", None, None],
            [None, None, None],
            ["Quarter", "Mortgage", "Total"],
            ["25:Q3", 12.3, 17.7],
            ["25:Q4", 12.4, 17.9],
            [None, None, None],
        ]
        table = _extract_table_from_rows("Page 3 Data", rows)
        self.assertEqual(table["table_name"], "Total Debt Balance and Its Composition")
        self.assertEqual(table["unit_label"], "Trillions of $")
        self.assertEqual(table["row_count"], 2)
        self.assertEqual(table["rows"][1]["Mortgage"], 12.4)

    def test_normalize_nyfed_payload(self) -> None:
        payload = read_json(PROJECT_ROOT / "tests" / "fixtures" / "nyfed_household_debt_sample.json", default={})
        observations = normalize_nyfed_payload(payload)
        self.assertEqual(len(observations), 68)

        latest_total = [item for item in observations if item.series_id == "household_debt_total"][-1]
        self.assertEqual(latest_total.period_date, "2025-12-31")
        self.assertEqual(latest_total.value, 17.9)
        self.assertEqual(latest_total.unit, "trillions_of_dollars")

        latest_delinquency = [
            item for item in observations if item.series_id == "household_debt_90_plus_delinquent_rate"
        ][-1]
        self.assertEqual(latest_delinquency.value, 2.19)
        self.assertEqual(latest_delinquency.release_date, "2026-02-10")

        latest_serious = [
            item for item in observations if item.series_id == "new_serious_delinquent_total_rate"
        ][-1]
        self.assertEqual(latest_serious.unit, "percent")
        self.assertEqual(latest_serious.value, 21.3)


if __name__ == "__main__":
    unittest.main()
