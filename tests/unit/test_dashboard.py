"""Tests for dashboard dataset and HTML builders."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from consumer_dashboard.config.settings import Settings
from consumer_dashboard.dashboard.datasets import build_dashboard_data
from consumer_dashboard.dashboard.html import build_dashboard_html
from consumer_dashboard.storage.filesystem import ensure_directory, read_json, write_json


def _observation(series_id: str, period_date: str, value: float, frequency: str, source: str) -> dict[str, object]:
    return {
        "series_id": series_id,
        "period_date": period_date,
        "value": value,
        "frequency": frequency,
        "unit": "percent",
        "source": source,
        "report": "test",
        "release_date": "2026-03-23T20:00:00Z",
    }


class DashboardTests(unittest.TestCase):
    def test_build_dashboard_data_and_html(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            processed_dir = ensure_directory(root / "data" / "processed")
            ensure_directory(root / "data" / "state")

            write_json(
                processed_dir / "bls_observations.json",
                {
                    "source_id": "bls",
                    "observations": [
                        _observation("unemployment_rate", "2026-01-01", 4.2, "monthly", "bls"),
                        _observation("unemployment_rate", "2026-02-01", 4.1, "monthly", "bls"),
                    ],
                },
            )
            write_json(
                processed_dir / "dol_observations.json",
                {
                    "source_id": "dol",
                    "observations": [
                        _observation("initial_jobless_claims_4_week_average", "2026-03-14", 225000, "weekly", "dol"),
                        _observation("initial_jobless_claims_4_week_average", "2026-03-21", 220000, "weekly", "dol"),
                    ],
                },
            )
            write_json(
                processed_dir / "new_york_fed_observations.json",
                {
                    "source_id": "new_york_fed",
                    "observations": [
                        _observation("household_debt_90_plus_delinquent_rate", "2025-09-30", 2.8, "quarterly", "new_york_fed"),
                        _observation("household_debt_90_plus_delinquent_rate", "2025-12-31", 3.1, "quarterly", "new_york_fed"),
                    ],
                },
            )
            write_json(
                processed_dir / "derived_observations.json",
                {
                    "source_id": "derived",
                    "observations": [
                        _observation("cpi_headline_yoy_pct", "2026-01-01", 2.6, "monthly", "derived"),
                        _observation("cpi_headline_yoy_pct", "2026-02-01", 2.4, "monthly", "derived"),
                        _observation("real_wage_growth", "2026-01-01", 0.9, "monthly", "derived"),
                        _observation("real_wage_growth", "2026-02-01", 1.2, "monthly", "derived"),
                        _observation("real_personal_spending_yoy_pct", "2025-12-01", 1.4, "monthly", "derived"),
                        _observation("real_personal_spending_yoy_pct", "2026-01-01", 1.8, "monthly", "derived"),
                    ],
                },
            )

            settings = Settings(
                project_root=root,
                manifest_path=root / "consumer_reports_manifest.csv",
                data_dir=root / "data",
                raw_dir=root / "data" / "raw",
                processed_dir=root / "data" / "processed",
                state_dir=root / "data" / "state",
            )

            payload = build_dashboard_data(settings)
            self.assertIn("executive_snapshot", payload)
            self.assertGreater(len(payload["executive_snapshot"]["cards"]), 0)
            self.assertGreater(len(payload["report_library"]), 0)
            self.assertIn("headline", payload["executive_snapshot"])

            status = build_dashboard_html(settings)
            self.assertEqual(status["status"], "built")

            data_output = read_json(processed_dir / "dashboard_data.json", default={})
            self.assertIn("sections", data_output)
            self.assertIn("investor_guide", data_output)
            self.assertIn("report_library", data_output)
            self.assertGreater(len(data_output["investor_guide"]["playbooks"]), 0)

            html = (processed_dir / "consumer_dashboard.html").read_text(encoding="utf-8")
            self.assertIn("The U.S. Consumer Workbench", html)
            self.assertIn("What matters right now", html)
            self.assertIn("How Economic Turns Usually Travel Into Markets", html)
            self.assertIn("Unemployment", html)
            self.assertIn("Report Library", html)


if __name__ == "__main__":
    unittest.main()
