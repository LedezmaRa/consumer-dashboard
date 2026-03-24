"""Tests for the derive pipeline."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from consumer_dashboard.config.settings import Settings
from consumer_dashboard.pipeline.derive import derive_metrics
from consumer_dashboard.storage.filesystem import ensure_directory, read_json, write_json


def _monthly_observation(series_id: str, period_date: str, value: float, source: str, report: str) -> dict[str, object]:
    return {
        "series_id": series_id,
        "period_date": period_date,
        "value": value,
        "frequency": "monthly",
        "unit": "index",
        "source": source,
        "report": report,
        "release_date": "2026-03-23T19:00:00Z",
    }


class DerivePipelineTests(unittest.TestCase):
    def test_derive_metrics_writes_observations_and_status(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            processed_dir = ensure_directory(root / "data" / "processed")

            bls_observations = []
            bea_observations = []
            census_observations = []
            for index in range(13):
                year = 2025 if index < 12 else 2026
                month = index + 1 if index < 12 else 1
                period_date = f"{year:04d}-{month:02d}-01"
                bls_observations.extend(
                    [
                        _monthly_observation("cpi_headline", period_date, 100 * (1.01**index), "bls", "consumer_price_index"),
                        _monthly_observation("cpi_core", period_date, 100 * (1.009**index), "bls", "consumer_price_index"),
                        _monthly_observation(
                            "average_hourly_earnings",
                            period_date,
                            30 * (1.012**index),
                            "bls",
                            "employment_situation",
                        ),
                    ]
                )
                bea_observations.extend(
                    [
                        _monthly_observation(
                            "pce_price_index",
                            period_date,
                            100 * (1.002**index),
                            "bea",
                            "personal_income_outlays",
                        ),
                        _monthly_observation(
                            "core_pce_price_index",
                            period_date,
                            100 * (1.0025**index),
                            "bea",
                            "personal_income_outlays",
                        ),
                        _monthly_observation(
                            "personal_consumption_expenditures",
                            period_date,
                            19000 * (1.006**index),
                            "bea",
                            "personal_income_outlays",
                        ),
                        _monthly_observation(
                            "real_disposable_personal_income",
                            period_date,
                            16000 * (1.003**index),
                            "bea",
                            "personal_income_outlays",
                        ),
                    ]
                )
                census_observations.append(
                    _monthly_observation("retail_sales", period_date, 700 * (1.007**index), "census", "retail_trade")
                )

            write_json(
                processed_dir / "bls_observations.json",
                {"source_id": "bls", "observations": bls_observations},
            )
            write_json(
                processed_dir / "bea_observations.json",
                {"source_id": "bea", "observations": bea_observations},
            )
            write_json(
                processed_dir / "census_observations.json",
                {"source_id": "census", "observations": census_observations},
            )

            settings = Settings(
                project_root=root,
                manifest_path=root / "consumer_reports_manifest.csv",
                data_dir=root / "data",
                raw_dir=root / "data" / "raw",
                processed_dir=root / "data" / "processed",
                state_dir=root / "data" / "state",
            )
            result = derive_metrics(settings)

            self.assertEqual(result["status"], "derived")
            self.assertGreater(result["observation_count"], 0)
            self.assertIn("cpi_headline_yoy_pct", result["derived_series"])
            self.assertIn("real_personal_spending", result["derived_series"])

            output = read_json(settings.processed_dir / "derived_observations.json", default={})
            self.assertEqual(output["source_id"], "derived")
            self.assertGreater(output["series_count"], 0)

            series_ids = {item["series_id"] for item in output["observations"]}
            self.assertIn("cpi_headline_yoy_pct", series_ids)
            self.assertIn("cpi_core_3m_annualized_pct", series_ids)
            self.assertIn("real_wage_growth", series_ids)
            self.assertIn("real_disposable_personal_income_yoy_pct", series_ids)
            self.assertIn("real_retail_sales_proxy_yoy_pct", series_ids)


if __name__ == "__main__":
    unittest.main()
