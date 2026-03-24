"""Tests for inflation-derived metrics."""

from datetime import date
import unittest

from consumer_dashboard.metrics.common import build_series_map
from consumer_dashboard.metrics.inflation import compute_inflation_metrics
from consumer_dashboard.models.observation import Observation


def _month_start(year: int, month: int) -> str:
    return date(year, month, 1).isoformat()


class InflationMetricTests(unittest.TestCase):
    def test_compute_inflation_metrics_generates_mom_yoy_and_annualized_series(self) -> None:
        observations = []
        for index in range(13):
            month = index + 1
            year = 2025 if month <= 12 else 2026
            month = month if month <= 12 else 1
            observations.append(
                Observation(
                    series_id="cpi_headline",
                    period_date=_month_start(year, month),
                    value=100 * (1.01**index),
                    frequency="monthly",
                    unit="index",
                    source="bls",
                    report="consumer_price_index",
                    release_date="2026-03-23T18:03:10Z",
                )
            )
            observations.append(
                Observation(
                    series_id="cpi_core",
                    period_date=_month_start(year, month),
                    value=100 * (1.009**index),
                    frequency="monthly",
                    unit="index",
                    source="bls",
                    report="consumer_price_index",
                    release_date="2026-03-23T18:03:10Z",
                )
            )

        derived = compute_inflation_metrics(build_series_map(observations))
        latest_mom = [item for item in derived if item.series_id == "cpi_headline_mom_pct"][-1]
        latest_yoy = [item for item in derived if item.series_id == "cpi_headline_yoy_pct"][-1]
        latest_annualized = [item for item in derived if item.series_id == "cpi_headline_3m_annualized_pct"][-1]

        self.assertAlmostEqual(latest_mom.value, 1.0, places=9)
        self.assertAlmostEqual(latest_yoy.value, ((1.01**12) - 1) * 100, places=9)
        self.assertAlmostEqual(latest_annualized.value, ((1.01**12) - 1) * 100, places=9)


if __name__ == "__main__":
    unittest.main()
