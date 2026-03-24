"""Tests for the DOL adapter and normalizer."""

from pathlib import Path
import unittest

from consumer_dashboard.sources.dol import _extract_latest_claims_entry
from consumer_dashboard.storage.filesystem import read_json
from consumer_dashboard.transform.normalize_dol import normalize_dol_payload


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class DolTests(unittest.TestCase):
    def test_extract_latest_claims_entry_uses_first_release(self) -> None:
        html = """
        <div class="image-left-teaser">
          <div class="left-teaser-text">
            <p class="dol-date-text">March 19, 2026</p>
            <a href=" /newsroom/releases/eta/eta20260319 "><h3><span>Unemployment Insurance Weekly Claims Report</span></h3></a>
            <div class="field field--name-field-press-body field--type-text-with-summary field--label-hidden clearfix"><p>In the week ending March 14, the advance figure for seasonally adjusted initial claims was 205,000. The 4-week moving average was 210,750.</p></div>
          </div>
        </div>
        <div class="image-left-teaser">
          <div class="left-teaser-text">
            <p class="dol-date-text">March 12, 2026</p>
            <a href=" /newsroom/releases/eta/eta20260312 "><h3><span>Unemployment Insurance Weekly Claims Report</span></h3></a>
            <div class="field field--name-field-press-body field--type-text-with-summary field--label-hidden clearfix"><p>In the week ending March 7, the advance figure for seasonally adjusted initial claims was 213,000. The 4-week moving average was 212,000.</p></div>
          </div>
        </div>
        """
        entry = _extract_latest_claims_entry(html)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["release_date"], "2026-03-19")
        self.assertEqual(entry["release_path"], "/newsroom/releases/eta/eta20260319")
        self.assertIn("week ending March 14", entry["summary_text"])

    def test_normalize_dol_payload_extracts_weekly_claims(self) -> None:
        payload = read_json(PROJECT_ROOT / "tests" / "fixtures" / "dol_weekly_claims_sample.json", default={})
        observations = normalize_dol_payload(payload)
        self.assertEqual([item.series_id for item in observations], [
            "initial_jobless_claims",
            "initial_jobless_claims_4_week_average",
        ])
        self.assertEqual(observations[0].period_date, "2026-03-14")
        self.assertEqual(observations[0].value, 243000.0)
        self.assertEqual(observations[1].value, 236500.0)


if __name__ == "__main__":
    unittest.main()
