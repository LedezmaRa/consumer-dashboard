"""Tests for manifest loading and source grouping."""

from pathlib import Path
import unittest

from consumer_dashboard.config.registry import list_automatable_sources, load_source_definitions


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = PROJECT_ROOT / "consumer_reports_manifest.csv"


class RegistryTests(unittest.TestCase):
    def test_load_source_definitions_groups_reports(self) -> None:
        sources = load_source_definitions(MANIFEST_PATH)
        self.assertIn("bea", sources)
        self.assertIn("bls", sources)
        self.assertIn("derived", sources)
        self.assertGreaterEqual(len(sources["bea"].report_slugs), 2)

    def test_automatable_sources_exclude_derived(self) -> None:
        sources = list_automatable_sources(MANIFEST_PATH)
        source_ids = {source.source_id for source in sources}
        self.assertIn("bea", source_ids)
        self.assertIn("federal_reserve_board", source_ids)
        self.assertNotIn("derived", source_ids)


if __name__ == "__main__":
    unittest.main()

