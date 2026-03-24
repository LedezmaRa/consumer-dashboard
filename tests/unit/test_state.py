"""Tests for the JSON-backed state store."""

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from consumer_dashboard.storage.state import StateStore


class StateStoreTests(unittest.TestCase):
    def test_update_source_persists_json_state(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store = StateStore(Path(temp_dir))
            state = store.update_source("bea", "stub", "Adapter scaffolded.")
            self.assertIn("bea", state["sources"])
            self.assertEqual(state["sources"]["bea"]["status"], "stub")


if __name__ == "__main__":
    unittest.main()

