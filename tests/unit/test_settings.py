"""Tests for environment-backed settings."""

from pathlib import Path
import unittest

from consumer_dashboard.config.settings import Settings


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class SettingsTests(unittest.TestCase):
    def test_from_env_uses_project_root_defaults(self) -> None:
        settings = Settings.from_env(project_root=PROJECT_ROOT)
        self.assertEqual(settings.project_root, PROJECT_ROOT)
        self.assertEqual(settings.manifest_path, PROJECT_ROOT / "consumer_reports_manifest.csv")
        self.assertEqual(settings.raw_dir, PROJECT_ROOT / "data" / "raw")


if __name__ == "__main__":
    unittest.main()

