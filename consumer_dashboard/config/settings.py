"""Project settings derived from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional outside the project venv
    def load_dotenv(*args, **kwargs) -> bool:
        return False


def _split_csv(value: str) -> Tuple[str, ...]:
    items = [item.strip() for item in value.split(",") if item.strip()]
    return tuple(items)


@dataclass(frozen=True)
class Settings:
    """Paths and runtime switches used across the pipeline."""

    project_root: Path
    manifest_path: Path
    data_dir: Path
    raw_dir: Path
    processed_dir: Path
    state_dir: Path
    http_timeout_seconds: int = 30
    http_retry_attempts: int = 3
    bea_api_key: str = ""
    bls_api_key: str = ""
    anthropic_api_key: str = ""
    enabled_sources: Tuple[str, ...] = ()

    @classmethod
    def from_env(cls, project_root: Path | None = None) -> "Settings":
        root = Path(
            os.environ.get("PROJECT_ROOT", str(project_root or Path.cwd()))
        ).expanduser().resolve()
        load_dotenv(root / ".env", override=True)
        data_dir = Path(os.environ.get("DATA_DIR", str(root / "data"))).expanduser().resolve()
        manifest_path = Path(
            os.environ.get("CONSUMER_MANIFEST_PATH", str(root / "consumer_reports_manifest.csv"))
        ).expanduser().resolve()

        enabled = os.environ.get("ENABLED_SOURCES", "")
        return cls(
            project_root=root,
            manifest_path=manifest_path,
            data_dir=data_dir,
            raw_dir=data_dir / "raw",
            processed_dir=data_dir / "processed",
            state_dir=data_dir / "state",
            http_timeout_seconds=int(os.environ.get("HTTP_TIMEOUT_SECONDS", "30")),
            http_retry_attempts=int(os.environ.get("HTTP_RETRY_ATTEMPTS", "3")),
            bea_api_key=os.environ.get("BEA_API_KEY", ""),
            bls_api_key=os.environ.get("BLS_API_KEY", ""),
            anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
            enabled_sources=_split_csv(enabled),
        )
