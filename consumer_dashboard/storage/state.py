"""Read and write local pipeline state."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from consumer_dashboard.storage.filesystem import read_json, write_json


class StateStore:
    """Simple JSON-backed state store."""

    def __init__(self, state_dir: Path) -> None:
        self.state_dir = state_dir
        self.path = state_dir / "pipeline_state.json"

    def load(self) -> Dict[str, object]:
        return read_json(self.path, default={"sources": {}})

    def update_source(self, source_id: str, status: str, message: str) -> Dict[str, object]:
        state = self.load()
        sources = state.setdefault("sources", {})
        sources[source_id] = {
            "status": status,
            "message": message,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        write_json(self.path, state)
        return state

