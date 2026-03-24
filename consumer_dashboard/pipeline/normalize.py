"""Normalization orchestration."""

from __future__ import annotations

from dataclasses import asdict

from consumer_dashboard.config.registry import get_source_definition
from consumer_dashboard.storage.filesystem import (
    ensure_project_directories,
    latest_directory,
    read_json,
    write_json,
)
from consumer_dashboard.storage.state import StateStore
from consumer_dashboard.transform.normalize_bea import normalize_bea_payload
from consumer_dashboard.transform.normalize_bls import normalize_bls_payload
from consumer_dashboard.transform.normalize_census import normalize_census_payload
from consumer_dashboard.transform.normalize_dol import normalize_dol_payload
from consumer_dashboard.transform.normalize_dfa import normalize_dfa_payload
from consumer_dashboard.transform.normalize_fed import normalize_fed_payload
from consumer_dashboard.transform.normalize_michigan import normalize_michigan_payload
from consumer_dashboard.transform.normalize_nyfed import normalize_nyfed_payload


def normalize_source(source_id: str, settings) -> dict:
    ensure_project_directories(settings)
    definition = get_source_definition(settings.manifest_path, source_id)
    if definition.source_id == "bea":
        run_dir = latest_directory(settings.raw_dir / "bea")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No BEA raw artifacts were found. Run ingest --source bea first.",
            }
            write_json(settings.processed_dir / "bea_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        observations = []
        raw_paths = sorted(run_dir.glob("*.json"))
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            observations.extend(normalize_bea_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "bea_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} BEA observations from "
                f"{len(raw_paths)} raw artifact(s)."
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "bea_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    if definition.source_id == "bls":
        run_dir = latest_directory(settings.raw_dir / "bls")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No BLS raw artifacts were found. Run ingest --source bls first.",
            }
            write_json(settings.processed_dir / "bls_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        raw_paths = sorted(run_dir.glob("*.json"))
        observations = []
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            observations.extend(normalize_bls_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "bls_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} BLS observations from "
                f"{len(raw_paths)} raw artifact(s)."
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "bls_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    if definition.source_id == "census":
        run_dir = latest_directory(settings.raw_dir / "census")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No Census raw artifacts were found. Run ingest --source census first.",
            }
            write_json(settings.processed_dir / "census_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        raw_paths = sorted(run_dir.glob("*.json"))
        observations = []
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            observations.extend(normalize_census_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "census_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} Census observations from "
                f"{len(raw_paths)} raw artifact(s)."
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "census_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    if definition.source_id == "dol":
        run_dir = latest_directory(settings.raw_dir / "dol")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No DOL raw artifacts were found. Run ingest --source dol first.",
            }
            write_json(settings.processed_dir / "dol_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        raw_paths = sorted(run_dir.glob("*.json"))
        observations = []
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            observations.extend(normalize_dol_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "dol_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} DOL observations from "
                f"{len(raw_paths)} raw artifact(s)."
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "dol_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    if definition.source_id == "federal_reserve_board":
        run_dir = latest_directory(settings.raw_dir / "federal_reserve_board")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No Federal Reserve Board raw artifacts were found. Run ingest --source federal_reserve_board first.",
            }
            write_json(settings.processed_dir / "federal_reserve_board_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        raw_paths = sorted(run_dir.glob("*.json"))
        observations = []
        dfa_observations = []
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            # DFA artifacts are stored in the same run directory — route by report_slug
            report_slug = envelope.get("metadata", {}).get("report_slug", "")
            if report_slug == "distributional_financial_accounts":
                dfa_observations.extend(normalize_dfa_payload(envelope))
            else:
                observations.extend(normalize_fed_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "federal_reserve_board_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        # Write DFA observations to a separate file so they do not pollute Fed Z.1 data
        if dfa_observations:
            dfa_serialized = [asdict(obs) for obs in dfa_observations]
            dfa_path = settings.processed_dir / "dfa_observations.json"
            write_json(
                dfa_path,
                {
                    "source_id": "federal_reserve_board",
                    "report_slug": "distributional_financial_accounts",
                    "raw_artifacts": [str(path) for path in raw_paths],
                    "observation_count": len(dfa_serialized),
                    "observations": dfa_serialized,
                },
            )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} Federal Reserve Board observations from "
                f"{len(raw_paths)} raw artifact(s)"
                + (f"; {len(dfa_observations)} DFA distributional observations." if dfa_observations else ".")
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "federal_reserve_board_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    if definition.source_id == "new_york_fed":
        run_dir = latest_directory(settings.raw_dir / "new_york_fed")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No New York Fed raw artifacts were found. Run ingest --source new_york_fed first.",
            }
            write_json(settings.processed_dir / "new_york_fed_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        raw_paths = sorted(run_dir.glob("*.json"))
        observations = []
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            observations.extend(normalize_nyfed_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "new_york_fed_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} New York Fed observations from "
                f"{len(raw_paths)} raw artifact(s)."
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "new_york_fed_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    if definition.source_id == "university_of_michigan":
        run_dir = latest_directory(settings.raw_dir / "university_of_michigan")
        if run_dir is None:
            payload = {
                "source_id": definition.source_id,
                "status": "missing_raw_data",
                "message": "No Michigan raw artifacts were found. Run ingest --source university_of_michigan first.",
            }
            write_json(settings.processed_dir / "university_of_michigan_normalize_status.json", payload)
            StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
            return payload

        raw_paths = sorted(run_dir.glob("*.json"))
        observations = []
        for raw_path in raw_paths:
            envelope = read_json(raw_path, default={})
            if isinstance(envelope, dict):
                envelope.setdefault("metadata", {})
                envelope["metadata"]["artifact_path"] = str(raw_path)
            observations.extend(normalize_michigan_payload(envelope))
        serialized = [asdict(observation) for observation in observations]
        observations_path = settings.processed_dir / "university_of_michigan_observations.json"
        write_json(
            observations_path,
            {
                "source_id": definition.source_id,
                "raw_artifacts": [str(path) for path in raw_paths],
                "observation_count": len(serialized),
                "observations": serialized,
            },
        )
        payload = {
            "source_id": definition.source_id,
            "status": "normalized",
            "message": (
                f"Normalized {len(serialized)} Michigan SCA observations from "
                f"{len(raw_paths)} raw artifact(s)."
            ),
            "output_path": str(observations_path),
        }
        write_json(settings.processed_dir / "university_of_michigan_normalize_status.json", payload)
        StateStore(settings.state_dir).update_source(definition.source_id, payload["status"], payload["message"])
        return payload

    payload = {
        "source_id": definition.source_id,
        "status": "stub",
        "message": (
            f"Normalization scaffold exists for '{definition.source_id}', "
            "but source-specific transforms still need to be implemented."
        ),
    }
    write_json(settings.processed_dir / f"{definition.source_id}_normalize_status.json", payload)
    StateStore(settings.state_dir).update_source(definition.source_id, "normalized_stub", payload["message"])
    return payload
