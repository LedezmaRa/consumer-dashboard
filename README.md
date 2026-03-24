# U.S. Consumer Dashboard

This project turns the consumer-research artifacts in this folder into a versioned data pipeline and dashboard-ready dataset.

## What Exists Today

- First-principles research and dashboard design artifacts
- A machine-readable report manifest
- A Python project skeleton with package boundaries, CLI entry points, and starter tests

## Current Implementation Status

The codebase is scaffolded but not yet connected to live source APIs. The next implementation step is wiring source adapters for the official data providers:

- BEA
- BLS
- Census
- Federal Reserve Board
- New York Fed
- DOL weekly claims

## Recommended Setup

Python `3.11+` is recommended for day-to-day development, even though the initial scaffold keeps the code itself lightweight.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

If `python3.11` is not installed yet, install it first and then create the virtual environment.

## Useful Commands

```bash
consumer-dashboard status
consumer-dashboard ingest --source bea
consumer-dashboard normalize --source bea
consumer-dashboard derive
consumer-dashboard refresh
consumer-dashboard dashboard build-data
consumer-dashboard dashboard build-html
consumer-dashboard memo monthly
python -m unittest discover -s tests/unit -p 'test_*.py'
```

## Project Layout

```text
consumer_dashboard/
  config/
  dashboard/
  metrics/
  models/
  pipeline/
  reporting/
  sources/
  storage/
  transform/
config/
data/
tests/
```

## Key Planning Artifacts

- `project_orchestration_blueprint.md`
- `consumer_dashboard_strategy.md`
- `consumer_reports_manifest.csv`
