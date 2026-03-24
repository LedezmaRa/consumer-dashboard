# Project Orchestration Blueprint

## Purpose

This document is the implementation blueprint for the U.S. consumer dashboard project. It translates the existing research artifacts into a buildable software system.

The goal is not to automate spreadsheets directly. The goal is to build a versioned, testable data pipeline that produces dashboard-ready datasets and narrative outputs.

## Review Of Work Already Done

The current workspace contains strong domain work and no application code yet.

### Existing assets

- `Consumer reports general information.docx`
  Defines the first-principles view of the U.S. consumer.
- `Consumer data master tracking table.xlsx`
  Converts that view into a report inventory and cadence tracker.
- `US_Consumer_Dashboard.xlsx`
  Converts the tracker into a dashboard shell.
- `consumer_dashboard_strategy.md`
  Summarizes required reports and acquisition options.
- `consumer_reports_manifest.csv`
  Provides the first machine-readable source manifest.

### What this means

The conceptual model is ahead of the implementation model. That is good. We do not need to invent the dashboard logic from scratch. We need to formalize it into:

1. a source catalog,
2. a canonical observation model,
3. a derived-metrics layer,
4. a dashboard snapshot layer,
5. a narrative layer.

## Project State

There is currently:

- no repo structure,
- no package manager configuration,
- no test suite,
- no CLI entry point,
- no downloader or normalization code,
- no dashboard application code.

Local environment observations:

- `python3` is available as `3.9.6`
- `uv` is not installed
- `pytest` is not installed

Recommendation:

- Target `Python 3.11+` for the project itself, even though the current system Python is 3.9.6.
- Build the project so it can still run locally after a project-specific environment is created.

## Architecture Principles

These rules should govern implementation:

1. Normalize everything into a canonical long-format time-series model.
2. Treat source releases as versioned artifacts, not just latest values.
3. Keep raw data, normalized data, derived metrics, and dashboard outputs separate.
4. Prefer official APIs and official downloads over page scraping.
5. Keep the MVP local-first and deterministic.
6. Make the dashboard read from processed datasets, never directly from source endpoints.
7. Separate fully automatable sources from manual or licensed sources.

## System Layers

The production system should be built in layers.

### Layer 1: Source metadata

Defines where data comes from and how it should be fetched.

Core entities:

- `sources`
- `reports`
- `series_catalog`

### Layer 2: Release ingestion

Fetches source artifacts and records publication context.

Core entities:

- `releases`
- raw files under `data/raw/...`

### Layer 3: Canonical observations

Transforms raw source payloads into standard observation rows.

Core entity:

- `observations`

Canonical fields:

- `series_id`
- `period_date`
- `value`
- `frequency`
- `unit`
- `source`
- `report`
- `release_date`
- `reference_period`
- `vintage`
- `seasonal_adjustment`

### Layer 4: Derived metrics

Computes the indicators that actually explain the consumer.

Core entity:

- `derived_observations`

### Layer 5: Dashboard outputs

Precomputes the tables used by the dashboard UI.

Core entities:

- `dashboard_snapshots`
- `regime_assessments`
- `memo_outputs`

## Proposed Module Boundaries

Use a Python package with explicit responsibility boundaries.

```text
consumer_dashboard/
  config/
  models/
  sources/
  storage/
  transform/
  metrics/
  pipeline/
  dashboard/
  reporting/
tests/
  fixtures/
  unit/
  integration/
  golden/
config/
data/
  raw/
  processed/
  state/
```

### `consumer_dashboard/config`

Responsibility:

- environment loading
- runtime settings
- path configuration
- source enable/disable flags

Key files:

- `settings.py`
- `registry.py`

### `consumer_dashboard/models`

Responsibility:

- typed models and schema definitions
- observation records
- release records
- dashboard snapshot records

Key files:

- `series.py`
- `release.py`
- `observation.py`
- `snapshot.py`

### `consumer_dashboard/sources`

Responsibility:

- source-specific acquisition logic
- request construction
- source response parsing
- source authentication if needed

Expected source adapters:

- `bea.py`
- `bls.py`
- `census.py`
- `fed.py`
- `nyfed.py`
- `dol.py`

Later adapters:

- `michigan.py`
- `conference_board.py`
- `nar.py`

Boundary rule:

- Source adapters may know source-specific payloads.
- They must not contain dashboard logic.

### `consumer_dashboard/storage`

Responsibility:

- raw artifact persistence
- processed table writes
- release-state tracking
- idempotency helpers

Key files:

- `filesystem.py`
- `catalog.py`
- `state.py`

### `consumer_dashboard/transform`

Responsibility:

- map raw source data into canonical observation rows
- keep provenance explicit

Key files:

- `normalize_bea.py`
- `normalize_bls.py`
- `normalize_census.py`
- `normalize_fed.py`
- `normalize_nyfed.py`
- `normalize_dol.py`

### `consumer_dashboard/metrics`

Responsibility:

- compute derived indicators
- build scoring logic for executive summary and regime state

Key files:

- `real_income.py`
- `real_spending.py`
- `labor.py`
- `credit.py`
- `regime.py`

### `consumer_dashboard/pipeline`

Responsibility:

- orchestration and workflow composition

Key files:

- `ingest.py`
- `normalize.py`
- `derive.py`
- `refresh.py`
- `backfill.py`

### `consumer_dashboard/dashboard`

Responsibility:

- prepare dashboard-ready tables or app-facing loaders

Key files:

- `datasets.py`
- `views.py`

### `consumer_dashboard/reporting`

Responsibility:

- memo generation
- freshness reports
- validation summaries

Key files:

- `memo.py`
- `status.py`

## Entry Points

Start with a CLI instead of a scheduler or a web app.

Recommended commands:

- `consumer-dashboard ingest --source bea`
- `consumer-dashboard ingest --source bls`
- `consumer-dashboard normalize --source bea`
- `consumer-dashboard derive`
- `consumer-dashboard refresh`
- `consumer-dashboard backfill --source census --start 2018-01 --end 2026-03`
- `consumer-dashboard status`
- `consumer-dashboard dashboard build-data`
- `consumer-dashboard memo monthly`

Command behavior:

- `refresh` should run the end-to-end pipeline for enabled sources.
- `status` should show last successful release date and dataset freshness.
- `dashboard build-data` should emit processed datasets without requiring the UI itself.

## Data Model Boundaries

The workbook should not define the storage model directly. The software model should.

### Required tables/files

- `sources`
- `reports`
- `series_catalog`
- `releases`
- `observations`
- `derived_observations`
- `dashboard_snapshots`
- `regime_assessments`
- `memo_outputs`

### Minimum derived metrics

- `real_disposable_personal_income`
- `real_personal_spending`
- `real_wage_growth`
- `savings_rate_trend`
- `credit_growth_vs_income_growth`
- `credit_card_delinquency_trend`
- `real_retail_sales_proxy`
- `labor_momentum_index`
- `consumer_stress_index`
- `wealth_support_index`
- `consumer_regime`

## Conventions

### Naming

- Use snake_case for Python modules, CLI options, and internal series ids.
- Use stable slugs for source ids and report ids.
- Keep public dashboard labels human-readable and separate from internal ids.

### Provenance

- Every observation must preserve source, release date, and artifact path.
- Derived metrics must record their input dependencies.

### Idempotency

- Re-running a fetch must not duplicate an existing release.
- Re-running normalization must be safe.
- Processed outputs should be reproducible from raw artifacts.

### Storage

- Save raw source artifacts under `data/raw/<source>/<release_date>/...`
- Save normalized outputs under `data/processed/`
- Save pipeline state under `data/state/`

### Manual overlays

- Manual or licensed data must not block the official-data pipeline.
- Use a separate ingestion path for manual uploads.

## Dependencies

### Recommended core dependencies

- `pydantic`
- `httpx`
- `pandas`
- `pyarrow`
- `typer`
- `python-dotenv`
- `tenacity`

### Optional later dependencies

- `duckdb`
- `streamlit` or `plotly-dash`

### Dependency guidance

- Avoid Airflow or Prefect in v1.
- Avoid overbuilding orchestration before the adapters are stable.
- Prefer simple local scripts and CLI flows first.

## Testing Patterns

There are no existing tests in the workspace. We should establish the test contract from the first implementation step.

### Unit tests

Test:

- request builders
- parser functions
- release detection logic
- metric formulas

### Fixture-based integration tests

Test:

- raw source payload to canonical observation rows
- normalization for each source

### Golden tests

Test:

- expected processed outputs for a fixed fixture set
- expected snapshot rows for executive, weekly, monthly, and quarterly views

### Pipeline smoke tests

Test:

- full `refresh` run against local fixtures only
- no network access during test runs

### Contract tests

Test:

- required columns
- no duplicate primary keys
- date monotonicity where expected
- valid frequencies and units

### Regression tests

Test:

- revision handling
- idempotent reruns
- manual source isolation from automated pipeline

## MVP Scope

The first implementation should cover the fully automatable sources only.

### Included in MVP

- BEA
- BLS
- Census
- Federal Reserve Board
- New York Fed
- DOL weekly claims

### Excluded from MVP

- Conference Board
- NAR existing home sales
- private card-spend datasets

### Conditional in MVP+

- University of Michigan sentiment and inflation expectations

## Known Risks

- The current workbook mixes raw and derived concepts.
- Revision tracking is not yet modeled.
- Executive summary scoring logic is not yet defined.
- Some high-value reports are not fully open or stable to automate.
- The current environment lacks the project tooling we will likely want later.

## Recommended Implementation Sequence

### Phase 1: Foundation

- create repo structure
- define config and schema models
- define source registry and canonical series ids

### Phase 2: Official-source ingestion

- implement BEA, BLS, Census, Fed, NY Fed, and DOL adapters
- save raw artifacts
- normalize to canonical observations

### Phase 3: Metrics and snapshots

- compute derived metrics
- compute executive snapshot and frequency-specific dashboard tables

### Phase 4: Dashboard UI

- build a local dashboard against processed datasets
- show freshness and source status

### Phase 5: Scheduling and automation

- add scheduled refresh
- add failure handling and status reporting

### Phase 6: Semi-manual overlays

- add Michigan, Conference Board, NAR, and private overlays as separate paths

## Orchestration Decisions

These are the decisions I recommend we treat as settled unless implementation proves otherwise:

1. Python is the right implementation language for the pipeline.
2. The canonical long-format observation model is the center of the system.
3. The dashboard should consume processed datasets, not source payloads.
4. The project should launch with official automatable sources only.
5. Manual or licensed series should be isolated operationally.

## Subagent Synthesis

Parallel architecture review confirmed two important points:

- The workbook already defines the analytical layers clearly.
- The main implementation risk is accidentally building a sheet updater instead of a time-series system.

That means the next real build step should be scaffolding the repository and codifying these boundaries before we write source adapters.

