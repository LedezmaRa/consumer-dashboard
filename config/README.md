# Config Notes

This folder is reserved for project-level configuration that should be versioned with the codebase.

Current conventions:

- Secrets stay in `.env`
- Source inventory lives in `consumer_reports_manifest.csv` at the project root
- Generated pipeline state should never live here

The `sources.example.yaml` file is a future-facing template for non-secret source settings such as retries, enablement, and release heuristics.

