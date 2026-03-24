"""Command-line entry points for the consumer dashboard project."""

from __future__ import annotations

import argparse
from typing import Optional, Sequence

from consumer_dashboard.config.settings import Settings
from consumer_dashboard.dashboard.datasets import build_dashboard_data
from consumer_dashboard.dashboard.html import build_dashboard_html
from consumer_dashboard.pipeline.backfill import backfill_source
from consumer_dashboard.pipeline.derive import derive_metrics
from consumer_dashboard.pipeline.ingest import ingest_source
from consumer_dashboard.pipeline.normalize import normalize_source
from consumer_dashboard.pipeline.refresh import refresh_pipeline
from consumer_dashboard.reporting.memo import generate_monthly_memo
from consumer_dashboard.reporting.status import render_status_report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="consumer-dashboard")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest_parser = subparsers.add_parser("ingest", help="Fetch source artifacts.")
    ingest_parser.add_argument("--source", required=True, help="Internal source id, for example 'bea'.")

    normalize_parser = subparsers.add_parser("normalize", help="Normalize a source into canonical observations.")
    normalize_parser.add_argument("--source", required=True, help="Internal source id, for example 'bea'.")

    subparsers.add_parser("derive", help="Compute derived metrics.")
    subparsers.add_parser("refresh", help="Run ingest, normalize, and derive for enabled sources.")
    subparsers.add_parser("status", help="Show source readiness and pipeline state.")

    backfill_parser = subparsers.add_parser("backfill", help="Backfill a source over a date range.")
    backfill_parser.add_argument("--source", required=True, help="Internal source id, for example 'bea'.")
    backfill_parser.add_argument("--start", required=True, help="Start period, for example 2018-01.")
    backfill_parser.add_argument("--end", required=True, help="End period, for example 2026-03.")

    dashboard_parser = subparsers.add_parser("dashboard", help="Dashboard dataset helpers.")
    dashboard_subparsers = dashboard_parser.add_subparsers(dest="dashboard_command", required=True)
    dashboard_subparsers.add_parser("build-data", help="Build dashboard-ready datasets from processed outputs.")
    dashboard_subparsers.add_parser("build-html", help="Build a static HTML dashboard from processed outputs.")

    memo_parser = subparsers.add_parser("memo", help="Narrative output helpers.")
    memo_subparsers = memo_parser.add_subparsers(dest="memo_command", required=True)
    memo_subparsers.add_parser("monthly", help="Generate a placeholder monthly memo from current pipeline state.")

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env()

    if args.command == "ingest":
        result = ingest_source(args.source, settings)
        print(result["message"])
        return 0

    if args.command == "normalize":
        result = normalize_source(args.source, settings)
        print(result["message"])
        return 0

    if args.command == "derive":
        result = derive_metrics(settings)
        print(result["message"])
        return 0

    if args.command == "refresh":
        result = refresh_pipeline(settings)
        print(result["message"])
        return 0

    if args.command == "status":
        print(render_status_report(settings))
        return 0

    if args.command == "backfill":
        result = backfill_source(args.source, args.start, args.end, settings)
        print(result["message"])
        return 0

    if args.command == "dashboard" and args.dashboard_command == "build-data":
        result = build_dashboard_data(settings)
        print(result["message"])
        return 0

    if args.command == "dashboard" and args.dashboard_command == "build-html":
        result = build_dashboard_html(settings)
        print(result["message"])
        return 0

    if args.command == "memo" and args.memo_command == "monthly":
        result = generate_monthly_memo(settings)
        print(result["message"])
        return 0

    parser.error("Unsupported command.")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
