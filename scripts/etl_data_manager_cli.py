#!/usr/bin/env python3
"""
ETL Data Manager CLI - Headless entrypoint for Docker execution.

This CLI runs inside Docker containers to perform ETL operations
(backfill, aggregate building, MV refresh, profit validation)
without Windows file locking/permission issues.

Usage (inside Docker):
    python scripts/etl_data_manager_cli.py refresh-mvs-cascading \
        --views mv_profit_daily,mv_sales_daily --start 2026-03-08 --end 2026-04-23 --auto-fetch

    python scripts/etl_data_manager_cli.py build-aggregates \
        --types sales_aggregates,profit_aggregates --start 2026-03-08 --end 2026-04-23

    python scripts/etl_data_manager_cli.py validate-profit \
        --start 2026-03-08 --end 2026-04-23
"""

import argparse
import json
import sys
from datetime import date
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.etl_data_manager import BackfillRunner


def _date(s: str) -> date:
    return date.fromisoformat(s)


def main() -> int:
    p = argparse.ArgumentParser(
        prog="etl_data_manager_cli",
        description="ETL Data Manager CLI for Docker execution"
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # refresh-mvs-cascading subcommand
    mv = sub.add_parser("refresh-mvs-cascading", help="Refresh materialized views with auto-fetch")
    mv.add_argument("--views", required=True, help="Comma-separated mv_* names (e.g., mv_profit_daily,mv_sales_daily)")
    mv.add_argument("--start", required=True, help="Start date (ISO format: YYYY-MM-DD)")
    mv.add_argument("--end", required=True, help="End date (ISO format: YYYY-MM-DD)")
    mv.add_argument("--auto-fetch", action="store_true", help="Auto-fetch missing raw data from Odoo")

    # build-aggregates subcommand
    ag = sub.add_parser("build-aggregates", help="Build aggregate tables")
    ag.add_argument("--types", required=True, help="Comma-separated: sales_aggregates,profit_aggregates")
    ag.add_argument("--start", required=True, help="Start date (ISO format: YYYY-MM-DD)")
    ag.add_argument("--end", required=True, help="End date (ISO format: YYYY-MM-DD)")

    # validate-profit subcommand
    vp = sub.add_parser("validate-profit", help="Validate profit calculations")
    vp.add_argument("--start", required=True, help="Start date (ISO format: YYYY-MM-DD)")
    vp.add_argument("--end", required=True, help="End date (ISO format: YYYY-MM-DD)")

    args = p.parse_args()

    # Initialize BackfillRunner with print for logging
    runner = BackfillRunner(log_fn=print)

    if args.cmd == "refresh-mvs-cascading":
        views = {v.strip() for v in args.views.split(",") if v.strip()}
        res = runner.refresh_materialized_views_cascading(
            views=views,
            start_date=_date(args.start),
            end_date=_date(args.end),
            auto_fetch=bool(args.auto_fetch),
            refresh_dims=False,
        )
        print("RESULT_JSON_START")
        print(json.dumps(res, default=str))
        print("RESULT_JSON_END")
        return 0

    if args.cmd == "build-aggregates":
        types = [t.strip() for t in args.types.split(",") if t.strip()]
        start = _date(args.start)
        end = _date(args.end)
        out = {"success": 0, "failed": 0, "errors": []}
        for t in types:
            r = runner.backfill_aggregates(t, start, end)
            out["success"] += r.get("success", 0)
            out["failed"] += r.get("failed", 0)
            out["errors"].extend(r.get("errors", []))
        print("RESULT_JSON_START")
        print(json.dumps(out, default=str))
        print("RESULT_JSON_END")
        return 0

    if args.cmd == "validate-profit":
        res = runner.validate_profit(_date(args.start), _date(args.end))
        print("RESULT_JSON_START")
        print(json.dumps(res, default=str))
        print("RESULT_JSON_END")
        return 0

    raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main())
