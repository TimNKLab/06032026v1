"""Force-refresh raw, clean, and fact POS data for a date range.

This script bypasses the Celery queue and invokes the ETL tasks locally to
re-extract POS orders from Odoo, write raw parquet files, clean them, and update
fact_sales. All outputs land in the configured DATA_LAKE_ROOT (e.g. D:\data-lake).

Usage (run from project root):
    docker-compose exec celery-worker python scripts/force_refresh_pos_data.py \
        --start 2025-02-01 --end 2025-02-28

To refresh a single day, omit --end:
    docker-compose exec celery-worker python scripts/force_refresh_pos_data.py \
        --start 2025-02-10
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence

from etl_tasks import (
    clean_pos_data,
    extract_pos_order_lines,
    save_raw_data,
    update_star_schema,
)

logger = logging.getLogger("force_refresh_pos_data")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Force-refresh raw/clean/fact POS data")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD). Defaults to start date")
    return parser.parse_args(argv)


def _date_range(start: str, end: str) -> List[str]:
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("end date must be on or after start date")

    days = (end_dt - start_dt).days
    return [(start_dt + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days + 1)]


def _run_pipeline_for_date(target_date: str) -> Dict[str, Optional[str]]:
    logger.info("Processing %s", target_date)

    extraction = extract_pos_order_lines.apply(args=(target_date,), throw=True).get()
    raw_path = save_raw_data.apply(args=(extraction,), throw=True).get()
    clean_path = clean_pos_data.apply(args=(raw_path, target_date), throw=True).get()
    fact_path = update_star_schema.apply(args=(clean_path, target_date), throw=True).get()

    return {
        "date": target_date,
        "raw_path": raw_path,
        "clean_path": clean_path,
        "fact_path": fact_path,
        "records": extraction.get("count", 0),
    }


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    start = args.start
    end = args.end or args.start

    dates = _date_range(start, end)
    logger.info("Forcing refresh for %d day(s): %s -> %s", len(dates), dates[0], dates[-1])

    results: List[Dict[str, Optional[str]]] = []
    for date_str in dates:
        try:
            results.append(_run_pipeline_for_date(date_str))
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Failed processing %s: %s", date_str, exc)

    summary = {
        "start_date": start,
        "end_date": end,
        "days": len(dates),
        "results": results,
    }

    print(json.dumps(summary, indent=2))
    logger.info("Completed refresh for %d day(s)", len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
