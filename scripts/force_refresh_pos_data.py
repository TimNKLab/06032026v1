"""Force-refresh raw, clean, and fact data for POS and invoice sales.

This script bypasses the Celery queue and invokes the ETL tasks locally to
re-extract POS orders or invoice sales from Odoo, write raw parquet files,
clean them, and update the corresponding fact tables. All outputs land in the
configured DATA_LAKE_ROOT (e.g. D:\data-lake).

Usage (run from project root):
    docker-compose exec celery-worker python scripts/force_refresh_pos_data.py \
        --start 2025-02-01 --end 2025-02-28 --targets pos invoice-sales

To refresh only POS data for a day:
    docker-compose exec celery-worker python scripts/force_refresh_pos_data.py \
        --start 2025-02-10 --targets pos
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence, Set

from etl_tasks import (
    clean_pos_data,
    clean_sales_invoice_lines,
    extract_pos_order_lines,
    extract_sales_invoice_lines,
    save_raw_data,
    save_raw_sales_invoice_lines,
    update_star_schema,
    update_invoice_sales_star_schema,
)

logger = logging.getLogger("force_refresh_pos_data")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


TARGET_CHOICES = ["pos", "invoice-sales"]


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Force-refresh raw/clean/fact POS or invoice sales data")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD). Defaults to start date")
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=TARGET_CHOICES,
        default=["pos"],
        help="Which pipelines to run (default: pos)",
    )
    return parser.parse_args(argv)


def _date_range(start: str, end: str) -> List[str]:
    start_dt = datetime.strptime(start, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end, "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("end date must be on or after start date")

    days = (end_dt - start_dt).days
    return [(start_dt + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(days + 1)]


def _run_pos_pipeline(target_date: str) -> Dict[str, Optional[str]]:
    logger.info("Processing POS for %s", target_date)

    extraction = extract_pos_order_lines.apply(args=(target_date,), throw=True).get()
    raw_path = save_raw_data.apply(args=(extraction,), throw=True).get()
    clean_path = clean_pos_data.apply(args=(raw_path, target_date), throw=True).get()

    fact_path = update_star_schema.apply(args=(clean_path, target_date), throw=True).get()

    return {
        "target": "pos",
        "date": target_date,
        "raw_path": raw_path,
        "clean_path": clean_path,
        "fact_path": fact_path,
        "records": extraction.get("count", 0),
    }


def _run_invoice_pipeline(target_date: str) -> Dict[str, Optional[str]]:
    logger.info("Processing invoice sales for %s", target_date)

    extraction = extract_sales_invoice_lines.apply(args=(target_date,), throw=True).get()
    raw_path = save_raw_sales_invoice_lines.apply(args=(extraction,), throw=True).get()
    clean_path = clean_sales_invoice_lines.apply(
        args=(raw_path, target_date), throw=True
    ).get()

    fact_path = update_invoice_sales_star_schema.apply(args=(clean_path, target_date), throw=True).get()

    return {
        "target": "invoice-sales",
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

    target_set: Set[str] = set(args.targets)
    results: List[Dict[str, Optional[str]]] = []

    def _run_for_targets(date_str: str):
        if "pos" in target_set:
            results.append(_run_pos_pipeline(date_str))
        if "invoice-sales" in target_set:
            results.append(_run_invoice_pipeline(date_str))

    for date_str in dates:
        try:
            _run_for_targets(date_str)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Failed processing %s: %s", date_str, exc)

    summary = {
        "start_date": start,
        "end_date": end,
        "targets": sorted(target_set),
        "days": len(dates),
        "results": results,
    }

    print(json.dumps(summary, indent=2))
    logger.info("Completed refresh for %d day(s)", len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
