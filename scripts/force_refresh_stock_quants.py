"""Force-refresh stock quant snapshots for a date range.

This script bypasses the Celery queue and invokes the stock quant ETL tasks
locally to extract stock.quant snapshots from Odoo, write raw parquet files,
clean them, and update the fact_stock_on_hand_snapshot table.

Usage (run from project root):
    docker-compose exec celery-worker python scripts/force_refresh_stock_quants.py \
        --start 2026-01-01 --end 2026-01-07

To refresh a single day, omit --end:
    docker-compose exec celery-worker python scripts/force_refresh_stock_quants.py \
        --start 2026-01-01
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence

from etl_tasks import (
    refresh_dimensions_incremental,
    extract_stock_quants,
    save_raw_stock_quants,
    clean_stock_quants,
    update_stock_quants_star_schema,
)

logger = logging.getLogger("force_refresh_stock_quants")
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Force-refresh stock quant snapshots")
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


def _run_stock_quants_pipeline(target_date: str) -> Dict[str, Optional[str]]:
    logger.info("Processing stock quants for %s", target_date)

    refresh_dimensions_incremental.apply(
        args=(["products", "locations", "lots"],),
        throw=True,
    ).get()

    extraction = extract_stock_quants.apply(args=(target_date,), throw=True).get()
    raw_path = save_raw_stock_quants.apply(args=(extraction,), throw=True).get()
    clean_path = clean_stock_quants.apply(args=(raw_path, target_date), throw=True).get()
    fact_path = update_stock_quants_star_schema.apply(args=(clean_path, target_date), throw=True).get()

    return {
        "target": "stock-quants",
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
    logger.info("Forcing stock quants refresh for %d day(s): %s -> %s", len(dates), dates[0], dates[-1])

    results: List[Dict[str, Optional[str]]] = []
    for date_str in dates:
        try:
            results.append(_run_stock_quants_pipeline(date_str))
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Failed processing %s: %s", date_str, exc)

    summary = {
        "start_date": start,
        "end_date": end,
        "days": len(dates),
        "results": results,
    }

    print(json.dumps(summary, indent=2))
    logger.info("Completed stock quants refresh for %d day(s)", len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
