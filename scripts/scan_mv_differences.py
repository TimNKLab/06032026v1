#!/usr/bin/env python3
"""
Scan for differences between available parquet data and materialized views.

This script compares:
1. Parquet files in the data lake (source of truth)
2. Materialized views in DuckDB (query acceleration layer)

Reports:
- Missing MVs (parquet exists but MV not created)
- Stale MVs (parquet has newer data than MV)
- Orphaned MVs (MV exists but parquet deleted)
- Date-by-date coverage gaps

Usage:
    python scripts/scan_mv_differences.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD]
    
Output:
    JSON or human-readable report of discrepancies
"""

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import duckdb

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from etl import config as etl_config


# =============================================================================
# Configuration
# =============================================================================

# Map materialized views to their source parquet paths
MV_TO_PARQUET_MAP = {
    "mv_sales_daily": {
        "path": etl_config.AGG_SALES_DAILY_PATH,
        "date_column": "date",
        "description": "Daily sales aggregates",
    },
    "mv_sales_by_product": {
        "path": etl_config.AGG_SALES_DAILY_BY_PRODUCT_PATH,
        "date_column": "date",
        "description": "Sales by product",
    },
    "mv_sales_by_principal": {
        "path": etl_config.AGG_SALES_DAILY_BY_PRINCIPAL_PATH,
        "date_column": "date",
        "description": "Sales by principal/brand",
    },
    "mv_profit_daily": {
        "path": etl_config.AGG_PROFIT_DAILY_PATH,
        "date_column": "date",
        "description": "Daily profit aggregates",
    },
    "mv_inventory_daily": {
        "path": etl_config.STAR_SCHEMA_PATH + "/fact_stock_on_hand_snapshot",
        "date_column": "date",
        "description": "Daily inventory snapshots",
    },
}

# Additional parquet datasets without MVs (for reporting)
PARQUET_DATASETS = {
    "fact_sales_pos": {
        "path": etl_config.CLEAN_PATH,
        "description": "POS sales fact data",
        "has_date_partitions": True,
    },
    "fact_invoice_sales": {
        "path": etl_config.CLEAN_SALES_INVOICE_PATH,
        "description": "Invoice sales fact data",
        "has_date_partitions": True,
    },
    "fact_purchases": {
        "path": etl_config.CLEAN_PURCHASES_PATH,
        "description": "Purchase invoice fact data",
        "has_date_partitions": True,
    },
    "fact_inventory_moves": {
        "path": etl_config.CLEAN_INVENTORY_MOVES_PATH,
        "description": "Inventory moves fact data",
        "has_date_partitions": True,
    },
    "fact_stock_quants": {
        "path": etl_config.CLEAN_STOCK_QUANTS_PATH,
        "description": "Stock quant snapshots",
        "has_date_partitions": True,
    },
    "fact_product_cost_events": {
        "path": etl_config.FACT_PRODUCT_COST_EVENTS_PATH,
        "description": "Product cost events",
        "has_date_partitions": True,
    },
    "fact_sales_lines_profit": {
        "path": etl_config.FACT_SALES_LINES_PROFIT_PATH,
        "description": "Sales lines with profit",
        "has_date_partitions": True,
    },
    "dim_products": {
        "path": etl_config.DIM_PRODUCTS_FILE,
        "description": "Products dimension",
        "has_date_partitions": False,
    },
}


# =============================================================================
# Scanner Class
# =============================================================================

class MVDifferenceScanner:
    """Scans for differences between parquet data and materialized views."""

    def __init__(self, db_path: Optional[str] = None):
        self.data_lake = etl_config.DATA_LAKE_ROOT
        self.db_path = db_path or f"{self.data_lake}/cache/nkdash.duckdb"
        self.conn: Optional[duckdb.DuckDBPyConnection] = None

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection."""
        if self.conn is None:
            self.conn = duckdb.connect(database=self.db_path, read_only=True)
        return self.conn

    def _close_connection(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def _get_parquet_dates(self, base_path: str, start_date: date, end_date: date) -> Set[date]:
        """Get set of dates with parquet files."""
        dates_found = set()
        
        if not os.path.exists(base_path):
            return dates_found

        # Check for hive-partitioned structure: path/year=YYYY/month=MM/day=DD/
        current = start_date
        while current <= end_date:
            year, month, day = current.strftime("%Y"), current.strftime("%m"), current.strftime("%d")
            partition_path = os.path.join(base_path, f"year={year}/month={month}/day={day}")
            
            if os.path.exists(partition_path):
                # Check for parquet files in this partition
                parquet_files = list(Path(partition_path).glob("*.parquet"))
                if parquet_files:
                    dates_found.add(current)
            
            current += timedelta(days=1)

        return dates_found

    def _get_mv_dates(self, mv_name: str, start_date: date, end_date: date) -> Set[date]:
        """Get set of dates in materialized view."""
        dates_found = set()
        
        try:
            conn = self._get_connection()
            
            # Check if MV exists
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{mv_name}'
            """).fetchone()
            
            if result[0] == 0:
                return dates_found  # MV doesn't exist
            
            # Query dates in MV within range
            rows = conn.execute(f"""
                SELECT DISTINCT date 
                FROM {mv_name} 
                WHERE date >= '{start_date}' AND date <= '{end_date}'
            """).fetchall()
            
            for row in rows:
                if row[0]:
                    dates_found.add(row[0])
                    
        except Exception as e:
            print(f"Warning: Error querying {mv_name}: {e}")
            
        return dates_found

    def _get_mv_metadata(self, mv_name: str) -> Optional[Dict]:
        """Get metadata about materialized view."""
        try:
            conn = self._get_connection()
            
            # Check if MV exists
            result = conn.execute(f"""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_name = '{mv_name}'
            """).fetchone()
            
            if result[0] == 0:
                return None  # MV doesn't exist
            
            # Get row count and max date
            result = conn.execute(f"""
                SELECT COUNT(*), MIN(date), MAX(date) FROM {mv_name}
            """).fetchone()
            
            # Get last refresh info if available
            refresh_info = None
            try:
                refresh_result = conn.execute(f"""
                    SELECT last_refresh_date, max_data_date, refresh_type
                    FROM mv_refresh_metadata
                    WHERE view_name = '{mv_name}'
                """).fetchone()
                if refresh_result:
                    refresh_info = {
                        "last_refresh_date": str(refresh_result[0]) if refresh_result[0] else None,
                        "max_data_date": str(refresh_result[1]) if refresh_result[1] else None,
                        "refresh_type": refresh_result[2],
                    }
            except Exception:
                pass  # mv_refresh_metadata table may not exist
            
            return {
                "exists": True,
                "row_count": result[0],
                "min_date": str(result[1]) if result[1] else None,
                "max_date": str(result[2]) if result[2] else None,
                "refresh_info": refresh_info,
            }
            
        except Exception as e:
            print(f"Warning: Error getting metadata for {mv_name}: {e}")
            return {"exists": False, "error": str(e)}

    def scan_mv_vs_parquet(
        self, start_date: date, end_date: date
    ) -> Dict[str, Dict]:
        """Scan all MVs and compare with their source parquet data."""
        results = {}
        
        print(f"Scanning MV differences from {start_date} to {end_date}...")
        print(f"DuckDB: {self.db_path}")
        print()
        
        for mv_name, config in MV_TO_PARQUET_MAP.items():
            print(f"Checking {mv_name}...")
            
            parquet_path = config["path"]
            description = config["description"]
            
            # Get parquet dates
            parquet_dates = self._get_parquet_dates(parquet_path, start_date, end_date)
            
            # Get MV dates
            mv_dates = self._get_mv_dates(mv_name, start_date, end_date)
            
            # Get MV metadata
            mv_metadata = self._get_mv_metadata(mv_name)
            
            # Calculate differences
            missing_in_mv = parquet_dates - mv_dates  # Parquet has, MV doesn't
            missing_in_parquet = mv_dates - parquet_dates  # MV has, parquet doesn't (orphaned)
            
            # Determine status
            if not mv_metadata or not mv_metadata.get("exists"):
                status = "MISSING_MV"
            elif not missing_in_mv and not missing_in_parquet:
                status = "SYNCED"
            elif missing_in_mv and not missing_in_parquet:
                status = "STALE_MV"
            elif missing_in_parquet and not missing_in_mv:
                status = "ORPHANED_MV"
            else:
                status = "MIXED"
            
            results[mv_name] = {
                "description": description,
                "parquet_path": parquet_path,
                "status": status,
                "parquet_dates_count": len(parquet_dates),
                "mv_dates_count": len(mv_dates),
                "missing_in_mv": sorted([d.isoformat() for d in missing_in_mv]),
                "missing_in_parquet": sorted([d.isoformat() for d in missing_in_parquet]),
                "mv_metadata": mv_metadata,
            }
        
        self._close_connection()
        return results

    def scan_parquet_datasets(
        self, start_date: date, end_date: date
    ) -> Dict[str, Dict]:
        """Scan parquet datasets for availability (no MV comparison)."""
        results = {}
        
        print(f"Scanning parquet datasets from {start_date} to {end_date}...")
        print()
        
        for dataset_name, config in PARQUET_DATASETS.items():
            path = config["path"]
            description = config["description"]
            has_partitions = config.get("has_date_partitions", True)
            
            if has_partitions:
                dates = self._get_parquet_dates(path, start_date, end_date)
                missing_dates = set()
                current = start_date
                while current <= end_date:
                    if current not in dates:
                        missing_dates.add(current)
                    current += timedelta(days=1)
                
                status = "COMPLETE" if not missing_dates else "INCOMPLETE"
                
                results[dataset_name] = {
                    "description": description,
                    "path": path,
                    "status": status,
                    "dates_available": len(dates),
                    "dates_missing": len(missing_dates),
                    "missing_dates": sorted([d.isoformat() for d in missing_dates]),
                }
            else:
                # Non-partitioned files (dimensions)
                exists = os.path.exists(path)
                results[dataset_name] = {
                    "description": description,
                    "path": path,
                    "status": "EXISTS" if exists else "MISSING",
                    "file_exists": exists,
                }
        
        return results


# =============================================================================
# Report Generation
# =============================================================================

def generate_report(
    mv_results: Dict[str, Dict],
    parquet_results: Dict[str, Dict],
    start_date: date,
    end_date: date,
    format: str = "human",
) -> str:
    """Generate formatted report."""
    
    if format == "json":
        return json.dumps(
            {
                "scan_range": {"start": start_date.isoformat(), "end": end_date.isoformat()},
                "materialized_views": mv_results,
                "parquet_datasets": parquet_results,
            },
            indent=2,
        )
    
    # Human-readable format
    lines = []
    lines.append("=" * 80)
    lines.append("MATERIALIZED VIEW vs PARQUET DIFFERENCE SCAN")
    lines.append(f"Date Range: {start_date} to {end_date}")
    lines.append("=" * 80)
    lines.append("")
    
    # Summary
    total_mvs = len(mv_results)
    synced = sum(1 for r in mv_results.values() if r["status"] == "SYNCED")
    stale = sum(1 for r in mv_results.values() if r["status"] == "STALE_MV")
    missing = sum(1 for r in mv_results.values() if r["status"] == "MISSING_MV")
    
    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Materialized Views: {total_mvs}")
    lines.append(f"  - Synced: {synced}")
    lines.append(f"  - Stale: {stale}")
    lines.append(f"  - Missing: {missing}")
    lines.append("")
    
    # Detailed MV results
    lines.append("MATERIALIZED VIEWS DETAIL")
    lines.append("-" * 40)
    
    for mv_name, result in mv_results.items():
        status = result["status"]
        status_symbol = {
            "SYNCED": "✓",
            "STALE_MV": "⚠",
            "MISSING_MV": "✗",
            "ORPHANED_MV": "?",
            "MIXED": "~",
        }.get(status, "?")
        
        lines.append(f"\n{status_symbol} {mv_name}")
        lines.append(f"   Description: {result['description']}")
        lines.append(f"   Status: {status}")
        lines.append(f"   Parquet dates: {result['parquet_dates_count']}")
        lines.append(f"   MV dates: {result['mv_dates_count']}")
        
        if result["missing_in_mv"]:
            missing_count = len(result["missing_in_mv"])
            lines.append(f"   Missing in MV: {missing_count} dates")
            if missing_count <= 10:
                for d in result["missing_in_mv"]:
                    lines.append(f"      - {d}")
            else:
                lines.append(f"      (first 10): {', '.join(result['missing_in_mv'][:10])}...")
        
        if result["missing_in_parquet"]:
            lines.append(f"   Orphaned in MV: {len(result['missing_in_parquet'])} dates")
        
        if result["mv_metadata"] and result["mv_metadata"].get("exists"):
            meta = result["mv_metadata"]
            lines.append(f"   MV rows: {meta.get('row_count', 'N/A')}")
            lines.append(f"   MV date range: {meta.get('min_date', 'N/A')} to {meta.get('max_date', 'N/A')}")
    
    lines.append("")
    lines.append("PARQUET DATASETS")
    lines.append("-" * 40)
    
    for dataset_name, result in parquet_results.items():
        status = result["status"]
        status_symbol = {"COMPLETE": "✓", "EXISTS": "✓", "INCOMPLETE": "⚠", "MISSING": "✗"}.get(status, "?")
        
        lines.append(f"\n{status_symbol} {dataset_name}")
        lines.append(f"   Description: {result['description']}")
        lines.append(f"   Status: {status}")
        
        if "dates_available" in result:
            lines.append(f"   Dates available: {result['dates_available']}")
            if result.get("dates_missing", 0) > 0:
                lines.append(f"   Dates missing: {result['dates_missing']}")
    
    lines.append("")
    lines.append("=" * 80)
    lines.append("RECOMMENDATIONS")
    lines.append("-" * 40)
    
    # Generate recommendations
    recommendations = []
    
    for mv_name, result in mv_results.items():
        if result["status"] == "MISSING_MV":
            recommendations.append(f"Create {mv_name} - run dashboard query to trigger generation")
        elif result["status"] == "STALE_MV" and result["missing_in_mv"]:
            dates = result["missing_in_mv"]
            if len(dates) <= 5:
                recommendations.append(f"Refresh {mv_name} for dates: {', '.join(dates)}")
            else:
                recommendations.append(f"Refresh {mv_name} for {len(dates)} dates")
    
    if recommendations:
        for i, rec in enumerate(recommendations, 1):
            lines.append(f"{i}. {rec}")
    else:
        lines.append("All materialized views are up to date!")
    
    lines.append("")
    lines.append("=" * 80)
    
    return "\n".join(lines)


# =============================================================================
# Main
# =============================================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Scan for differences between parquet data and materialized views"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=(date.today() - timedelta(days=30)).isoformat(),
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=date.today().isoformat(),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--format",
        choices=["human", "json"],
        default="human",
        help="Output format",
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Output file (default: stdout)",
    )
    parser.add_argument(
        "--mv-only",
        action="store_true",
        help="Only scan materialized views (skip parquet datasets)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Parse dates
    try:
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM-DD.", file=sys.stderr)
        sys.exit(1)
    
    if end_date < start_date:
        start_date, end_date = end_date, start_date
    
    # Limit range for safety
    if (end_date - start_date).days > 365:
        print("Error: Date range too large (max 365 days).", file=sys.stderr)
        sys.exit(1)
    
    # Run scan
    scanner = MVDifferenceScanner()
    
    mv_results = scanner.scan_mv_vs_parquet(start_date, end_date)
    
    parquet_results = {}
    if not args.mv_only:
        parquet_results = scanner.scan_parquet_datasets(start_date, end_date)
    
    # Generate report
    report = generate_report(
        mv_results, parquet_results, start_date, end_date, format=args.format
    )
    
    # Output
    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Report written to: {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
