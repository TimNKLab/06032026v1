#!/usr/bin/env python3
"""
Profit ETL Performance Monitoring Script

This script monitors the performance of profit ETL tables by:
1. Counting parquet files in each dataset
2. Checking partition distribution
3. Running sample queries with timing
4. Recommending compaction if needed

Usage:
    python scripts/monitor_profit_performance.py [--days 30] [--verbose]
"""

import os
import time
import argparse
from datetime import date, timedelta
from pathlib import Path
import polars as pl
from services.duckdb_connector import get_duckdb_connection, DuckDBManager


def count_parquet_files(base_path: str, verbose: bool = False) -> dict:
    """Count parquet files and total size for a dataset."""
    base_path = Path(base_path)
    
    if not base_path.exists():
        return {'files': 0, 'size_mb': 0, 'partitions': 0}
    
    total_files = 0
    total_size = 0
    partitions = set()
    
    for root, dirs, files in os.walk(base_path):
        parquet_files = [f for f in files if f.endswith('.parquet')]
        total_files += len(parquet_files)
        
        for f in parquet_files:
            file_path = Path(root) / f
            total_size += file_path.stat().st_size
        
        # Extract partition info from path
        rel_path = Path(root).relative_to(base_path)
        if len(rel_path.parts) >= 3:
            partitions.add((rel_path.parts[0], rel_path.parts[1], rel_path.parts[2]))
    
    if verbose:
        print(f"  Files: {total_files:,}")
        print(f"  Size: {total_size / 1024 / 1024:.1f} MB")
        print(f"  Partitions: {len(partitions)}")
    
    return {
        'files': total_files,
        'size_mb': total_size / 1024 / 1024,
        'partitions': len(partitions)
    }


def run_performance_queries(days: int = 30, verbose: bool = False) -> dict:
    """Run sample queries and measure performance."""
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    conn = get_duckdb_connection()
    results = {}
    
    # Query 1: Daily profit trends (30 days)
    query1 = """
    SELECT 
        date,
        SUM(gross_profit) as gross_profit,
        SUM(revenue_tax_in) as revenue
    FROM agg_profit_daily
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
    GROUP BY date
    ORDER BY date
    """
    
    start = time.time()
    result1 = conn.execute(query1, [start_date, end_date]).fetchdf()
    results['daily_trends'] = {
        'time_seconds': time.time() - start,
        'rows': len(result1),
        'query': 'Daily profit trends'
    }
    
    # Query 2: Top products by profit
    query2 = """
    WITH product_profit AS (
        SELECT 
            product_id,
            SUM(gross_profit) as total_profit
        FROM agg_profit_daily_by_product
        WHERE date >= ? AND date < ? + INTERVAL 1 DAY
        GROUP BY product_id
        ORDER BY total_profit DESC
        LIMIT 20
    )
    SELECT COUNT(*) as top_products
    FROM product_profit
    """
    
    start = time.time()
    result2 = conn.execute(query2, [start_date, end_date]).fetchone()
    results['top_products'] = {
        'time_seconds': time.time() - start,
        'rows': result2[0] if result2 else 0,
        'query': 'Top products by profit'
    }
    
    # Query 3: Profit summary
    query3 = """
    SELECT 
        SUM(revenue_tax_in) as revenue,
        SUM(gross_profit) as gross_profit,
        SUM(transactions) as transactions
    FROM agg_profit_daily
    WHERE date >= ? AND date < ? + INTERVAL 1 DAY
    """
    
    start = time.time()
    result3 = conn.execute(query3, [start_date, end_date]).fetchone()
    results['summary'] = {
        'time_seconds': time.time() - start,
        'rows': 1,
        'query': 'Profit summary'
    }
    
    if verbose:
        for name, result in results.items():
            print(f"  {result['query']}: {result['time_seconds']:.3f}s ({result['rows']} rows)")
    
    return results


def check_partition_pruning(verbose: bool = False) -> dict:
    """Check if Hive partitioning is working by examining query plan."""
    conn = get_duckdb_connection()
    
    # Use EXPLAIN to check if partition pruning is active
    query = """
    EXPLAIN ANALYZE 
    SELECT date, SUM(gross_profit) as profit
    FROM agg_profit_daily
    WHERE date >= '2025-01-01' AND date < '2025-01-31'
    GROUP BY date
    """
    
    try:
        result = conn.execute(query).fetchdf()
        plan = result['explain_analyze'].iloc[0] if not result.empty else ""
        
        # Look for signs of partition pruning
        has_partitioning = 'hive_partitioning' in str(conn).lower() or 'PARQUET_SCAN' in plan
        has_filter = 'FILTER' in plan
        
        if verbose:
            print(f"  Partitioning enabled: {has_partitioning}")
            print(f"  Filter pushdown: {has_filter}")
        
        return {
            'partitioning_enabled': has_partitioning,
            'filter_pushdown': has_filter,
            'plan_sample': plan[:200] + "..." if len(plan) > 200 else plan
        }
    except Exception as e:
        if verbose:
            print(f"  Error checking plan: {e}")
        return {'error': str(e)}


def generate_recommendations(file_stats: dict, query_stats: dict, verbose: bool = False) -> list:
    """Generate performance recommendations based on metrics."""
    recommendations = []
    
    # Check file count
    total_files = sum(stats['files'] for stats in file_stats.values())
    if total_files > 1000:
        recommendations.append(
            f"⚠️  High file count ({total_files:,} files). Consider compaction to reduce file overhead."
        )
    elif total_files > 500:
        recommendations.append(
            f"ℹ️  Moderate file count ({total_files:,} files). Monitor as data grows."
        )
    
    # Check query performance
    avg_query_time = sum(r['time_seconds'] for r in query_stats.values()) / len(query_stats)
    if avg_query_time > 2.0:
        recommendations.append(
            f"⚠️  Slow queries (avg {avg_query_time:.2f}s). Check partitioning and file counts."
        )
    elif avg_query_time > 1.0:
        recommendations.append(
            f"ℹ️  Moderate query performance (avg {avg_query_time:.2f}s). Acceptable for 30-day ranges."
        )
    
    # Check partition distribution
    for table, stats in file_stats.items():
        if stats['partitions'] > 0 and stats['files'] > stats['partitions'] * 10:
            recommendations.append(
                f"ℹ️  {table}: Many files per partition ({stats['files']}/{stats['partitions']}). "
                f"Consider compaction."
            )
    
    if not recommendations:
        recommendations.append("✅ Performance looks good for current data volume.")
    
    return recommendations


def main():
    parser = argparse.ArgumentParser(description='Monitor profit ETL performance')
    parser.add_argument('--days', type=int, default=30, help='Number of days for query testing')
    parser.add_argument('--verbose', action='store_true', help='Verbose output')
    args = parser.parse_args()
    
    print("🔍 Profit ETL Performance Monitor")
    print(f"Query range: {args.days} days")
    print()
    
    # Get data paths
    paths = DuckDBManager._get_data_paths()
    profit_tables = {
        'Cost Events': paths[6],
        'Latest Daily Cost': paths[7], 
        'Sales Lines Profit': paths[8],
        'Profit Daily': paths[9],
        'Profit Daily by Product': paths[10]
    }
    
    # Check file statistics
    print("📊 File Statistics:")
    file_stats = {}
    for table, path in profit_tables.items():
        print(f"{table}:")
        stats = count_parquet_files(path, args.verbose)
        file_stats[table] = stats
        if not args.verbose:
            print(f"  {stats['files']:,} files, {stats['size_mb']:.1f} MB, {stats['partitions']} partitions")
        print()
    
    # Run performance queries
    print("⚡ Query Performance:")
    query_stats = run_performance_queries(args.days, args.verbose)
    if not args.verbose:
        for name, result in query_stats.items():
            print(f"  {result['query']}: {result['time_seconds']:.3f}s")
    print()
    
    # Check partitioning
    print("🔧 Partitioning Check:")
    partition_info = check_partition_pruning(args.verbose)
    if not args.verbose:
        print(f"  Partitioning enabled: {partition_info.get('partitioning_enabled', 'Unknown')}")
        print(f"  Filter pushdown: {partition_info.get('filter_pushdown', 'Unknown')}")
    print()
    
    # Generate recommendations
    print("💡 Recommendations:")
    recommendations = generate_recommendations(file_stats, query_stats, args.verbose)
    for rec in recommendations:
        print(f"  {rec}")
    print()
    
    # Summary
    total_files = sum(stats['files'] for stats in file_stats.values())
    total_size_mb = sum(stats['size_mb'] for stats in file_stats.values())
    avg_query_time = sum(r['time_seconds'] for r in query_stats.values()) / len(query_stats)
    
    print("📈 Summary:")
    print(f"  Total files: {total_files:,}")
    print(f"  Total size: {total_size_mb:.1f} MB")
    print(f"  Avg query time: {avg_query_time:.3f}s")
    print(f"  Partitions: {sum(stats['partitions'] for stats in file_stats.values())}")


if __name__ == "__main__":
    main()
