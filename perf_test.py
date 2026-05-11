#!/usr/bin/env python
"""Performance baseline test for Phase 2 analysis."""

import time
import sys
sys.path.append('.')
from services.duckdb_connector import DuckDBManager

def run_performance_tests():
    db = DuckDBManager()
    conn = db.get_connection()
    
    print('=== QUERY PERFORMANCE BASELINE ===')
    
    # Test 1: Simple count
    start = time.time()
    result = conn.execute('SELECT COUNT(*) FROM fact_sales').fetchone()
    print(f'fact_sales total count: {result[0]:,} records, {time.time()-start:.2f}s')
    
    # Test 2: Date filter
    query = "SELECT COUNT(*), SUM(revenue) FROM fact_sales WHERE date >= DATE '2026-02-01' AND date < DATE '2026-02-07'"
    start = time.time()
    result = conn.execute(query).fetchone()
    print(f'Week filter: {result[0]:,} records, ${result[1]:,.0f} revenue, {time.time()-start:.2f}s')
    
    # Test 3: Product join
    query = "SELECT COUNT(*), SUM(fs.revenue) FROM fact_sales fs JOIN dim_products dp ON fs.product_id = dp.product_id WHERE fs.date >= DATE '2026-02-01'"
    start = time.time()
    result = conn.execute(query).fetchone()
    print(f'Product join: {result[0]:,} records, ${result[1]:,.0f} revenue, {time.time()-start:.2f}s')
    
    # Test 4: Aggregate query
    query = "SELECT DATE_TRUNC('month', date) as month, SUM(revenue), COUNT(*) FROM fact_sales WHERE date >= DATE '2026-01-01' GROUP BY DATE_TRUNC('month', date)"
    start = time.time()
    result = conn.execute(query).fetchall()
    print(f'Monthly aggregate: {len(result)} months, {time.time()-start:.2f}s')
    
    # Test 5: Complex join with filters
    query = """
        SELECT dp.product_category, SUM(fs.revenue), COUNT(DISTINCT fs.order_id)
        FROM fact_sales fs
        JOIN dim_products dp ON fs.product_id = dp.product_id
        WHERE fs.date >= DATE '2026-02-01' AND fs.revenue > 0
        GROUP BY dp.product_category
        ORDER BY SUM(fs.revenue) DESC
    """
    start = time.time()
    result = conn.execute(query).fetchall()
    print(f'Complex category query: {len(result)} categories, {time.time()-start:.2f}s')
    
    print()
    print('=== MATERIALIZED VIEW PERFORMANCE ===')
    
    # Compare MV vs source
    query1 = "SELECT * FROM mv_sales_daily WHERE date >= DATE '2026-02-01' LIMIT 100"
    start = time.time()
    result = conn.execute(query1).fetchall()
    mv_time = time.time() - start
    print(f'MV query: {len(result)} rows, {mv_time:.3f}s')
    
    query2 = """
        SELECT CAST(date AS DATE) as date, SUM(revenue) as daily_revenue, COUNT(*) as transactions
        FROM fact_sales 
        WHERE date >= DATE '2026-02-01'
        GROUP BY CAST(date AS DATE)
        LIMIT 100
    """
    start = time.time()
    result = conn.execute(query2).fetchall()
    source_time = time.time() - start
    print(f'Source query: {len(result)} rows, {source_time:.3f}s')
    
    if mv_time > 0 and source_time > 0:
        speedup = source_time / mv_time
        print(f'MV speedup: {speedup:.1f}x')
    
    print()
    print('=== PARTITION PRUNING TEST ===')
    
    # Single day partition test
    query = "SELECT COUNT(*), SUM(revenue) FROM fact_sales WHERE date >= DATE '2026-02-15' AND date < DATE '2026-02-16'"
    start = time.time()
    result = conn.execute(query).fetchone()
    print(f'Single day partition: {result[0]:,} records, ${result[1]:,.0f} revenue, {time.time()-start:.3f}s')
    
    # Full scan comparison
    query = "SELECT COUNT(*), SUM(revenue) FROM fact_sales WHERE date >= DATE '2026-02-01' AND date < DATE '2026-02-28'"
    start = time.time()
    result = conn.execute(query).fetchone()
    print(f'Full month scan: {result[0]:,} records, ${result[1]:,.0f} revenue, {time.time()-start:.3f}s')
    
    print()
    print('=== PERFORMANCE BASELINE COMPLETE ===')

if __name__ == '__main__':
    run_performance_tests()
