import polars as pl
import os

data_lake_root = os.environ.get('DATA_LAKE_ROOT', '/data-lake')

# Check parquet data for May 2-27
print("Checking parquet data for May 2-27...")
parquet_path = f"{data_lake_root}/star-schema/agg_sales_daily/**/*.parquet"
df_sales = pl.scan_parquet(parquet_path, hive_partitioning=True, cast_options=pl.ScanCastOptions(integer_cast='upcast')).filter(
    (pl.col("date") >= pl.lit('2026-05-02').cast(pl.Date)) & 
    (pl.col("date") <= pl.lit('2026-05-27').cast(pl.Date))
).collect()

print(f"Parquet sales data for May 2-27: {len(df_sales)} rows")
if len(df_sales) > 0:
    total_revenue = df_sales['revenue'].sum()
    total_transactions = df_sales['transactions'].sum()
    print(f"Total revenue: {total_revenue:,.2f}")
    print(f"Total transactions: {total_transactions:,}")
    print("\nDaily breakdown:")
    print(df_sales.sort('date').select(['date', 'revenue', 'transactions']))

# Check SQLite MV data
print("\nChecking SQLite MV data for May 2-27...")
import sqlite3
db_path = f"{data_lake_root}/cache/nkdash.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("""
    SELECT date, revenue, transactions 
    FROM mv_sales_daily 
    WHERE date >= '2026-05-02' AND date <= '2026-05-27'
    ORDER BY date
""")
mv_data = cursor.fetchall()

print(f"SQLite MV sales data for May 2-27: {len(mv_data)} rows")
if len(mv_data) > 0:
    mv_revenue = sum(row[1] for row in mv_data)
    mv_transactions = sum(row[2] for row in mv_data)
    print(f"Total revenue: {mv_revenue:,.2f}")
    print(f"Total transactions: {mv_transactions:,}")
    print("\nDaily breakdown:")
    for row in mv_data:
        print(f"{row[0]}: revenue={row[1]:,.2f}, transactions={row[2]:,}")

conn.close()

# Compare
if len(df_sales) > 0 and len(mv_data) > 0:
    print("\nComparison:")
    print(f"Parquet revenue: {total_revenue:,.2f}")
    print(f"MV revenue: {mv_revenue:,.2f}")
    print(f"Difference: {abs(total_revenue - mv_revenue):,.2f}")
    
    print(f"\nParquet transactions: {total_transactions:,}")
    print(f"MV transactions: {mv_transactions:,}")
    print(f"Difference: {abs(total_transactions - mv_transactions):,}")
    
    if abs(total_revenue - mv_revenue) < 0.01 and total_transactions == mv_transactions:
        print("\n✓ Data matches between parquet and MV!")
    else:
        print("\n✗ Data mismatch between parquet and MV!")
