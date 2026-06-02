#!/usr/bin/env python3
import duckdb

conn = duckdb.connect('/data-lake/cache/nkdash.duckdb')

print("=== Materialized Views and Fact Tables ===")
result = conn.execute("""
    SELECT table_name, table_type 
    FROM information_schema.tables 
    WHERE table_name LIKE 'mv_%' OR table_name LIKE 'fact_%' 
    ORDER BY table_name
""").fetchall()

for row in result:
    print(f"{row[0]}: {row[1]}")

print(f"\nTotal: {len(result)} tables/views")
conn.close()
