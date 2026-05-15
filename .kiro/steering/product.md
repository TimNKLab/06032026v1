# Product: New Khatulistiwa KPI Dashboard

Real-time sales analytics for New Khatulistiwa retail operations.

**Source:** Odoo ERP (POS, invoices, inventory)  
**Pipeline:** ETL via Celery → Parquet data lake → DuckDB → Dash dashboard  
**Layers:** raw → clean → star-schema  
**Pages:** Overview, Sales, Sales Drilldowns, Inventory Management, Customer Experience, Data Sync  
**Design:** Cohere enterprise theme (22px radius, blue #1863dc, cool grays)  
**Schedule:** ETL daily 02:00, profit pipeline 02:20 (Jakarta timezone)