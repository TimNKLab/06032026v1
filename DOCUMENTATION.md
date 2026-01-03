# New Khatulistiwa KPI Dashboard - Complete Guide

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Running the Application](#running-the-application)
6. [ETL Process](#etl-process)
6.1. [Alur Data & Referensi ETL (Bahasa Indonesia)](#alur-data--referensi-etl-bahasa-indonesia)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)
9. [Development](#development)

## Overview

This dashboard provides real-time sales analytics for New Khatulistiwa, built with:
- **Frontend**: Plotly Dash with Dash Mantine Components
- **Backend**: Python (Polars, Celery, Redis)
- **Data Pipeline**: Odoo → Raw Parquet → Clean Parquet → Star Schema
- **Containerization**: Docker with multi-stage builds

## Prerequisites

- Docker & Docker Compose
- Python 3.9+
- Odoo API access credentials
- Redis (included in Docker setup)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/nkdash.git
   cd nkdash
   ```

2. Create and configure `.env` file:
   ```bash
   cp .env.example .env
   # Edit with your credentials
   ```

## Configuration

### Environment Variables (`.env`)
```ini
# Odoo Connection
ODOO_HOST=your-odoo-instance.odoo.com
ODOO_PORT=443
ODOO_PROTOCOL=jsonrpc+ssl
ODOO_DB=your_database
ODOO_USERNAME=your_email@example.com
ODOO_API_KEY=your_api_key

# Redis
REDIS_URL=redis://redis:6379/0

# Data Lake
# - Recommended (Docker bind mount): /data-lake
# - Legacy default in some components: /app/data-lake
# Keep this consistent across all containers.
DATA_LAKE_ROOT=/data-lake
```

### Data Lake Structure
```
data-lake/
├── raw/            # Raw data from Odoo
├── clean/          # Processed data
└── star-schema/    # Analytics-ready data
```

## Running the Application

### Development Mode
```bash
# Start all services
docker-compose up --build

# Or start specific services
docker-compose up web celery-worker redis
```

### Production Mode
```bash
docker-compose -f docker-compose.prod.yml up --build -d
```

## ETL Process

### ETL Tasks Architecture (Developer Notes)

`etl_tasks.py` is the **single entry point** ("mother" script) for ETL execution.

- It defines the Celery `app` and all task names.
- It wires **Celery Beat** schedules and **task routing** (queues).
- Other scripts (for example `etl_runner.py` and `scripts/force_refresh_*.py`) import tasks/functions directly from `etl_tasks.py`.

#### Task Groups (current)

- **Extraction**
  - `extract_pos_order_lines`
  - `extract_sales_invoice_lines`
  - `extract_purchase_invoice_lines`
  - `extract_inventory_moves`
- **Raw persistence**
  - `save_raw_data`
  - `save_raw_sales_invoice_lines`
  - `save_raw_purchase_invoice_lines`
  - `save_raw_inventory_moves`
- **Cleaning / transformation**
  - `clean_pos_data`
  - `clean_sales_invoice_lines`
  - `clean_purchase_invoice_lines`
  - `clean_inventory_moves`
- **Star-schema loading**
  - `update_star_schema`
  - `update_invoice_sales_star_schema`
  - `update_purchase_star_schema`
  - `update_inventory_moves_star_schema`
- **Dimensions**
  - `refresh_dimensions_incremental`
- **Orchestration / health**
  - `daily_etl_pipeline`, `date_range_etl_pipeline`, `catch_up_etl`, `health_check`

#### Proposed Modular Layout (planned)

We will keep `etl_tasks.py` as the entry point, but move implementation into modules and keep the task functions as thin wrappers.

Proposed structure:
```python
nkdash/
  etl_tasks.py                      # Celery app + task definitions (wrappers) + schedules/routes
  etl/
    __init__.py
    config.py                       # constants, env parsing, data-lake paths
    io_parquet.py                   # atomic parquet write/read helpers
    metadata.py                     # ETLMetadata
    dimension_cache.py              # DimensionLoader
    odoo_pool.py                    # get_pooled_odoo_connection
    odoo_helpers.py                 # safe_extract_m2o, batch_ids, extract_o2m_ids, model field helpers
    extract/
      pos.py                        # POS extraction implementation
      invoices.py                   # invoice line extraction implementation
      inventory_moves.py            # inventory extraction implementation
    transform/
      pos.py                        # clean_pos_data implementation
      invoices.py                   # invoice cleaning implementation
      inventory_moves.py            # inventory cleaning implementation
    load/
      facts.py                      # update_fact_* implementations
      dimensions.py                 # refresh_dimensions_incremental implementation
    pipelines/
      daily.py                      # daily_*_pipeline implementations
      ranges.py                     # date_range_etl_pipeline implementation
      health.py                     # catch_up_etl / health_check implementations
```

#### Backward-Compatibility Rules

- Celery task names **must remain** `etl_tasks.<task_name>` (Beat schedule and routing depend on this).
- `etl_tasks.py` will continue to export the same symbols so existing scripts keep working.
- Internal implementation can move, but wrappers in `etl_tasks.py` stay stable.

#### Adding a New ETL Dataset (new developers)

- Add an **extractor** under `etl/extract/<dataset>.py`.
- Add a **raw writer** (if needed) under `etl/io_parquet.py` or a dataset-specific helper.
- Add a **cleaner** under `etl/transform/<dataset>.py`.
- Add a **loader** under `etl/load/<dataset>.py` (or extend `etl/load/facts.py`).
- Register/Expose task wrappers in `etl_tasks.py` and add:
  - Beat schedule entry (optional)
  - Task routing queue (recommended)

### Alur Data & Referensi ETL (Bahasa Indonesia)

Bagian ini menjelaskan alur data **end-to-end** dari Odoo sampai tampil di dashboard, termasuk peran tiap **Celery task**, script yang memanggilnya, serta perintah operasional & troubleshooting.

#### 1) Alur Data Tingkat Tinggi (Odoo → Dashboard)

1. **Odoo (Sumber Data)**
   - Data diambil via Odoo RPC (lihat `odoorpc_connector.py`).
   - Contoh dataset:
     - POS: `pos.order` (order + `lines` + `payments_id`)
     - Invoice sales: `account.move` / `account.move.line` (posted `out_invoice`)
     - Purchases: `account.move` / `account.move.line` (posted `in_invoice`)
     - Inventory moves: `stock.move.line` (+ join ke `stock.move`, `stock.picking`, `stock.location`, dll.)

2. **Celery Tasks (ETL Orchestration)**
   - Semua task resmi tetap bernama `etl_tasks.<nama_task>`.
   - `etl_tasks.py` adalah entrypoint; sebagian implementasi dipindah ke paket `etl/`.

3. **Data Lake (Parquet Layers)**
   - Layer yang digunakan:
     - `raw/`: hasil ekstraksi mentah dari Odoo
     - `clean/`: hasil pembersihan/normalisasi
     - `star-schema/`: tabel fakta/dimensi siap query
   - Catatan path:
     - Lokasi root mengikuti env `DATA_LAKE_ROOT`.
     - Pada setup Docker Windows yang umum, folder host `D:\data-lake` di-mount menjadi `/data-lake` di dalam container.
     - Beberapa bagian kode/skrip masih menggunakan default lama `/app/data-lake`.
     - Rekomendasi: set `DATA_LAKE_ROOT=/data-lake` di `.env` dan gunakan value yang sama untuk semua service (web/celery-worker).

4. **DuckDB Views (Query Layer untuk Dashboard)**
   - `services/duckdb_connector.py` membuat view dari Parquet:
     - `fact_sales`, `fact_invoice_sales`, `fact_purchases`, `fact_inventory_moves`
     - `dim_products`, `dim_categories`, `dim_brands`, `dim_taxes`
     - `fact_sales_all` = gabungan POS + invoice sales

5. **Dashboard (Dash Pages)**
   - Contoh: `pages/sales.py`
     - KPI & chart memanggil `services/sales_metrics.py` dan `services/sales_charts.py`
     - Modul tersebut melakukan query ke DuckDB (fast path), lalu fallback ke Odoo bila query gagal.

#### 2) Katalog ETL Tasks (Apa fungsinya?)

Di bawah ini ringkasan tiap task utama, output file layer, dan siapa yang biasanya memanggilnya.

##### A. Extraction

- **`etl_tasks.extract_pos_order_lines(target_date)`**
  - **Fungsi**: ambil POS order lines untuk `target_date`.
  - **Implementasi**: `etl/extract/pos.py`.
  - **Dipakai oleh**:
    - `etl_tasks.daily_etl_pipeline`
    - `scripts/force_refresh_pos_data.py --targets pos`

- **`etl_tasks.extract_sales_invoice_lines(target_date)`**
  - **Fungsi**: ambil invoice lines untuk sales (posted `out_invoice`).
  - **Dipakai oleh**:
    - `etl_tasks.daily_invoice_sales_pipeline`
    - `scripts/force_refresh_pos_data.py --targets invoice-sales`

- **`etl_tasks.extract_purchase_invoice_lines(target_date)`**
  - **Fungsi**: ambil vendor bill lines (posted `in_invoice`).
  - **Dipakai oleh**:
    - `etl_tasks.daily_invoice_purchases_pipeline`
    - `scripts/force_refresh_purchase_data.py`

- **`etl_tasks.extract_inventory_moves(target_date)`**
  - **Fungsi**: ambil inventory moves (executed move lines) untuk `target_date`.
  - **Dipakai oleh**:
    - `etl_tasks.daily_inventory_moves_pipeline`
    - `scripts/force_refresh_pos_data.py --targets inventory-moves`

##### B. Raw Persistence

- **`etl_tasks.save_raw_data(extraction_result)`**
  - **Fungsi**: tulis raw POS ke layer `raw/` (dataset POS).

- **`etl_tasks.save_raw_sales_invoice_lines(extraction_result)`**
  - **Fungsi**: tulis raw invoice sales ke layer `raw/` (dataset invoice sales).

- **`etl_tasks.save_raw_purchase_invoice_lines(extraction_result)`**
  - **Fungsi**: tulis raw purchases ke layer `raw/` (dataset purchases).

- **`etl_tasks.save_raw_inventory_moves(extraction_result)`**
  - **Fungsi**: tulis raw inventory moves ke layer `raw/` (dataset inventory moves).

##### C. Cleaning / Transformation

- **`etl_tasks.clean_pos_data(raw_file_path, target_date)`**
  - **Fungsi**: validasi + normalisasi POS, output ke layer `clean/` (dataset POS).

- **`etl_tasks.clean_sales_invoice_lines(raw_file_path, target_date)`**
  - **Fungsi**: bersihkan invoice sales, output ke layer `clean/` (dataset invoice sales).

- **`etl_tasks.clean_purchase_invoice_lines(raw_file_path, target_date)`**
  - **Fungsi**: bersihkan purchases, output ke layer `clean/` (dataset purchases).

- **`etl_tasks.clean_inventory_moves(raw_file_path, target_date)`**
  - **Fungsi**: bersihkan inventory moves, output ke layer `clean/` (dataset inventory moves).

##### D. Star Schema / Loading

- **`etl_tasks.update_star_schema(clean_file_path, target_date)`**
  - **Fungsi**: update `star-schema/fact_sales/` (POS).

- **`etl_tasks.update_invoice_sales_star_schema(clean_file_path, target_date)`**
  - **Fungsi**: update `star-schema/fact_invoice_sales/`.

- **`etl_tasks.update_purchase_star_schema(clean_file_path, target_date)`**
  - **Fungsi**: update `star-schema/fact_purchases/`.

- **`etl_tasks.update_inventory_moves_star_schema(clean_file_path, target_date)`**
  - **Fungsi**: update `star-schema/fact_inventory_moves/`.

##### E. Dimensions

- **`etl_tasks.refresh_dimensions_incremental(targets=None)`**
  - **Fungsi**: build/update dimensi yang dipakai untuk enrichment (contoh: products, locations, uoms, partners, users, companies, lots).
  - **Output**: file dimensi di `star-schema/` (misalnya `dim_products.parquet`).
  - **Dipakai oleh**:
    - schedule beat `incremental-dimension-refresh`
    - pipeline inventory moves (sebelum ekstraksi)

##### F. Orchestration / Pipeline

- **`etl_tasks.daily_etl_pipeline(target_date=None)`**
  - POS end-to-end (extract → raw → clean → fact).

- **`etl_tasks.daily_invoice_sales_pipeline(target_date=None)`**
  - Invoice sales end-to-end.

- **`etl_tasks.daily_invoice_purchases_pipeline(target_date=None)`**
  - Purchases end-to-end.

- **`etl_tasks.daily_inventory_moves_pipeline(target_date=None)`**
  - Inventory moves end-to-end (+ refresh dimensi terkait).

- **`etl_tasks.date_range_etl_pipeline(start_date, end_date=None)`**
  - Menjalankan pipeline harian dalam rentang tanggal.
  - Catatan: saat ini paralel via Celery `group`.

- **`etl_tasks.catch_up_etl()`**
  - Auto backfill jika `ETLMetadata.last_processed_date` tertinggal.

- **`etl_tasks.health_check()`**
  - Health check sederhana; jika tertinggal akan trigger catch-up.

#### 3) Script yang Memanggil ETL (Operator Tools)

- **`etl_runner.py` (GUI lokal / operator)**
  - Memanggil:
    - `date_range_etl_pipeline(start, end)`
    - `refresh_dimensions_incremental(...)` (catatan: target yang tersedia mengikuti implementasi task tersebut)

- **`scripts/force_refresh_pos_data.py`**
  - Memanggil langsung task-task (tanpa antre Celery):
    - POS: `extract_pos_order_lines` → `save_raw_data` → `clean_pos_data` → `update_star_schema`
    - Invoice sales: `extract_sales_invoice_lines` → `save_raw_sales_invoice_lines` → `clean_sales_invoice_lines` → `update_invoice_sales_star_schema`
    - Inventory moves: refresh dimensi → `extract_inventory_moves` → `save_raw_inventory_moves` → `clean_inventory_moves` → `update_inventory_moves_star_schema`

- **`scripts/force_refresh_purchase_data.py`**
  - Purchases: `extract_purchase_invoice_lines` → `save_raw_purchase_invoice_lines` → `clean_purchase_invoice_lines` → `update_purchase_star_schema`

- **`scripts/force_refresh_dimensions.py`**
  - Refresh dimensi secara manual/full (misalnya products/categories/brands/cashiers/taxes) lalu tulis ke `star-schema/`.

#### 4) Perintah Operasional (Command Reference)

##### Jalankan ETL via Celery (dari dalam container)

```bash
# Single date
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.daily_etl_pipeline --args='["2025-12-23"]'

# Date range
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.date_range_etl_pipeline --args='["2025-12-01", "2025-12-31"]'
```

##### Monitoring

```bash
# Logs
docker-compose logs -f celery-worker

# Celery worker status
docker-compose exec celery-worker celery -A etl_tasks inspect active
docker-compose exec celery-worker celery -A etl_tasks inspect stats

# Redis keys (status/cache)
docker-compose exec redis redis-cli KEYS "etl:*"
docker-compose exec redis redis-cli --scan --pattern "celery-task-meta-*"
```

##### Validasi Data (cek layer output)

```bash
# Cek jumlah record fact_sales
docker-compose exec celery-worker python -c "
import polars as pl
df = pl.scan_parquet('/data-lake/star-schema/fact_sales/*.parquet')
print(f'Total records: {df.collect().height:,}')
"
```

#### 5) Troubleshooting (Checklist Praktis)

1. **Tidak ada data di dashboard**
   - Pastikan ada parquet di `star-schema/` (mis: `/data-lake/star-schema/fact_sales/*.parquet`).
   - Cek error log: `docker-compose logs -f celery-worker`.

2. **Task gagal / exception Odoo**
   - Cek env `.env` untuk credential Odoo.
   - Jalankan test koneksi (lihat bagian Troubleshooting di bawah).

3. **Task stuck / result meta menumpuk di Redis**
   - Periksa task aktif:
     - `docker-compose exec celery-worker celery -A etl_tasks inspect active`
   - Bersihkan task meta yang nyangkut (hati-hati, ini menghapus metadata result):
     - `docker-compose exec redis redis-cli --scan --pattern "celery-task-meta-*" | xargs docker-compose exec redis redis-cli del`

4. **Ingin rebuild total**
   - Gunakan bagian “Purging the Data Lake and Resetting Metadata” (di bawah) lalu jalankan backfill (`date_range_etl_pipeline`).

### Manual Trigger
```bash
# Trigger ETL for specific date
docker-compose exec celery-worker celery -A etl_tasks call etl_tasks.daily_etl_pipeline --args='["2025-12-23"]'

# Force full refresh
docker-compose exec celery-worker celery -A etl_tasks purge -f
docker-compose restart celery-worker
```

### Scheduled Tasks
- **Daily ETL**: Runs at 2 AM UTC
- **Data Retention**: 90 days (configurable in `etl_tasks.py`)

## Monitoring

### Logs
```bash
# View all logs
docker-compose logs -f

# View specific service
docker-compose logs -f celery-worker
docker-compose logs -f web

# Check ETL progress
docker exec -it nkdash-redis-1 redis-cli KEYS "etl:*"
```

### Performance Metrics
```bash
# Check Celery stats
docker-compose exec celery-worker celery -A etl_tasks inspect stats

# Monitor task queue
docker-compose exec celery-worker celery -A etl_tasks inspect active
```

### Data Quality
```bash
# Check data volumes
docker volume ls
docker volume inspect nkdash_data-lake

# Sample data check
docker exec nkdash-celery-worker-1 python -c "import polars as pl; df = pl.read_parquet('/app/data-lake/star-schema/dim_products.parquet'); print(f'Total products: {len(df)}')"
```

## Troubleshooting

### Purging the Data Lake and Resetting Metadata
Use these commands from the project root when you need a full rebuild. They remove existing parquet layers (fact, clean, raw) and reset ETL metadata—the Docker bind mount ensures `/data-lake` maps to `D:\data-lake` on Windows.

```powershell
# Delete star-schema fact partitions
docker-compose run --rm celery-worker bash -c "
  rm -rf /data-lake/star-schema/fact_sales &&
  mkdir -p /data-lake/star-schema/fact_sales
"

# Delete cleaned POS partitions
docker-compose run --rm celery-worker bash -c "
  rm -rf /data-lake/clean/pos_order_lines &&
  mkdir -p /data-lake/clean/pos_order_lines
"

# Delete raw POS extracts
docker-compose run --rm celery-worker bash -c "
  rm -rf /data-lake/raw/pos_order_lines &&
  mkdir -p /data-lake/raw/pos_order_lines
"

# Reset ETL metadata baseline (adjust date if needed)
docker-compose run --rm celery-worker bash -c "
  python - <<'PY'
import json, os
metadata_file = '/data-lake/metadata/etl_status.json'
os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
data = {'last_processed_date': '2023-01-01', 'last_updated': '2023-01-01T00:00:00'}
with open(metadata_file, 'w', encoding='utf-8') as fh:
    json.dump(data, fh, indent=2)
print('Metadata reset to 2023-01-01')
PY
"
```

After purging, rerun the desired ETL range (e.g., via `date_range_etl_pipeline`) to repopulate all layers.

### Common Issues
1. **Connection Errors**
   ```bash
   # Test Odoo connection
   docker-compose exec web python -c "from odoorpc_connector import get_odoo_connection; print(get_odoo_connection().db.list())"
   ```

2. **ETL Stuck**
   ```bash
   # List active tasks
   docker-compose exec celery-worker celery -A etl_tasks inspect active
   ```

3. **Disk Space**
   ```bash
   # Clean old Parquet files
   find data-lake/ -name "*.parquet" -mtime +90 -delete
   ```

## Development

### Adding New Pages
1. Create new file in `pages/` directory
2. Follow Dash Mantine Components patterns
3. Access at `http://localhost:8050/page-name`

### Testing
```bash
# Run tests
docker-compose exec web pytest tests/

# Lint code
docker-compose exec web black .
docker-compose exec web flake8
```

### Backup & Restore
```bash
# Backup data
docker-compose exec -T redis redis-cli SAVE
docker cp nkdash-redis-1:/data/dump.rdb ./backup_$(date +%Y%m%d).rdb

# Restore
docker cp backup.rdb nkdash-redis-1:/data/dump.rdb
docker-compose restart redis
```
