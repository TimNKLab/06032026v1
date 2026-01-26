# Dashboard KPI New Khatulistiwa - Panduan Lengkap

## Daftar Isi
1. [Ikhtisar](#ikhtisar)
2. [Prasyarat](#prasyarat)
3. [Instalasi](#instalasi)
4. [Konfigurasi](#konfigurasi)
5. [Menjalankan Aplikasi](#menjalankan-aplikasi)
6. [Proses ETL](#proses-etl)
6.1. [Alur Data & Referensi ETL (Bahasa Indonesia)](#alur-data--referensi-etl-bahasa-indonesia)
7. [Pemantauan](#pemantauan)
8. [Pemecahan Masalah](#pemecahan-masalah)
9. [Pengembangan](#pengembangan)

## Ikhtisar

Dashboard ini menyediakan analitik penjualan real-time untuk New Khatulistiwa, dibangun dengan:
- **Frontend**: Plotly Dash dengan Dash Mantine Components
- **Backend**: Python (Polars, Celery, Redis)
- **Pipeline Data**: Odoo → Parquet Raw → Parquet Clean → Star Schema
- **Kontainerisasi**: Docker dengan multi-stage build

## Prasyarat

- Docker & Docker Compose
- Python 3.9+
- Kredensial akses API Odoo
- Redis (sudah termasuk dalam setup Docker)

## Instalasi

1. Clone repository:
   ```bash
   git clone <your-repo-url>
   cd nkdash
   ```

2. Buat dan konfigurasi file `.env`:
   ```bash
   # Buat .env (tidak ada .env.example di repo ini)
   # Edit dengan kredensial Anda
   ```

## Cara Menggunakan

### Variabel Environment (`.env`)
```ini
# Koneksi Odoo
di .env, ganti credentials berikut.
ODOO_HOST=your-odoo-instance.odoo.com
ODOO_PORT=443
ODOO_PROTOCOL=jsonrpc+ssl
ODOO_DB=your_database
ODOO_USERNAME=your_email@example.com
ODOO_API_KEY=your_api_key

# Redis
REDIS_URL=redis://redis:6379/0

# Data Lake
# - Rekomendasi (Docker bind mount): /data-lake
# - Default lama pada beberapa komponen: /app/data-lake
# Pastikan ini konsisten di semua container.
DATA_LAKE_ROOT=/data-lake
```

### Struktur Data Lake
```
data-lake/
├── raw/            # Data mentah dari Odoo
├── clean/          # Data yang sudah diproses
└── star-schema/    # Data siap analitik
```

## Menjalankan Aplikasi

### Mode Development
```bash
# Jalankan semua service
docker-compose up --build

# Atau jalankan service tertentu
docker-compose up dash-app celery-worker celery-beat redis
```

### Catatan (Windows)

- **Bind mount data lake**: host `D:\\data-lake` di-mount ke `/data-lake` di dalam container.
- **Bind mount logs**: host `D:\\logs` di-mount ke `/app/logs`.
- **Port Dash app**: `http://localhost:8050`.

## Proses ETL

### Arsitektur Task ETL (Catatan Developer)

`etl_tasks.py` adalah **satu-satunya entry point** ("mother" script) untuk eksekusi ETL.

- Mendefinisikan Celery `app` dan semua nama task.
- Mengatur jadwal **Celery Beat** dan **routing task** (queue).
- Script lain (misalnya `etl_runner.py` dan `scripts/force_refresh_*.py`) mengimpor task/fungsi langsung dari `etl_tasks.py`.

#### Grup Task (saat ini)

- **Ekstraksi**
  - `extract_pos_order_lines`
  - `extract_sales_invoice_lines`
  - `extract_purchase_invoice_lines`
  - `extract_inventory_moves`
- **Penyimpanan raw**
  - `save_raw_data`
  - `save_raw_sales_invoice_lines`
  - `save_raw_purchase_invoice_lines`
  - `save_raw_inventory_moves`
- **Cleaning / transformasi**
  - `clean_pos_data`
  - `clean_sales_invoice_lines`
  - `clean_purchase_invoice_lines`
  - `clean_inventory_moves`
- **Loading star-schema**
  - `update_star_schema`
  - `update_invoice_sales_star_schema`
  - `update_purchase_star_schema`
  - `update_inventory_moves_star_schema`
- **Dimensi**
  - `refresh_dimensions_incremental`
- **Orkestrasi / kesehatan**
  - `daily_etl_pipeline`, `date_range_etl_pipeline`, `catch_up_etl`, `health_check`

#### Layout Modular yang Diusulkan (rencana)

Kita akan mempertahankan `etl_tasks.py` sebagai entry point, tetapi memindahkan implementasi ke modul-modul dan menjaga fungsi task sebagai wrapper tipis.

Struktur yang diusulkan:
```python
nkdash/
  etl_tasks.py                      # Celery app + definisi task (wrapper) + schedule/route
  etl/
    __init__.py
    config.py                       # konstanta, parsing env, path data-lake
    io_parquet.py                   # helper write/read parquet secara atomik
    metadata.py                     # ETLMetadata
    dimension_cache.py              # Loader dim tables
    odoo_pool.py                    # Pool connection ke odoo
    odoo_helpers.py                 # safe_extract_m2o, batch_ids, extract_o2m_ids, helper field model
    extract/
      pos.py                        # implementasi ekstraksi POS
      invoices.py                   # implementasi ekstraksi invoice line
      inventory_moves.py            # implementasi ekstraksi inventory
    transform/
      pos.py                        # implementasi clean_pos_data
      invoices.py                   # implementasi cleaning invoice
      inventory_moves.py            # implementasi cleaning inventory
    load/
      facts.py                      # implementasi update_fact_*
      dimensions.py                 # implementasi refresh_dimensions_incremental
    pipelines/
      daily.py                      # implementasi daily_*_pipeline
      ranges.py                     # implementasi date_range_etl_pipeline
      health.py                     # implementasi catch_up_etl / health_check
```

#### Aturan Backward Compatibility

- Nama task Celery **harus tetap** `etl_tasks.<task_name>` (Beat schedule dan routing bergantung pada ini).
- `etl_tasks.py` akan tetap mengekspor simbol yang sama agar script lama tetap berjalan.
- Implementasi internal boleh dipindah, tetapi wrapper di `etl_tasks.py` harus stabil.

#### Menambahkan Dataset ETL Baru (developer baru)

- Tambahkan **extractor** di `etl/extract/<dataset>.py`.
- Tambahkan **raw writer** (jika perlu) di `etl/io_parquet.py` atau helper spesifik dataset.
- Tambahkan **cleaner** di `etl/transform/<dataset>.py`.
- Tambahkan **loader** di `etl/load/<dataset>.py` (atau extend `etl/load/facts.py`).
- Register/Expose wrapper task di `etl_tasks.py` dan tambahkan:
  - Entri schedule Beat (opsional)
  - Queue routing task (disarankan)

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

2. **Celery Tasks (Orkestrasi ETL)**
   - Semua task bernama `etl_tasks.<nama_task>`.
   - `etl_tasks.py` adalah entrypoint; sebagian implementasi dipindah ke paket `etl/`.

3. **Data Lake (Layer Parquet)**
   - Layer yang digunakan:
     - `raw/`: hasil ekstraksi mentah dari Odoo
     - `clean/`: hasil pembersihan/normalisasi
     - `star-schema/`: tabel fakta/dimensi siap query
   - Catatan path:
     - Lokasi root mengikuti env `DATA_LAKE_ROOT`.
     - Pada setup Docker Windows yang umum, folder host `D:\data-lake` di-mount menjadi `/data-lake` di dalam container.
     - Beberapa bagian kode/skrip masih menggunakan default lama `/app/data-lake`.
     - Rekomendasi: set `DATA_LAKE_ROOT=/data-lake` di `.env` dan gunakan value yang sama untuk semua service (dash-app/celery-worker/celery-beat).

4. **DuckDB Views (Layer Query untuk Dashboard)**
   - `services/duckdb_connector.py` membuat view dari Parquet:
     - `fact_sales`, `fact_invoice_sales`, `fact_purchases`, `fact_inventory_moves`
     - `dim_products`, `dim_categories`, `dim_brands`, `dim_taxes`
     - `fact_sales_all` = gabungan POS + invoice sales

5. **Dashboard (Dash Pages)**
   - Contoh: `pages/sales.py`
     - KPI & chart memanggil `services/sales_metrics.py` dan `services/sales_charts.py`
     - Modul tersebut melakukan query ke DuckDB (fast path), lalu fallback ke Odoo bila query gagal.

#### 2) Katalog Task ETL (Apa fungsinya?)

Di bawah ini ringkasan tiap task utama, output file layer, dan siapa yang biasanya memanggilnya.

##### A. Ekstraksi
Setiap ekstraktor bisa diakses melalui penjadwalan maupun by command.

- **`etl_tasks.extract_pos_order_lines(target_date)`**
  - **Fungsi**: ambil POS order `target_date`.
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

###### Inventory moves — schema penting (untuk rekonsiliasi)

`fact_inventory_moves` sekarang menyimpan kolom tambahan untuk klasifikasi pergerakan stok:

- **`movement_type`**: tipe pergerakan (string)
  - `incoming`: penerimaan barang ke lokasi internal (vendor receipts)
  - `production_in`: hasil produksi masuk ke stok internal
  - `production_out`: konsumsi bahan baku untuk produksi
  - `adjustment`: penyesuaian stok (stock count / write-off / koreksi manual; biasanya melibatkan location usage `inventory`)
  - `transfer`: perpindahan internal (antar lokasi internal)
- **`inventory_adjustment_flag`**: boolean, `True` jika diklasifikasikan sebagai adjustment
- **`manufacturing_order_id`**: ID manufacturing order bila terkait produksi (output/consumption)

Catatan:
- Dashboard Sell-through menggunakan **`incoming + production_in`** sebagai “Units Received” default (adjustment & production_out tidak dihitung sebagai receipt).

##### B. Penyimpanan Raw

- **`etl_tasks.save_raw_data(extraction_result)`**
  - **Fungsi**: tulis raw POS ke layer `raw/` (dataset POS).

- **`etl_tasks.save_raw_sales_invoice_lines(extraction_result)`**
  - **Fungsi**: tulis raw invoice sales ke layer `raw/` (dataset invoice sales).

- **`etl_tasks.save_raw_purchase_invoice_lines(extraction_result)`**
  - **Fungsi**: tulis raw purchases ke layer `raw/` (dataset purchases).

- **`etl_tasks.save_raw_inventory_moves(extraction_result)`**
  - **Fungsi**: tulis raw inventory moves ke layer `raw/` (dataset inventory moves).

##### C. Cleaning / Transformasi

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

##### E. Dimensi

- **`etl_tasks.refresh_dimensions_incremental(targets=None)`**
  - **Fungsi**: build/update dimensi yang dipakai untuk enrichment (contoh: products, locations, uoms, partners, users, companies, lots).
  - **Output**: file dimensi di `star-schema/` (misalnya `dim_products.parquet`).
  - **Dipakai oleh**:
    - schedule beat `incremental-dimension-refresh`
    - pipeline inventory moves (sebelum ekstraksi)

##### F. Orkestrasi / Pipeline

- **`etl_tasks.daily_etl_pipeline(target_date=None)`**
  - POS end-to-end (extract → raw → clean → fact).

- **`etl_tasks.daily_invoice_sales_pipeline(target_date=None)`**
  - Invoice sales end-to-end.

- **`etl_tasks.daily_invoice_purchases_pipeline(target_date=None)`**
  - Purchases end-to-end.

- **`etl_tasks.daily_inventory_moves_pipeline(target_date=None)`**
  - Inventory moves end-to-end (+ refresh dimensi terkait).

- **`etl_tasks.daily_stock_quants_pipeline(target_date=None)`**
  - Stock quants snapshot end-to-end (+ refresh dimensi terkait).

- **`etl_tasks.date_range_etl_pipeline(start_date, end_date=None)`**
  - Menjalankan pipeline harian dalam rentang tanggal.
  - Catatan: saat ini paralel via Celery `group`.

- **`etl_tasks.catch_up_etl()`**
  - Auto backfill jika `ETLMetadata.last_processed_date` tertinggal.

- **`etl_tasks.health_check()`**
  - Health check sederhana; jika tertinggal akan memicu catch-up.

#### 3) Script Force Refresh (Tools Operator)

- **`etl_runner.py` (GUI lokal / operator)**
  - Memanggil:
    - `date_range_etl_pipeline(start, end)`
    - `refresh_dimensions_incremental(...)` (catatan: target yang tersedia mengikuti implementasi task tersebut)

- **`scripts/force_refresh_pos_data.py`**
  - Memanggil langsung task-task (tanpa antre Celery):
    - POS: `extract_pos_order_lines` → `save_raw_data` → `clean_pos_data` → `update_star_schema`
    - Invoice sales: `extract_sales_invoice_lines` → `save_raw_sales_invoice_lines` → `clean_sales_invoice_lines` → `update_invoice_sales_star_schema`
    - Inventory moves: refresh dimensi → `extract_inventory_moves` → `save_raw_inventory_moves` → `clean_inventory_moves` → `update_inventory_moves_star_schema`
    
    refresh data PoS dari docker: `docker-compose exec celery-worker python scripts/force_refresh_pos_data.py --start 2026-01-06 --end 2026-01-07 --targets pos`
    refresh penjualan invoice dari docker: `docker-compose exec celery-worker python scripts/force_refresh_pos_data.py --start 2026-01-06 --end 2026-01-07 --targets invoice-sales`
    refresh pergerakan inventory dari docker: `docker-compose exec celery-worker python scripts/force_refresh_pos_data.py --start 2026-01-06 --end 2026-01-07 --targets inventory-moves`
    refresh stock quants dari docker: `docker-compose exec celery-worker python scripts/force_refresh_stock_quants.py --start 2026-01-06 --end 2026-01-07`

- **`scripts/force_refresh_purchase_data.py`**
  - Purchases: `extract_purchase_invoice_lines` → `save_raw_purchase_invoice_lines` → `clean_purchase_invoice_lines` → `update_purchase_star_schema`
    cara refresh data purchase dari docker: `docker-compose exec celery-worker python scripts/force_refresh_purchase_data.py --start 2026-01-06 --end 2026-01-07`

- **`scripts/force_refresh_dimensions.py`**
  - Refresh dimensi secara manual/full (misalnya products/categories/brands/cashiers/taxes) lalu tulis ke `star-schema/`.
  
    cara refresh dimensi dari docker: `docker-compose exec celery-worker python scripts/force_refresh_dimensions.py`

- **`scripts/force_refresh_stock_quants.py`**
  - Stock quants snapshot: refresh dimensi → `extract_stock_quants` → `save_raw_stock_quants` → `clean_stock_quants` → `update_stock_quants_star_schema`
    cara refresh stock quants dari docker: `docker-compose exec celery-worker python scripts/force_refresh_stock_quants.py --start 2026-01-06 --end 2026-01-07`

#### 4) Perintah Operasional (Referensi Command)

##### Jalankan ETL via Celery (dari dalam container)

via command prompt 
```bash
# Tanggal tunggal (pipeline POS)
docker-compose exec celery-worker python -c "from etl_tasks import daily_etl_pipeline; daily_etl_pipeline.delay('2025-12-24')"

# Tanggal tunggal (invoice sales)
docker-compose exec celery-worker python -c "from etl_tasks import daily_invoice_sales_pipeline; daily_invoice_sales_pipeline.delay('2025-12-24')"

# Tanggal tunggal (purchases)
docker-compose exec celery-worker python -c "from etl_tasks import daily_invoice_purchases_pipeline; daily_invoice_purchases_pipeline.delay('2025-12-24')"

# Tanggal tunggal (inventory moves)
docker-compose exec celery-worker python -c "from etl_tasks import daily_inventory_moves_pipeline; daily_inventory_moves_pipeline.delay('2025-12-24')"

# Tanggal tunggal (stock quants snapshot)
docker-compose exec celery-worker python -c "from etl_tasks import daily_stock_quants_pipeline; daily_stock_quants_pipeline.delay('2025-12-24')"

# Rentang tanggal (mengantrikan group paralel pipeline POS harian)
docker-compose exec celery-worker python -c "from etl_tasks import date_range_etl_pipeline; date_range_etl_pipeline.delay('2025-12-23','2025-12-24')"
```

##### Pemantauan

```bash
# Log
docker-compose logs -f celery-worker

# Status worker Celery
docker-compose exec celery-worker celery -A etl_tasks inspect active
docker-compose exec celery-worker celery -A etl_tasks inspect stats

# Redis keys (status/cache)
docker-compose exec redis redis-cli KEYS "etl:*"
docker-compose exec redis redis-cli --scan --pattern "celery-task-meta-*"
```

##### Validasi Data (cek output layer)

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

3. **Task macet / result meta menumpuk di Redis**
   - Periksa task aktif:
     - `docker-compose exec celery-worker celery -A etl_tasks inspect active`
   - Bersihkan task meta yang nyangkut (hati-hati, ini menghapus metadata result):
     - `docker-compose exec redis redis-cli --scan --pattern "celery-task-meta-*" | xargs docker-compose exec redis redis-cli del`

4. **Ingin rebuild total**
   - Gunakan bagian “Mengosongkan Data Lake dan Reset Metadata” (di bawah) lalu jalankan backfill (`date_range_etl_pipeline`).

### Trigger Manual
```bash
# Trigger ETL untuk tanggal tertentu (disarankan di Windows: hindari masalah quoting JSON Celery CLI)
docker-compose exec celery-worker python -c "from etl_tasks import daily_etl_pipeline; daily_etl_pipeline.delay('2025-12-23')"

# Paksa full refresh antrean (BERBAHAYA: menghapus task yang sudah terantre)
docker-compose exec celery-worker celery -A etl_tasks purge -f
```

### Task Terjadwal
- **ETL Harian**: berjalan pukul `02:00` waktu lokal (timezone Celery default `Asia/Jakarta` via `etl_tasks.py`).
- **Pipeline harian lain**: invoice sales `02:05`, purchases `02:10`, inventory moves `02:15`.

## Pemantauan

### Log
```bash
# Lihat semua log
docker-compose logs -f

# Lihat log service tertentu
docker-compose logs -f celery-worker
docker-compose logs -f dash-app

# Cek progres ETL
docker-compose exec redis redis-cli KEYS "etl:*"
```

### Metrik Performa
```bash
# Cek statistik Celery
docker-compose exec celery-worker celery -A etl_tasks inspect stats

# Pantau antrean task
docker-compose exec celery-worker celery -A etl_tasks inspect active
```

### Kualitas Data
```bash
# Cek volume data
docker volume ls
docker volume inspect nkdash_data-lake

# Cek sampel data
docker-compose exec celery-worker python -c "import polars as pl; df = pl.read_parquet('/data-lake/star-schema/dim_products.parquet'); print(f'Total products: {len(df)}')"
```

## Pemecahan Masalah

### Mengosongkan Data Lake dan Reset Metadata
Gunakan perintah ini dari root project saat Anda perlu rebuild penuh. Perintah ini menghapus layer parquet yang ada (fact, clean, raw) dan mereset metadata ETL—Docker bind mount memastikan `/data-lake` memetakan ke `D:\data-lake` di Windows.

```powershell
# Hapus partisi fact pada star-schema
docker-compose run --rm celery-worker bash -c "
  rm -rf /data-lake/star-schema/fact_sales &&
  mkdir -p /data-lake/star-schema/fact_sales
"

# Hapus partisi POS yang sudah dibersihkan
docker-compose run --rm celery-worker bash -c "
  rm -rf /data-lake/clean/pos_order_lines &&
  mkdir -p /data-lake/clean/pos_order_lines
"

# Hapus hasil ekstraksi raw POS
docker-compose run --rm celery-worker bash -c "
  rm -rf /data-lake/raw/pos_order_lines &&
  mkdir -p /data-lake/raw/pos_order_lines
"

# Reset baseline metadata ETL (sesuaikan tanggal jika perlu)
docker-compose run --rm celery-worker bash -c "
  python - <<'PY'
import json, os
metadata_file = '/data-lake/metadata/etl_status.json'
os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
data = {'last_processed_date': '2023-01-01', 'last_updated': '2023-01-01T00:00:00'}
with open(metadata_file, 'w', encoding='utf-8') as fh:
    json.dump(data, fh, indent=2)
print('Metadata direset ke 2023-01-01')
PY
"
```

Setelah purge, jalankan ulang rentang ETL yang diinginkan (misalnya via `date_range_etl_pipeline`) untuk mengisi ulang semua layer.

### Masalah Umum
1. **Error Koneksi**
   ```bash
   # Tes koneksi Odoo
   docker-compose exec web python -c "from odoorpc_connector import get_odoo_connection; print(get_odoo_connection().db.list())"
   ```

2. **ETL Macet**
   ```bash
   # Daftar task yang aktif
   docker-compose exec celery-worker celery -A etl_tasks inspect active
   ```

3. **Ruang Disk**
   ```bash
   # Bersihkan file Parquet lama
   find data-lake/ -name "*.parquet" -mtime +90 -delete
   ```

## Pengembangan

### Menambahkan Halaman Baru
1. Buat file baru di direktori `pages/`
2. Ikuti pola Dash Mantine Components
3. Akses di `http://localhost:8050/page-name`

### Pengujian
```bash
# Jalankan test
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