# Subtask 0.1 Reflection: Render Topology & Data Contract Finalization

**Completed:** 2026-06-02
**Workstream:** NK_20260602_migration_solo_render_0a1b
**Researcher:** AI Assistant

---

## Original Assumption
We assumed a multi-service Render architecture:
- `nkdash-app` (Dash Web Service)
- `nkdash-admin` (Streamlit Web Service)
- `nkdash-scheduler` (Background Worker or Cron Job)
- 1 shared Render Persistent Disk

## Troubleshooting Finding
### Blocker: Render Disk Cannot Be Shared
Source: Render Community & Official Docs [1](https://community.render.com/t/how-do-i-share-a-disk-between-services/1478)
> "Sharing a disk between services is not supported... use object storage like Amazon S3."

### Blocker: Render Cron Jobs Cannot Mount Disks
Source: Render Scheduled Tasks FAQ [2](https://render.com/articles/how-render-handles-scheduled-tasks)
> "No. Cron jobs can't provision or access a persistent disk."

### Codebase Finding: `duckdb_connector.py` Has Conflicting `get_readonly_connection()`
File: `services/duckdb_connector.py` lines 42-52 and 54-61.
- First definition: returns `duckdb.connect(database=':memory:')` (correct, in-memory).
- Second definition: returns `self.get_connection()` (disk-backed, overrides the first).
- **Impact:** All dashboard "read-only" queries actually open the disk-backed DuckDB, creating file-lock conflicts with Celery workers and causing the MV refresh stuck issues logged in `NK_20260514_mv_refresh_stuck_0001`.

### Codebase Finding: Windows Host Path in `docker-compose.yml`
`volumes: - D:\data-lake:/data-lake` — This is a local dev artifact that will break on Render Linux containers.

## Adjusted Architecture
### Render Solo Mode
Deploy **1 Render Web Service** running 3 processes via `supervisord`:
1. **gunicorn** (Dash, port 8050, public)
2. **streamlit** (Admin, port 8501, reverse-proxied at `/admin`)
3. **python-schedule** (ETL background loop, no port)

All share **1 local filesystem** mounted to **1 Render Persistent Disk**.

### Why This Works
- Render Persistent Disk attaches to **one service instance**. Solo Mode respects this constraint.
- No distributed queue (Redis) means no networking config, no broker failures.
- Python `schedule` library is sufficient: ETL is daily batch, single-threaded, <30 min runtime.
- Solo maintainer (user) can debug one container, one log stream, one disk.

### Why Pure DuckDB (No SQLite Hybrid)
- User explicitly requested: "kita menggunakan duckdb sebagai ETL dan query, tidak ada lagi hibrida yang memusingkan".
- ETL process writes **Parquet files** (immutable, partition-pruned).
- Dashboard creates **in-memory DuckDB** (`:memory:`), registers views via `read_parquet(...)`, queries, then discards connection. No file lock.
- Since Parquet files are append-only (daily partition writes), there is no write conflict between ETL writer and Dashboard reader.

## Risk Acknowledged
- **Single point of failure:** 1 service = 1 point of failure. Mitigation: Render auto-restart; acceptable for solo maintainer.
- **Fat container:** Not cloud-native best practice. Mitigation: simplicity over purity for current team size.
- **Resource contention:** Dash + Streamlit + ETL in one box. Mitigation: ETL runs at 02:00 WIB when users sleep; Render Starter/Standard tier sufficient.

## Data Contract
```
/var/data/data-lake/
├── raw/              # ETL writes; Dashboard/Admin read-only
├── clean/          # ETL writes; Dashboard/Admin read-only
├── star-schema/    # ETL writes; Dashboard/Admin read-only
└── admin/          # ETL writes logs; Streamlit writes queue
    ├── etl_queue.sqlite
    └── logs/
```

## Troubleshoot 0.2: Bootstrap Constraint (Free Tier + Local-First)

**Date:** 2026-06-02
**Trigger:** User clarified: "kita masih bootstrapping, jadi free tier adalah prioritas utama. bisa juga pikirkan menggunakan deployable container yang asalnya dari local dulu."

### Riset Ulang Cloud Free Tier 2026
| Platform | Free Tier Status 2026 | Cocok untuk NKDash? |
|---|---|---|
| **Render** | Free web service only (no disk, sleeps after 15 min) | ❌ No persistent storage for data lake |
| **Fly.io** | $5 trial credit only, no always-free tier | ❌ Not permanent free |
| **Railway** | Trial credit only, no always-free tier | ❌ Not permanent free |
| **Oracle Cloud** | **Always Free: 2 VMs + 200GB storage, NEVER expires** | ✅ Ideal for bootstrap |

**Source:** [Oracle Cloud Free Tier 2026](https://cloudpricecheck.com/free-tier/oracle) — Always Free services never expire. New accounts also receive $300 trial credit for 30 days.

### Keputusan Revisi: Local-First + Oracle Cloud Free Tier
1. **Develop locally** in single Docker container (now — zero cost)
2. **Validate locally** — all ETL pipelines, all dashboard pages, all admin UI functions
3. **Deploy later** to Oracle Cloud Free Tier — deploy the exact same container, zero monthly cost
4. **No external database service needed** — DuckDB + Parquet IS the database. Saves $7-20/month.

### Perubahan Arsitektur dari 0.1 → 0.2
| Aspek | 0.1 (Render Paid) | 0.2 (Local + Oracle Free) |
|---|---|---|
| Deploy target | Render Web Service ($7/mo Starter) | Oracle Cloud ARM VM ($0) |
| Data persistence | Render Disk ($0.25/GB/mo) | Block Volume (200GB free) |
| External DB service | None | None (DuckDB embedded) |
| Monthly cost | ~$10/mo | $0 |
| Sleep/timeout | Render Free sleeps after 15 min | Always-on VM |

### Oracle Cloud Free Tier Allocation untuk NKDash
| Resource | Free Limit | NKDash Usage | Status |
|---|---|---|---|
| ARM Compute | 4 OCPU + 24GB RAM | 2 OCPU + 4GB RAM | ✅ Within limit |
| Block Storage | 200GB total | 50GB (2+ years retail data) | ✅ Within limit |
| Egress | 10TB/month | <1GB (dashboard HTML+JSON) | ✅ Within limit |

### Perubahan pada Masterplan
- Masterplan title diubah dari "Render Solo Mode" → "Solo Mode: 1 Container, 3 Processes, No Celery, Pure DuckDB, Bootstrap-First"
- Deploy pathway diubah: Local Docker (Phase A) → Oracle Cloud Free Tier (Phase C) → Paid upgrade (Phase D)
- Repo structure: tetap **one repo** (bukan split `nkdash-ui`/`nkdash-pipelines`) untuk simpelitas solo maintainer

## Next Subtask
**1.1** — Extract pure functions from `etl_tasks.py` into `etl/core/profit_calculator.py`, `etl/core/cost_engine.py`, `etl/core/schema.py`.

## Files Referenced
- `docker-compose.yml` (Windows path finding)
- `services/duckdb_connector.py` (double `get_readonly_connection` finding)
- `SSOT.md` (milestones, workstreams)
- `docs/decisions.md` (decision log format)
- Render docs (disk sharing, cron limitations)

---

*End of Reflection 0.1*
