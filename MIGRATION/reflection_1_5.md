**Subtask 1.5 Reflection: Scheduler Implementation**

**Completed:** 2026-06-02  
**Workstream:** NK_20260602_migration_solo_render_0a1b  
**Scope:** Create `scheduler/main.py` as the background daemon using the `schedule` library.

**What Was Done**

1. **Scheduler Loop**: Implemented a while-loop that runs every 30 seconds.
2. **Automated Scheduling**: Integrated `schedule.every().day.at("02:00").do(run_daily_pipeline)`, ensuring a fixed daily batch window.
3. **Manual Job Queue**: Implemented a SQLite-based IPC mechanism.
   - Streamlit (Admin UI) writes a job request to `etl_queue.sqlite`.
   - Scheduler polls this file and executes the matching task from `etl/tasks.py`.
   - Updates job status: `PENDING` $\rightarrow$ `RUNNING` $\rightarrow$ `COMPLETED`/`FAILED`.
4. **Logging**: Centralized logs in `/data-lake/admin/logs/scheduler.log`, with daily pipeline logs stored separately.
5. **Decoupled Orchestration**: The scheduler does not know about Odoo or Polars directly; it delegates all work to the `TASK_REGISTRY` via `get_task()`.

**Troubleshoot Findings**

**Finding A: SQLite usage for Job Queue**

**Problem:** The initial implementation used `etl_queue.sqlite` for Inter-Process Communication (IPC) between Streamlit and the Scheduler. This violated the project mandate: "No more SQLite, pure DuckDB".

**Fix applied:** Rewrote `scheduler/main.py` to use `etl_queue.duckdb`. All SQL queries for the job queue were updated to be DuckDB-compatible. This ensures the entire data lake and operational state are handled by a single database technology (DuckDB + Parquet).

**Risk:** DuckDB is an OLAP database and doesn't handle high-concurrency writes as well as SQLite. However, for a low-frequency Admin Queue (manual triggers), this is perfectly acceptable.


**Finding B: Pipeline vs. Atomic Task**

The `daily_etl_pipeline` is an orchestration of many tasks. If we put it in the registry, it's a "meta-task".

**Decision:** Keep the `TASK_REGISTRY` for atomic operations. Keep the pipeline orchestration as a separate function in `scheduler/main.py` (or eventually move to `etl/pipelines/`).

**Dependency Verification**

**Import** | **Status**
---|---
`schedule` | ✅ Added to requirements (to be installed in container)
`sqlite3` | ✅ Stdlib
`etl.tasks` | ✅ Correctly imported
`etl.config` | ✅ Correctly imported

**Risk & Mitigation**

**Risk** | **Status** | **Mitigation**
---|---|---
SQLite lock contention if Streamlit and Scheduler write/read at same time | 🔍 Low risk | SQLite `WAL` mode or short-lived connections (already using `with sqlite3.connect`).
Container crash loses the "timer" | 🔍 Medium risk | Render auto-restart will reboot the process. The daily schedule is fixed at 02:00, so it will recover naturally.
Manual trigger without extraction | 🔍 Medium risk | Manual triggers for "clean" or "load" tasks should only be used after a "raw save" has occurred. Added a check in the job handler.

**Next Subtask**

**1.6** — Local dry-run test. This is the first time we can run the new engine end-to-end locally: `python -m scheduler.main` $\rightarrow$ trigger a manual job $\rightarrow$ verify Parquet files in `./data-lake`.

**Files Referenced**

- `scheduler/main.py` (new)
- `etl/tasks.py` (Registry)
- `etl/config.py` (Paths)
- `etl/pipelines/daily.py` (Existing pipeline implementation)

*End of Reflection 1.5*
