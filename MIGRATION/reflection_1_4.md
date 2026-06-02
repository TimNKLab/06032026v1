**Subtask 1.4 Reflection: ETL Task Registry**

**Completed:** 2026-06-02  
**Workstream:** NK_20260602_migration_solo_render_0a1b  
**Scope:** Create `etl/tasks.py` as a plain Python function registry mapping string keys to modular ETL functions.

**What Was Done**

1. **Created `etl/tasks.py`**:
   - Implemented `TASK_REGISTRY` dictionary.
   - Mapped all atomic ETL operations from `etl/extract/`, `etl/transform/`, `etl/load/`, and `etl/core/`.
   - Added `get_task()` and `list_registered_tasks()` helper functions.
2. **Additional Extractions (to avoid circular dependencies)**:
   - Moved `refresh_dimensions_incremental` from `etl_tasks.py` to `etl/extract/dimensions.py`.
   - Moved `load_beginning_costs_from_csv` from `etl_tasks.py` to `etl/core/cost_engine.py`.

**Troubleshoot Findings**

**Finding A: Circular Dependency Risk**

If `etl/tasks.py` had imported from `etl_tasks.py` (the god file), we would have had a circular dependency once we started wiring `etl_tasks.py` to use the registry. 

**Fix applied:** Ensured that `etl/tasks.py` ONLY imports from the new modular sub-packages (`etl.core`, `etl.extract`, `etl.transform`, `etl.load`). Any remaining atomic logic in `etl_tasks.py` was extracted before finalising the registry.

**Finding B: Pipeline vs. Atomic Task**

Some functions in `etl_tasks.py` (e.g., `daily_etl_pipeline`) are not atomic tasks but "orchestrators" that call multiple other tasks. Putting them in the `TASK_REGISTRY` would mix concerns.

**Decision:** Only **atomic** functions (those that do one specific thing: extract, clean, or load) are included in `TASK_REGISTRY`. Pipelines are deferred to `scheduler/main.py` where the actual execution loop and dependency ordering are managed.

**Dependency Verification**

**Import** | **Status**
---|---
Celery, Redis, Dash, Flask, gunicorn, odoorpc, services.* | ❌ **Absent** (Registry is framework-agnostic)
`etl.core.*`, `etl.extract.*`, `etl.transform.*`, `etl.load.*` | ✅ **Present** (Correct)

**Risk & Mitigation**

**Risk** | **Status** | **Mitigation**
---|---|---
Task name mismatch between Registry and Scheduler | 🔍 Low risk | Use `list_registered_tasks()` in the scheduler to verify available keys during startup.
Missing a critical task from the original `etl_tasks.py` | 🔍 Medium risk | Manual cross-check of all `@app.task` decorators in `etl_tasks.py` against the registry.

**Next Subtask**

**1.5** — Create `scheduler/main.py`. This will be the heart of the new ETL engine: a Python `schedule` loop that calls tasks from `etl/tasks.py` and manages the daily/manual pipeline flow.

**Files Referenced**

- `etl_tasks.py` (Source of task names)
- `etl/tasks.py` (New registry)
- `etl/extract/dimensions.py` (New extraction)
- `etl/core/cost_engine.py` (New loading logic)

*End of Reflection 1.4*
