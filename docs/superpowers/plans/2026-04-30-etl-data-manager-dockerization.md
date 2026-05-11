# ETL Data Manager Dockerization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `scripts/etl_data_manager.py` execute *all ETL/MV operations inside Docker* to eliminate Windows↔Docker DuckDB permission + locking issues, while keeping the GUI usable on Windows.

**Architecture:** Keep the Tkinter GUI on Windows, but refactor it into a thin UI that delegates work to a new container-executed CLI (`etl_data_manager_cli.py`). The CLI runs inside the existing `celery-worker` container via `docker-compose exec`, so DuckDB + data-lake access happen in a single Linux context. GUI streams logs from the CLI back to the user.

**Tech Stack:** Python 3.9, Tkinter (Windows GUI), Docker Compose, Celery worker container, DuckDB.

---

## 0) What Works vs What Doesn’t (Current State)

### What works
- Running the Dash app and ETL pipelines *entirely inside Docker* uses `DATA_LAKE_ROOT=/data-lake` and is consistent.
- `pages/home.py` queries `mv_profit_daily` via `services/profit_metrics.py` and `services/duckdb_connector.py`.
- DuckDB path in Docker is `/data-lake/cache/nkdash.duckdb` (mapped from `D:\data-lake\cache\nkdash.duckdb`).

### What doesn’t
- Running `scripts/etl_data_manager.py` *natively on Windows* creates/locks `D:\data-lake\cache\nkdash.duckdb` under Windows ACL/locking semantics.
- Docker Dash app then fails with `Permission denied` (ACL mismatch) or `IO Error: File is already open` (concurrent opens).
- The GUI can be killed mid-refresh, leaving “success” logs but no persisted MV update.
- `docker-compose exec celery-worker python scripts/force_refresh_pos_data.py ...` failed with `Errno 5 Input/output error` — likely a transient Docker Desktop/WSL mount issue that needs a reproducible diagnosis.

### Core invariant to enforce
- **Only one OS context should touch the DuckDB file.** If Dash runs in Docker, ETL + MV building must also run in Docker.

---

## File/Module Structure Changes

### Create
- `scripts/etl_data_manager_cli.py`
  - Headless CLI entrypoint for scanning/backfill/aggregate/MV refresh.
  - Runs inside Docker containers.

- `services/docker_compose_runner.py`
  - A small utility for the GUI to execute `docker-compose exec ...` safely and stream stdout/stderr.

### Modify
- `scripts/etl_data_manager.py`
  - Keep GUI.
  - Replace direct in-process backfill/MV building calls with calls to the CLI via `docker-compose exec`.

- `docker-compose.yml`
  - Optional: add an explicit `etl-cli` service (or standardize that CLI always runs through `celery-worker`).

### Tests
- `tests/test_docker_compose_runner.py`
  - Unit tests for argument construction and platform quoting behavior.

---

# Task 1: Create a Container-Executed CLI for ETL + MV Refresh

**Files:**
- Create: `scripts/etl_data_manager_cli.py`

- [ ] **Step 1: Create CLI skeleton and argument parsing**

Implement a CLI that supports:
- `scan` (facts/dims/aggs)
- `refresh-mvs-cascading` (with `--views`, `--start`, `--end`, `--auto-fetch`)
- `build-aggregates` (sales/profit)
- `validate-profit`

```python
# scripts/etl_data_manager_cli.py
import argparse
from datetime import date

from scripts.etl_data_manager import BackfillRunner  # reuse logic


def _date(s: str) -> date:
    return date.fromisoformat(s)


def main() -> int:
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    mv = sub.add_parser("refresh-mvs-cascading")
    mv.add_argument("--views", required=True, help="Comma-separated mv_* names")
    mv.add_argument("--start", required=True)
    mv.add_argument("--end", required=True)
    mv.add_argument("--auto-fetch", action="store_true")

    ag = sub.add_parser("build-aggregates")
    ag.add_argument("--types", required=True, help="Comma-separated: sales_aggregates,profit_aggregates")
    ag.add_argument("--start", required=True)
    ag.add_argument("--end", required=True)

    vp = sub.add_parser("validate-profit")
    vp.add_argument("--start", required=True)
    vp.add_argument("--end", required=True)

    args = p.parse_args()

    runner = BackfillRunner(log_fn=print)

    if args.cmd == "refresh-mvs-cascading":
        views = {v.strip() for v in args.views.split(",") if v.strip()}
        res = runner.refresh_materialized_views_cascading(
            views=views,
            start_date=_date(args.start),
            end_date=_date(args.end),
            auto_fetch=bool(args.auto_fetch),
            refresh_dims=False,
        )
        print("RESULT_JSON_START")
        import json
        print(json.dumps(res, default=str))
        print("RESULT_JSON_END")
        return 0

    if args.cmd == "build-aggregates":
        types = [t.strip() for t in args.types.split(",") if t.strip()]
        start = _date(args.start)
        end = _date(args.end)
        out = {"success": 0, "failed": 0, "errors": []}
        for t in types:
            r = runner.backfill_aggregates(t, start, end)
            out["success"] += r.get("success", 0)
            out["failed"] += r.get("failed", 0)
            out["errors"].extend(r.get("errors", []))
        print("RESULT_JSON_START")
        import json
        print(json.dumps(out, default=str))
        print("RESULT_JSON_END")
        return 0

    if args.cmd == "validate-profit":
        res = runner.validate_profit(_date(args.start), _date(args.end))
        print("RESULT_JSON_START")
        import json
        print(json.dumps(res, default=str))
        print("RESULT_JSON_END")
        return 0

    raise SystemExit(2)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Run CLI locally (Windows) as a smoke test**

Run:

`python scripts/etl_data_manager_cli.py -h`

Expected:
- Usage text prints.

- [ ] **Step 3: Run CLI inside Docker via celery-worker**

Run:

`docker-compose exec celery-worker python scripts/etl_data_manager_cli.py -h`

Expected:
- Same usage text prints.

If you hit `Errno 5 Input/output error`, perform the diagnosis steps in Task 2.

- [ ] **Step 4: Commit**

```bash
git add scripts/etl_data_manager_cli.py
git commit -m "feat: add etl data manager CLI for docker execution"
```

---

# Task 2: Diagnose and Stabilize `docker-compose exec` File Access (Errno 5)

**Files:**
- Modify: `docs/runbook.md` (optional) OR add notes to SSOT decision log if you have one

- [ ] **Step 1: Validate container sees the repo scripts**

Run:

`docker-compose exec celery-worker python -c "import os; import glob; print(os.getcwd()); print(glob.glob('scripts/*.py')[:10])"`

Expected:
- Current working dir `/app`
- `scripts/force_refresh_pos_data.py` appears in listing.

- [ ] **Step 2: Validate mount health**

Run:

`docker-compose exec celery-worker ls -la scripts | head`

Expected:
- Directory listing prints quickly.

- [ ] **Step 3: If mount errors persist, apply Docker Desktop/WSL reset procedure**

On Windows (PowerShell):
- Stop containers: `docker-compose down`
- Restart Docker Desktop
- Hard reset WSL filesystem integration:
  - `wsl --shutdown`
- Re-run: `docker-compose up -d redis celery-worker dash-app`

Expected:
- No more `/run/desktop/mnt/host/d/... mkdir ... file exists` errors.

- [ ] **Step 4: Commit any runbook/notes updates**

Only if docs were edited.

---

# Task 3: Add a Safe Docker Compose Runner Utility (GUI → Docker)

**Files:**
- Create: `services/docker_compose_runner.py`
- Test: `tests/test_docker_compose_runner.py`

- [ ] **Step 1: Write failing tests for command building**

```python
# tests/test_docker_compose_runner.py
from services.docker_compose_runner import build_exec_command


def test_build_exec_command_windows_quoting():
    cmd = build_exec_command(
        service="celery-worker",
        args=["python", "scripts/etl_data_manager_cli.py", "-h"],
    )
    assert cmd[0] == "docker-compose"
    assert "exec" in cmd
    assert "celery-worker" in cmd
```

- [ ] **Step 2: Run test to verify it fails**

Run:

`pytest tests/test_docker_compose_runner.py -q`

Expected:
- FAIL because `services/docker_compose_runner.py` doesn’t exist.

- [ ] **Step 3: Implement minimal runner**

```python
# services/docker_compose_runner.py
from __future__ import annotations

import subprocess
from typing import Iterable, List, Optional


def build_exec_command(service: str, args: List[str]) -> List[str]:
    return ["docker-compose", "exec", service, *args]


def run_compose_exec(
    *,
    service: str,
    args: List[str],
    cwd: Optional[str] = None,
) -> subprocess.Popen:
    cmd = build_exec_command(service, args)
    return subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
```

- [ ] **Step 4: Run tests again**

Run:

`pytest tests/test_docker_compose_runner.py -q`

Expected:
- PASS.

- [ ] **Step 5: Commit**

```bash
git add services/docker_compose_runner.py tests/test_docker_compose_runner.py
git commit -m "feat: add docker-compose exec runner for GUI delegation"
```

---

# Task 4: Refactor Tkinter GUI to Delegate Operations to Docker CLI

**Files:**
- Modify: `scripts/etl_data_manager.py`

- [ ] **Step 1: Add a feature flag and service name constants**

Add near the top-level config:

```python
DOCKER_ETL_ENABLED = os.environ.get("ETL_DATA_MANAGER_USE_DOCKER") in {"1", "true", "True", "yes", "YES"}
DOCKER_ETL_SERVICE = os.environ.get("ETL_DATA_MANAGER_DOCKER_SERVICE", "celery-worker")
```

- [ ] **Step 2: Implement a helper to stream docker output into the GUI log**

In `ETLDataManagerApp`, add:

```python
from services.docker_compose_runner import run_compose_exec


def _run_docker_cli(self, cli_args: list[str]) -> int:
    proc = run_compose_exec(service=DOCKER_ETL_SERVICE, args=cli_args, cwd=str(project_root))
    assert proc.stdout is not None
    for line in proc.stdout:
        self.root.after(0, lambda ln=line: self.log(ln.rstrip("\n")))
    return proc.wait()
```

- [ ] **Step 3: Update MV refresh to call docker CLI when enabled**

Replace the direct call to `self.runner.refresh_materialized_views_cascading(...)` with:

```python
if DOCKER_ETL_ENABLED and start_date and end_date:
    views_csv = ",".join(sorted(views))
    code = self._run_docker_cli([
        "python", "scripts/etl_data_manager_cli.py",
        "refresh-mvs-cascading",
        "--views", views_csv,
        "--start", start_date.isoformat(),
        "--end", end_date.isoformat(),
        "--auto-fetch",
    ])
    if code != 0:
        raise RuntimeError(f"Docker CLI failed with exit code {code}")
else:
    # existing in-process logic
```

- [ ] **Step 4: Update Backfill + Build Aggregates + Validate Profit handlers similarly**

- Backfill facts/dims → docker CLI commands that call force refresh scripts inside container OR call into `BackfillRunner` via CLI.
- Build aggregates → `python scripts/etl_data_manager_cli.py build-aggregates --types ...`
- Validate profit → `python scripts/etl_data_manager_cli.py validate-profit ...`

- [ ] **Step 5: Run `python -m py_compile`**

Run:

`python -m py_compile scripts/etl_data_manager.py scripts/etl_data_manager_cli.py`

Expected:
- No output (success).

- [ ] **Step 6: Commit**

```bash
git add scripts/etl_data_manager.py
git commit -m "refactor: delegate etl data manager operations to docker CLI"
```

---

# Task 5: Add a Dedicated Docker Compose Service (Optional but Cleaner)

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Add `etl-manager-cli` service**

```yaml
  etl-manager-cli:
    build: .
    user: root
    environment:
      - DATA_LAKE_ROOT=/data-lake
      - REDIS_URL=redis://redis:6379/0
    volumes:
      - .:/app
      - D:\data-lake:/data-lake
      - D:\logs:/app/logs
    depends_on:
      redis:
        condition: service_healthy
    entrypoint: ["python", "scripts/etl_data_manager_cli.py"]
```

- [ ] **Step 2: Verify it runs**

Run:

`docker-compose run --rm etl-manager-cli -h`

Expected:
- CLI help text.

- [ ] **Step 3: Point GUI default service to `etl-manager-cli`**

Set:
- `ETL_DATA_MANAGER_DOCKER_SERVICE=etl-manager-cli`

- [ ] **Step 4: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: add etl manager CLI service to docker-compose"
```

---

# Task 6: End-to-End Verification

**Files:**
- No code changes required

- [ ] **Step 1: Start stack**

Run:

`docker-compose up -d redis celery-worker dash-app`

Expected:
- All healthy.

- [ ] **Step 2: Run GUI with docker mode enabled**

Run:

`$env:ETL_DATA_MANAGER_USE_DOCKER="1"; python scripts/etl_data_manager.py`

Expected:
- GUI opens.
- MV refresh logs come from container (should include `[duckdb] connecting to /data-lake/cache/nkdash.duckdb...`).

- [ ] **Step 3: Refresh MVs for a known-missing range**

In GUI:
- Set date range `2026-03-08 → 2026-04-23`
- Click `Refresh All MVs`

Expected:
- No Windows `dllhost.exe` locking issues.
- No `Permission denied` errors in Docker logs.

- [ ] **Step 4: Verify MV coverage from inside Docker**

Run:

`docker-compose exec dash-app python -c "from services.duckdb_connector import get_duckdb_connection as g; c=g(); print(c.execute('select min(date), max(date), count(*) from mv_profit_daily').fetchall())"`

Expected:
- Max date >= `2026-04-23`.

- [ ] **Step 5: Verify Home page**

Open `http://localhost:8050` and set date range.

Expected:
- No 500 errors.
- Chart shows 2026 bars.

---

## Self-Review Checklist

- **Spec coverage:** This plan ensures (1) ETL/MV runs in Docker, (2) GUI remains usable, (3) reproducible verification steps.
- **Placeholder scan:** No TBDs; each step includes runnable commands/snippets.
- **Type consistency:** CLI args and GUI delegation use consistent subcommands and ISO dates.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-04-30-etl-data-manager-dockerization.md`.

Two execution options:

1) **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks

2) **Inline Execution** — Execute tasks in this session with checkpoints

Which approach do you want?
