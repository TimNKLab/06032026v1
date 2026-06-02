"""Tests for docker_compose_runner utility."""

import pytest
from services.docker_compose_runner import build_exec_command, run_compose_exec


def test_build_exec_command_basic():
    """Test basic command construction."""
    cmd = build_exec_command(
        service="celery-worker",
        args=["python", "scripts/etl_data_manager_cli.py", "-h"],
    )
    assert cmd[0] == "docker-compose"
    assert "exec" in cmd
    assert "celery-worker" in cmd
    assert "python" in cmd
    assert "scripts/etl_data_manager_cli.py" in cmd
    assert "-h" in cmd


def test_build_exec_command_with_multiple_args():
    """Test command with multiple complex arguments."""
    cmd = build_exec_command(
        service="celery-worker",
        args=[
            "python", "scripts/etl_data_manager_cli.py",
            "refresh-mvs-cascading",
            "--views", "mv_profit_daily,mv_sales_daily",
            "--start", "2026-03-08",
            "--end", "2026-04-23",
            "--auto-fetch",
        ],
    )
    assert cmd[0] == "docker-compose"
    assert cmd[1] == "exec"
    assert cmd[2] == "celery-worker"
    assert "--views" in cmd
    assert "mv_profit_daily,mv_sales_daily" in cmd
    assert "--start" in cmd
    assert "2026-03-08" in cmd


def test_build_exec_command_different_service():
    """Test command with different service name."""
    cmd = build_exec_command(
        service="etl-manager-cli",
        args=["python", "-c", "print('hello')"],
    )
    assert cmd[2] == "etl-manager-cli"
