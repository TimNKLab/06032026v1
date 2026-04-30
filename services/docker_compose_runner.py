"""Docker Compose execution runner for GUI delegation.

This module provides utilities for the Tkinter GUI to execute
docker-compose exec commands safely and stream stdout/stderr back
to the user interface.
"""

from __future__ import annotations

import subprocess
from typing import List, Optional, Callable


def build_exec_command(service: str, args: List[str]) -> List[str]:
    """Build a docker-compose exec command.
    
    Args:
        service: Docker service name (e.g., 'celery-worker', 'etl-manager-cli')
        args: Command and arguments to execute inside the container
        
    Returns:
        List of command parts ready for subprocess
    """
    return ["docker-compose", "exec", service, *args]


def run_compose_exec(
    *,
    service: str,
    args: List[str],
    cwd: Optional[str] = None,
) -> subprocess.Popen:
    """Execute docker-compose exec and return the process handle.
    
    The caller is responsible for reading stdout/stderr and waiting.
    
    Args:
        service: Docker service name
        args: Command and arguments to execute
        cwd: Working directory for the docker-compose command
        
    Returns:
        subprocess.Popen instance with stdout/stderr piped
    """
    cmd = build_exec_command(service, args)
    return subprocess.Popen(
        cmd,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        encoding="utf-8",
        errors="replace",
    )


def run_compose_exec_with_output(
    *,
    service: str,
    args: List[str],
    cwd: Optional[str] = None,
    line_callback: Optional[Callable[[str], None]] = None,
) -> int:
    """Execute docker-compose exec with streaming output.
    
    Args:
        service: Docker service name
        args: Command and arguments to execute
        cwd: Working directory
        line_callback: Optional callback function(line: str) -> None
        
    Returns:
        Exit code from the command
    """
    proc = run_compose_exec(service=service, args=args, cwd=cwd)
    
    if proc.stdout is not None:
        for line in proc.stdout:
            clean_line = line.rstrip("\n")
            if line_callback:
                line_callback(clean_line)
    
    return proc.wait()
