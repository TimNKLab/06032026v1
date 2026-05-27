import time
import logging
from typing import Dict, Any
from functools import wraps

logger = logging.getLogger(__name__)

class PerformanceMonitor:
    """Performance monitoring and metrics collection."""
    
    def __init__(self):
        self.metrics: Dict[str, Any] = {}
    
    def record_timing(self, operation: str, duration: float) -> None:
        """Record operation timing."""
        key = f"{operation}_duration"
        self.metrics[key] = duration
        logger.info(f"[PERF] {operation} completed in {duration:.3f}s")
    
    def record_row_count(self, operation: str, count: int) -> None:
        """Record row count for operation."""
        key = f"{operation}_row_count"
        self.metrics[key] = count
        logger.info(f"[PERF] {operation} processed {count} rows")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get all collected metrics."""
        return self.metrics
    
    def clear_metrics(self) -> None:
        """Clear all metrics."""
        self.metrics = {}

def monitor_performance(operation_name: str):
    """Decorator to monitor function performance."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            monitor = PerformanceMonitor()
            start = time.time()
            
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                monitor.record_timing(operation_name, duration)
                return result
            except Exception as e:
                duration = time.time() - start
                monitor.record_timing(f"{operation_name}_failed", duration)
                raise
        return wrapper
    return decorator
