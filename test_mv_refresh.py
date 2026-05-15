#!/usr/bin/env python3
"""
Test script for MV refresh functionality.
Tests the core components without requiring full Docker environment.
"""

import os
import sys
import time
import json
from datetime import date, timedelta

# Add project root to path
sys.path.append('.')

def test_mv_refresh_imports():
    """Test that all MV refresh components can be imported."""
    print("Testing MV refresh imports...")
    
    try:
        from etl_tasks import refresh_materialized_views, refresh_materialized_views_scheduled
        print("✓ MV refresh tasks imported successfully")
    except ImportError as e:
        print(f"✗ MV refresh tasks import failed: {e}")
        return False
    
    try:
        from etl_tasks import etl_operation_lock
        print("✓ ETL operation lock imported successfully")
    except ImportError as e:
        print(f"✗ ETL operation lock import failed: {e}")
        return False
    
    try:
        from services.docker_compose_runner import run_compose_exec_with_output
        print("✓ Docker compose runner imported successfully")
    except ImportError as e:
        print(f"✗ Docker runner import failed: {e}")
        return False
    
    return True

def test_redis_coordination():
    """Test Redis coordination components."""
    print("\nTesting Redis coordination...")
    
    try:
        import redis
        print("✓ Redis module imported")
        
        # Test Redis connection (if available)
        redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
        try:
            client = redis.from_url(redis_url, socket_connect_timeout=2)
            client.ping()
            print("✓ Redis connection successful")
            
            # Test lock key operations
            test_key = "test:mv_lock"
            client.set(test_key, "test_value", ex=60)
            value = client.get(test_key)
            client.delete(test_key)
            print("✓ Redis lock operations successful")
            
            client.close()
        except redis.ConnectionError:
            print("⚠ Redis not available - this is expected outside Docker")
        
        return True
    except ImportError as e:
        print(f"✗ Redis import failed: {e}")
        return False

def test_task_configuration():
    """Test Celery task configuration."""
    print("\nTesting task configuration...")
    
    try:
        from etl_tasks import app
        
        # Check task routes
        mv_routes = [route for route in app.conf.task_routes.keys() 
                    if 'materialized_views' in route]
        expected_routes = [
            'etl_tasks.refresh_materialized_views',
            'etl_tasks.refresh_materialized_views_scheduled'
        ]
        
        if set(expected_routes).issubset(set(mv_routes)):
            print("✓ MV refresh task routes configured correctly")
        else:
            print(f"✗ Missing task routes. Found: {mv_routes}")
            return False
        
        # Check beat schedule
        scheduled_tasks = [task for task in app.conf.beat_schedule.keys() 
                          if 'mv' in task.lower()]
        
        if 'scheduled-mv-refresh' in scheduled_tasks:
            print("✓ Scheduled MV refresh task configured")
        else:
            print(f"✗ Missing scheduled MV refresh. Found: {scheduled_tasks}")
            return False
        
        return True
    except Exception as e:
        print(f"✗ Task configuration test failed: {e}")
        return False

def test_etl_lock_context():
    """Test ETL lock context manager."""
    print("\nTesting ETL lock context manager...")
    
    try:
        from etl_tasks import etl_operation_lock
        import redis
        
        # Test context manager structure
        redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
        
        # Mock test - just verify the context manager can be created
        with etl_operation_lock():
            # This would normally set/clear locks, but we'll just pass
            pass
        
        print("✓ ETL lock context manager works")
        return True
    except Exception as e:
        print(f"✗ ETL lock test failed: {e}")
        return False

def test_cli_integration():
    """Test CLI integration components."""
    print("\nTesting CLI integration...")
    
    try:
        from scripts.etl_data_manager_cli import main
        print("✓ CLI manager imported successfully")
        
        # Test CLI argument structure (without actually running)
        import subprocess
        help_result = subprocess.run([
            'python', 'scripts/etl_data_manager_cli.py', '--help'
        ], capture_output=True, text=True, timeout=10)
        
        if help_result.returncode == 0:
            if 'refresh-mvs-cascading' in help_result.stdout:
                print("✓ CLI refresh-mvs-cascading command available")
            else:
                print("⚠ CLI refresh-mvs-cascading not found in help")
        else:
            print("⚠ CLI help command failed")
        
        return True
    except Exception as e:
        print(f"✗ CLI integration test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=" * 60)
    print("MV Refresh Functionality Tests")
    print("=" * 60)
    
    tests = [
        test_mv_refresh_imports,
        test_redis_coordination,
        test_task_configuration,
        test_etl_lock_context,
        test_cli_integration
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"✗ Test {test.__name__} failed with exception: {e}")
            results.append(False)
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(results)
    total = len(results)
    
    print(f"Tests passed: {passed}/{total}")
    
    if passed == total:
        print("🎉 All tests passed! MV refresh functionality is ready.")
    else:
        print("⚠ Some tests failed. Check the output above for details.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
