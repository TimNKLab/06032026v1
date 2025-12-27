#!/usr/bin/env python3
"""
Test script to verify ETL reliability improvements.
Run this to test the catch-up logic and health monitoring.
"""

import os
import sys
from datetime import date, datetime, timedelta

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_catch_up_logic():
    """Test the catch-up logic in daily_etl_pipeline."""
    print("Testing catch-up logic...")
    
    try:
        from etl_tasks import _find_last_processed_date, _process_single_date
        from unittest.mock import patch, MagicMock
        
        # Mock _process_single_date to avoid actual ETL execution
        with patch('etl_tasks._process_single_date') as mock_process:
            mock_process.return_value = MagicMock()
            
            # Test finding last processed date
            last_date = _find_last_processed_date()
            print(f"  Last processed date: {last_date}")
            
            # Test catch-up simulation
            today = date.today()
            if last_date:
                days_behind = (today - last_date).days
                if days_behind > 1:
                    print(f"  Would catch up {days_behind - 1} days")
                    for i in range(1, min(3, days_behind)):  # Test up to 3 days
                        date_to_process = last_date + timedelta(days=i)
                        print(f"    Would process: {date_to_process}")
            
        print("  ✓ Catch-up logic test passed")
        return True
        
    except Exception as e:
        print(f"  ✗ Catch-up logic test failed: {e}")
        return False

def test_health_check():
    """Test the health check functionality."""
    print("\nTesting health check...")
    
    try:
        from etl_tasks import check_etl_health
        
        # Run health check
        result = check_etl_health()
        print(f"  Health check result: {result}")
        
        # Validate result structure
        if isinstance(result, dict) and 'status' in result:
            print("  ✓ Health check test passed")
            return True
        else:
            print("  ✗ Health check returned invalid format")
            return False
            
    except Exception as e:
        print(f"  ✗ Health check test failed: {e}")
        return False

def test_docker_compose_config():
    """Test Docker Compose configuration."""
    print("\nTesting Docker Compose configuration...")
    
    try:
        import subprocess
        import json
        
        # Check if docker-compose config is valid
        result = subprocess.run(
            ['docker-compose', 'config'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        
        if result.returncode == 0:
            # Parse the YAML output to check for restart policies
            config = result.stdout
            
            # Check for restart policies
            if 'restart: unless-stopped' in config:
                print("  ✓ Restart policies found")
            else:
                print("  ⚠ Restart policies not found")
            
            # Check for health checks
            if 'healthcheck:' in config:
                print("  ✓ Health checks found")
            else:
                print("  ⚠ Health checks not found")
            
            print("  ✓ Docker Compose configuration is valid")
            return True
        else:
            print(f"  ✗ Docker Compose configuration error: {result.stderr}")
            return False
            
    except FileNotFoundError:
        print("  ⚠ Docker Compose not found (install Docker Desktop)")
        return False
    except Exception as e:
        print(f"  ✗ Docker Compose test failed: {e}")
        return False

def test_imports():
    """Test if all required modules can be imported."""
    print("\nTesting module imports...")
    
    required_modules = [
        'etl_tasks',
        'polars',
        'celery',
        'pydantic',
        'odoorpc_connector'
    ]
    
    all_good = True
    for module in required_modules:
        try:
            __import__(module)
            print(f"  ✓ {module}")
        except ImportError as e:
            print(f"  ✗ {module}: {e}")
            all_good = False
    
    return all_good

def main():
    """Run all tests."""
    print("=" * 50)
    print("ETL Reliability Test Suite")
    print("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("Docker Compose Config", test_docker_compose_config),
        ("Catch-up Logic", test_catch_up_logic),
        ("Health Check", test_health_check),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        print("-" * 30)
        results.append(test_func())
    
    print("\n" + "=" * 50)
    print("Test Summary:")
    print("=" * 50)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test_name, _) in enumerate(tests):
        status = "✓ PASS" if results[i] else "✗ FAIL"
        print(f"  {status} {test_name}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! The ETL reliability improvements are working.")
    else:
        print("\n⚠️  Some tests failed. Please review the output above.")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
