#!/usr/bin/env python3
"""
Comprehensive test runner for the Arion Challenge project.

This script provides various test execution modes with detailed reporting,
coverage analysis, and performance metrics.
"""

import os
import sys
import argparse
import subprocess
import time
import json
from pathlib import Path
from typing import List, Dict, Any, Optional


class TestRunner:
    """Main test runner class with various execution modes."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.tests_dir = project_root / "tests"
        self.results = {
            "start_time": None,
            "end_time": None,
            "duration": 0,
            "total_tests": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": [],
            "test_files": {},
            "coverage": None
        }
    
    def run_command(self, cmd: List[str], capture_output: bool = True) -> subprocess.CompletedProcess:
        """Run a command and return the result."""
        print(f"Running: {' '.join(cmd)}")
        try:
            if capture_output:
                result = subprocess.run(
                    cmd, 
                    capture_output=True, 
                    text=True, 
                    cwd=self.project_root,
                    timeout=300  # 5 minute timeout
                )
            else:
                result = subprocess.run(
                    cmd,
                    cwd=self.project_root,
                    timeout=300
                )
            return result
        except subprocess.TimeoutExpired:
            print("❌ Command timed out after 5 minutes")
            raise
        except Exception as e:
            print(f"❌ Command failed: {e}")
            raise
    
    def discover_test_files(self) -> List[Path]:
        """Discover all test files in the tests directory."""
        test_files = []
        for pattern in ["test_*.py", "*_test.py"]:
            test_files.extend(self.tests_dir.glob(pattern))
        return sorted(test_files)
    
    def run_unit_tests(self, verbose: bool = False, pattern: Optional[str] = None) -> bool:
        """Run unit tests for individual components."""
        print("\n🧪 Running Unit Tests")
        print("=" * 50)
        
        test_files = [
            "test_lazy_iterator.py",
            "test_plugins.py", 
            "test_resource_manager.py"
        ]
        
        if pattern:
            test_files = [f for f in test_files if pattern in f]
        
        cmd = ["python", "-m", "pytest"] + [str(self.tests_dir / f) for f in test_files]
        if verbose:
            cmd.extend(["-v", "--tb=short"])
        else:
            cmd.append("-q")
        
        result = self.run_command(cmd, capture_output=False)
        success = result.returncode == 0
        
        if success:
            print("✅ Unit tests passed")
        else:
            print("❌ Unit tests failed")
        
        return success
    
    def run_integration_tests(self, verbose: bool = False) -> bool:
        """Run integration tests between components."""
        print("\n🔗 Running Integration Tests")
        print("=" * 50)
        
        cmd = ["python", "-m", "pytest", str(self.tests_dir / "test_integration.py")]
        if verbose:
            cmd.extend(["-v", "--tb=short"])
        else:
            cmd.append("-q")
        
        result = self.run_command(cmd, capture_output=False)
        success = result.returncode == 0
        
        if success:
            print("✅ Integration tests passed")
        else:
            print("❌ Integration tests failed")
        
        return success
    
    def run_performance_tests(self, verbose: bool = False) -> bool:
        """Run performance and stress tests."""
        print("\n⚡ Running Performance Tests")
        print("=" * 50)
        
        cmd = ["python", "-m", "pytest", str(self.tests_dir / "test_performance.py")]
        if verbose:
            cmd.extend(["-v", "--tb=short", "-s"])  # -s to see print statements
        else:
            cmd.extend(["-q", "-s"])
        
        result = self.run_command(cmd, capture_output=False)
        success = result.returncode == 0
        
        if success:
            print("✅ Performance tests passed")
        else:
            print("❌ Performance tests failed")
        
        return success
    
    def run_webhook_tests(self, verbose: bool = False) -> bool:
        """Run webhook and API tests."""
        print("\n🌐 Running Webhook Tests")
        print("=" * 50)
        
        cmd = ["python", "-m", "pytest", str(self.tests_dir / "test_webhook.py")]
        if verbose:
            cmd.extend(["-v", "--tb=short"])
        else:
            cmd.append("-q")
        
        result = self.run_command(cmd, capture_output=False)
        success = result.returncode == 0
        
        if success:
            print("✅ Webhook tests passed")
        else:
            print("❌ Webhook tests failed")
        
        return success
    
    def run_all_tests(self, verbose: bool = False, coverage: bool = False) -> bool:
        """Run all tests with optional coverage."""
        print("\n🚀 Running All Tests")
        print("=" * 50)
        
        cmd = ["python", "-m", "pytest", str(self.tests_dir)]
        
        if coverage:
            cmd = ["python", "-m", "pytest", "--cov=.", "--cov-report=html", "--cov-report=term", str(self.tests_dir)]
        
        if verbose:
            cmd.extend(["-v", "--tb=short"])
        else:
            cmd.append("-q")
        
        # Add markers for async tests
        cmd.extend(["-m", "not slow"])  # Skip slow tests by default
        
        start_time = time.time()
        result = self.run_command(cmd, capture_output=False)
        duration = time.time() - start_time
        
        success = result.returncode == 0
        
        print(f"\n⏱️  Total test duration: {duration:.2f} seconds")
        
        if success:
            print("✅ All tests passed")
        else:
            print("❌ Some tests failed")
        
        return success
    
    def run_fast_tests(self, verbose: bool = False) -> bool:
        """Run only fast tests (no performance/stress tests)."""
        print("\n⚡ Running Fast Tests Only")
        print("=" * 50)
        
        test_files = [
            "test_lazy_iterator.py",
            "test_plugins.py",
            "test_resource_manager.py"
        ]
        
        cmd = ["python", "-m", "pytest"] + [str(self.tests_dir / f) for f in test_files]
        if verbose:
            cmd.extend(["-v", "--tb=short"])
        else:
            cmd.append("-q")
        
        result = self.run_command(cmd, capture_output=False)
        success = result.returncode == 0
        
        if success:
            print("✅ Fast tests passed")
        else:
            print("❌ Fast tests failed")
        
        return success
    
    def run_with_coverage(self, pattern: Optional[str] = None) -> bool:
        """Run tests with coverage analysis."""
        print("\n📈 Running Tests with Coverage Analysis")
        print("=" * 50)
        
        cmd = [
            "python", "-m", "pytest",
            "--cov=.",
            "--cov-report=html:htmlcov",
            "--cov-report=term-missing",
            "--cov-report=json:coverage.json"
        ]
        
        if pattern:
            cmd.append(f"tests/*{pattern}*")
        else:
            cmd.append("tests/")
        
        cmd.extend(["-v", "--tb=short"])
        
        result = self.run_command(cmd, capture_output=False)
        success = result.returncode == 0
        
        # Try to read coverage results
        coverage_file = self.project_root / "coverage.json"
        if coverage_file.exists():
            try:
                with open(coverage_file) as f:
                    coverage_data = json.load(f)
                    total_coverage = coverage_data.get("totals", {}).get("percent_covered", 0)
                    print(f"\n📊 Total Coverage: {total_coverage:.1f}%")
                    
                    if total_coverage >= 80:
                        print("✅ Good coverage (≥80%)")
                    elif total_coverage >= 60:
                        print("⚠️  Moderate coverage (60-80%)")
                    else:
                        print("❌ Low coverage (<60%)")
            except Exception as e:
                print(f"⚠️  Could not read coverage data: {e}")
        
        if success:
            print("✅ Coverage analysis completed")
            print(f"📁 HTML coverage report: {self.project_root}/htmlcov/index.html")
        else:
            print("❌ Tests failed during coverage analysis")
        
        return success
    
    def check_test_environment(self) -> bool:
        """Check if the test environment is properly set up."""
        print("\n🔍 Checking Test Environment")
        print("=" * 50)
        
        checks = []
        
        # Check Python version
        python_version = sys.version_info
        if python_version >= (3, 8):
            print(f"✅ Python {python_version.major}.{python_version.minor}.{python_version.micro}")
            checks.append(True)
        else:
            print(f"❌ Python version too old: {python_version}")
            checks.append(False)
        
        # Check required packages
        required_packages = ["pytest", "aiohttp", "asyncio"]
        for package in required_packages:
            try:
                __import__(package)
                print(f"✅ {package} available")
                checks.append(True)
            except ImportError:
                print(f"❌ {package} not available")
                checks.append(False)
        
        # Check test files exist
        test_files = self.discover_test_files()
        if test_files:
            print(f"✅ Found {len(test_files)} test files")
            for test_file in test_files:
                print(f"   📄 {test_file.name}")
            checks.append(True)
        else:
            print("❌ No test files found")
            checks.append(False)
        
        # Check project structure
        required_dirs = ["etl_pipeline", "task_scheduler", "plugins", "tests"]
        for dir_name in required_dirs:
            dir_path = self.project_root / dir_name
            if dir_path.exists():
                print(f"✅ {dir_name}/ directory exists")
                checks.append(True)
            else:
                print(f"❌ {dir_name}/ directory missing")
                checks.append(False)
        
        all_good = all(checks)
        if all_good:
            print("\n✅ Test environment is ready")
        else:
            print("\n❌ Test environment has issues")
        
        return all_good
    
    def generate_test_report(self) -> None:
        """Generate a detailed test report."""
        print("\n📋 Generating Test Report")
        print("=" * 50)
        
        # Run pytest with JSON output
        cmd = [
            "python", "-m", "pytest", 
            "--json-report", "--json-report-file=test_report.json",
            str(self.tests_dir)
        ]
        
        try:
            result = self.run_command(cmd)
            
            report_file = self.project_root / "test_report.json"
            if report_file.exists():
                with open(report_file) as f:
                    report_data = json.load(f)
                
                print(f"📊 Test Summary:")
                print(f"   Total: {report_data.get('summary', {}).get('total', 0)}")
                print(f"   Passed: {report_data.get('summary', {}).get('passed', 0)}")
                print(f"   Failed: {report_data.get('summary', {}).get('failed', 0)}")
                print(f"   Skipped: {report_data.get('summary', {}).get('skipped', 0)}")
                print(f"   Duration: {report_data.get('duration', 0):.2f}s")
                
                print(f"\n📁 Detailed report: {report_file}")
            else:
                print("⚠️  Could not generate detailed report")
        except Exception as e:
            print(f"⚠️  Report generation failed: {e}")


def main():
    """Main entry point for the test runner."""
    parser = argparse.ArgumentParser(
        description="Comprehensive test runner for Arion Challenge",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --all                    # Run all tests
  %(prog)s --unit --verbose         # Run unit tests with verbose output
  %(prog)s --integration            # Run integration tests only
  %(prog)s --performance            # Run performance tests only
  %(prog)s --fast                   # Run only fast tests
  %(prog)s --coverage               # Run with coverage analysis
  %(prog)s --check                  # Check test environment
  %(prog)s --pattern lazy           # Run tests matching 'lazy'
  %(prog)s --report                 # Generate detailed test report
        """
    )
    
    # Test mode selection
    parser.add_argument("--all", action="store_true", help="Run all tests")
    parser.add_argument("--unit", action="store_true", help="Run unit tests only")
    parser.add_argument("--integration", action="store_true", help="Run integration tests only")
    parser.add_argument("--performance", action="store_true", help="Run performance tests only")
    parser.add_argument("--webhook", action="store_true", help="Run webhook tests only")
    parser.add_argument("--fast", action="store_true", help="Run fast tests only (no performance)")
    
    # Analysis options
    parser.add_argument("--coverage", action="store_true", help="Run with coverage analysis")
    parser.add_argument("--check", action="store_true", help="Check test environment setup")
    parser.add_argument("--report", action="store_true", help="Generate detailed test report")
    
    # Filtering and output options
    parser.add_argument("--pattern", help="Run tests matching this pattern")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--quiet", "-q", action="store_true", help="Quiet output")
    
    args = parser.parse_args()
    
    # Find project root
    current_dir = Path.cwd()
    project_root = current_dir
    
    # Look for tests directory or key project files
    while project_root != project_root.parent:
        if (project_root / "tests").exists() or (project_root / "etl_pipeline").exists():
            break
        project_root = project_root.parent
    else:
        print("❌ Could not find project root. Run from project directory.")
        sys.exit(1)
    
    runner = TestRunner(project_root)
    
    print("🧪 Arion Challenge Test Runner")
    print("=" * 50)
    print(f"📁 Project root: {project_root}")
    print(f"📁 Tests directory: {runner.tests_dir}")
    
    success = True
    
    try:
        if args.check:
            success = runner.check_test_environment()
        elif args.report:
            runner.generate_test_report()
        elif args.coverage:
            success = runner.run_with_coverage(args.pattern)
        elif args.unit:
            success = runner.run_unit_tests(args.verbose, args.pattern)
        elif args.integration:
            success = runner.run_integration_tests(args.verbose)
        elif args.performance:
            success = runner.run_performance_tests(args.verbose)
        elif args.webhook:
            success = runner.run_webhook_tests(args.verbose)
        elif args.fast:
            success = runner.run_fast_tests(args.verbose)
        elif args.all:
            success = runner.run_all_tests(args.verbose, args.coverage)
        else:
            # Default: run fast tests
            print("ℹ️  No specific test mode selected, running fast tests")
            success = runner.run_fast_tests(args.verbose)
    
    except KeyboardInterrupt:
        print("\n⏹️  Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Test runner failed: {e}")
        sys.exit(1)
    
    if success:
        print("\n🎉 All tests completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Some tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()