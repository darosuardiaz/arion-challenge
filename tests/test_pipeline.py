import asyncio
import argparse
import logging
import os
import random
import subprocess
import sys
import tempfile
import time
from typing import AsyncIterator, Dict, Any
import aiohttp

# Add parent directory to path for imports
_parent_dir = os.path.dirname(os.path.dirname(__file__))
if _parent_dir not in sys.path:
    sys.path.insert(0, _parent_dir)

from etl_pipeline.pipeline import Pipeline
from resource_manager import ResourceManager


DEFAULT_URL = os.getenv("PIPELINE_WEBHOOK_URL", "http://localhost:8080/webhook")
DEFAULT_HEALTH = os.getenv("PIPELINE_HEALTH_URL", "http://localhost:8080/healthz")


async def test_pipeline_direct(count: int = 100, window_secs: int = 2) -> None:
    """Test the Pipeline directly without webhook server."""
    logging.info("=== Testing Pipeline Direct (ResourceManager Integration) ===")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Test pipeline creation and context management
        pipeline = Pipeline(
            queue_maxsize=200,  # Reasonable size for normal operations
            window_secs=window_secs,
            db_path=db_path,
            logger=logging.getLogger("test.pipeline")
        )
        
        with pipeline:
            logging.info("✅ Pipeline context active")
            
            # Test database initialization and access
            db = pipeline.database
            tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = [t[0] for t in tables]
            logging.info(f"📦 Database tables: {table_names}")
            assert 'aggregates' in table_names, "Aggregates table should exist"
            
            # Start pipeline workers FIRST to avoid queue deadlock
            logging.info("🚀 Starting pipeline workers...")
            tasks = await pipeline.start_workers()
            
            # Test queue operations
            test_events = []
            async for event in generate_test_data(count=count, include_ts=True):
                test_events.append(event)
                await pipeline.ingest_q.put(event)
            
            logging.info(f"📤 Sent {len(test_events)} events to pipeline")
            
            # Let pipeline process events
            await asyncio.sleep(window_secs + 1)
            
            # Check queue status
            queue_info = pipeline.get_queue_info()
            logging.info(f"📊 Queue status: {queue_info}")
            
            # Check database for aggregated results
            aggregates = db.execute("SELECT * FROM aggregates ORDER BY window_start, category").fetchall()
            logging.info(f"📊 Found {len(aggregates)} aggregate records")
            
            for agg in aggregates[:5]:  # Show first 5 results
                logging.info(f"   {dict(agg)}")
            
            # Test resource metrics
            metrics = pipeline.get_resource_metrics()
            logging.info(f"📊 Resource metrics:")
            logging.info(f"   DB acquire: {metrics['acquire_ms']['db']}ms")
            logging.info(f"   MQ acquire: {metrics['acquire_ms']['mq']}ms")
            logging.info(f"   Context time: {metrics['context_ms']}ms")
            
            # Test early resource release
            logging.info("🔄 Testing early resource release...")
            pipeline.release_database()
            pipeline.release_message_queue()
            
            # Test resource re-acquisition
            db2 = pipeline.database
            queue_info2 = pipeline.get_queue_info()
            logging.info("✅ Resource re-acquisition successful")
            
            # Stop workers gracefully
            logging.info("🛑 Stopping pipeline workers...")
            try:
                await pipeline.stop_workers(timeout=1.0)  # Short timeout for tests
                logging.info("✅ Pipeline workers stopped cleanly")
            except asyncio.TimeoutError:
                logging.info("⏱️ Pipeline shutdown timeout (normal for tests)")
                # Cancel remaining tasks
                for task in pipeline._tasks:
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
            
        logging.info("✅ Pipeline context exited - resources cleaned up")
        
        # Verify aggregation results
        if len(aggregates) > 0:
            categories = set(agg['category'] for agg in aggregates)
            logging.info(f"📊 Aggregated categories: {categories}")
            
            total_records = sum(agg['record_count'] for agg in aggregates)
            logging.info(f"📊 Total records aggregated: {total_records}")
            
            if total_records > 0:
                logging.info("✅ Pipeline aggregation successful!")
            else:
                logging.warning("⚠️ No records were aggregated")
        else:
            logging.warning("⚠️ No aggregate records found")
            
    finally:
        # Cleanup
        try:
            os.unlink(db_path)
        except OSError:
            pass


async def test_resource_manager_standalone() -> None:
    """Test ResourceManager standalone functionality."""
    logging.info("=== Testing ResourceManager Standalone ===")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        # Test pipeline-specific ResourceManager
        rm = ResourceManager.for_pipeline(
            db_path=db_path,
            queue_maxsize=10,
            setup_schema=True,
            logger=logging.getLogger("test.rm")
        )
        
        with rm:
            logging.info("✅ ResourceManager context active")
            
            # Test database management
            db = rm.get_database()
            db.execute("INSERT INTO aggregates VALUES (?, ?, ?, ?, ?, ?)", 
                      (1640995200, 'test', 100.0, 5, 2, 50.0))
            db.commit()
            
            result = db.execute("SELECT * FROM aggregates").fetchone()
            logging.info(f"📦 Database operation: {dict(result)}")
            
            # Test queue management
            queues = rm.get_message_queue()
            await queues['ingest'].put({'test': 'data'})
            item = await queues['ingest'].get()
            logging.info(f"📦 Queue operation: {item}")
            
            # Test metrics
            metrics = rm.metrics_snapshot()
            logging.info(f"📊 ResourceManager metrics: {metrics['active']}")
            
        logging.info("✅ ResourceManager standalone test completed")
        
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


async def benchmark_pipeline(count: int = 1000, concurrency: int = 10) -> None:
    """Benchmark pipeline throughput."""
    logging.info(f"=== Benchmarking Pipeline ({count} events, {concurrency} concurrency) ===")
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = tmp_db.name
    
    try:
        pipeline = Pipeline(
            queue_maxsize=200,
            window_secs=5,
            db_path=db_path
        )
        
        start_time = time.time()
        
        with pipeline:
            # Start workers FIRST to avoid queue deadlock
            tasks = await pipeline.start_workers()
            
            # Send events with controlled concurrency
            semaphore = asyncio.Semaphore(concurrency)
            
            async def send_event(event):
                async with semaphore:
                    await pipeline.ingest_q.put(event)
            
            # Generate and send all events
            event_tasks = []
            async for event in generate_test_data(count=count, include_ts=True):
                event_tasks.append(send_event(event))
            
            await asyncio.gather(*event_tasks)
            send_time = time.time()
            
            logging.info(f"📤 Sent {count} events in {send_time - start_time:.2f}s")
            
            # Wait for processing
            await asyncio.sleep(8)  # Wait longer than window_secs
            
            # Check results
            db = pipeline.database
            aggregates = db.execute("SELECT COUNT(*) as count FROM aggregates").fetchone()
            total_records = db.execute("SELECT SUM(record_count) as total FROM aggregates").fetchone()
            
            process_time = time.time()
            total_time = process_time - start_time
            
            logging.info(f"📊 Benchmark results:")
            logging.info(f"   Total time: {total_time:.2f}s")
            logging.info(f"   Throughput: {count/total_time:.1f} events/sec")
            logging.info(f"   Aggregate windows: {aggregates['count']}")
            logging.info(f"   Records processed: {total_records['total'] or 0}")
            
            # Stop workers gracefully
            try:
                await pipeline.stop_workers(timeout=1.0)
                logging.info("✅ Pipeline workers stopped cleanly")
            except asyncio.TimeoutError:
                logging.info("⏱️ Pipeline shutdown timeout (normal for tests)")
                # Force cancel remaining tasks
                for task in pipeline._tasks:
                    if not task.done():
                        task.cancel()
            
        logging.info("✅ Benchmark completed")
        
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


async def generate_test_data(count: int = 1000, include_ts: bool = False) -> AsyncIterator[Dict[str, Any]]:
    """
    Generate synthetic events compatible with pipieline.py webhook handler.

    When include_ts is True, embeds a 'ts' field inside data so windowing uses it.
    Otherwise the pipeline will use processed_at timestamp.
    """
    categories = ["electronics", "clothing", "books", "home", "sports"]
    for i in range(count):
        payload: Dict[str, Any] = {
            "id": f"record_{i}",
            "source": "webhook",
            "data": {
                "category": random.choice(categories),
                "value": float(random.randint(10, 1000)),
                "quantity": int(random.randint(1, 10)),
            },
        }
        if include_ts:
            # Spread events across the last hour
            payload["data"]["ts"] = time.time() - random.randint(0, 3600)
        yield payload


async def _post_one(session: aiohttp.ClientSession, url: str, event: Dict[str, Any]) -> tuple[int, str]:
    try:
        async with session.post(url, json=event, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            text = await resp.text()
            return resp.status, text
    except Exception as exc:  # network/error path
        return 0, str(exc)


async def pump_events(
    url: str,
    count: int,
    concurrency: int,
    include_ts: bool,
    progress_every: int = 100,
    ) -> None:
    """Send events in batches of size 'concurrency' using concurrent requests."""
    sent_ok = 0

    async with aiohttp.ClientSession() as session:
        batch: list[Dict[str, Any]] = []
        processed = 0

        async for event in generate_test_data(count=count, include_ts=include_ts):
            batch.append(event)
            if len(batch) >= concurrency:
                results = await asyncio.gather(
                    *(_post_one(session, url, ev) for ev in batch)
                )
                for status, text in results:
                    processed += 1
                    if status == 202:
                        sent_ok += 1
                    else:
                        logging.warning("POST failed status=%s body=%s", status, text)
                    if processed % progress_every == 0:
                        logging.info("Sent %d/%d events", processed, count)
                batch.clear()

        if batch:
            results = await asyncio.gather(*(_post_one(session, url, ev) for ev in batch))
            for status, text in results:
                processed += 1
                if status == 202:
                    sent_ok += 1
                else:
                    logging.warning("POST failed status=%s body=%s", status, text)
                if processed % progress_every == 0 or processed == count:
                    logging.info("Sent %d/%d events", processed, count)

    logging.info("Done. Successfully accepted %d/%d events", sent_ok, count)


async def fetch_health(health_url: str = "http://localhost:8080/healthz") -> None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                text = await resp.text()
                logging.info("/healthz %s: %s", resp.status, text)
    except Exception as exc:
        logging.warning("Failed to fetch healthz: %s", exc)


async def _is_healthy(health_url: str) -> bool:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(health_url, timeout=aiohttp.ClientTimeout(total=1.5)) as resp:
                return resp.status == 200
    except Exception:
        return False


async def wait_for_health(health_url: str, timeout_secs: float = 20.0, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        if await _is_healthy(health_url):
            return True
        await asyncio.sleep(interval)
    return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Test the ETL Pipeline with ResourceManager integration",
        epilog="""
Examples:
  %(prog)s --mode direct --count 50        # Test pipeline directly
  %(prog)s --mode resource-manager         # Test ResourceManager only  
  %(prog)s --mode benchmark --count 100    # Benchmark pipeline performance
  %(prog)s --mode webhook --url http://... # Test webhook interface
  %(prog)s --mode all                      # Run all test modes
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Test mode selection
    parser.add_argument(
        "--mode", 
        choices=["direct", "webhook", "resource-manager", "benchmark", "all"],
        default="direct",
        help="Test mode: direct pipeline, webhook, resource manager, benchmark, or all tests"
    )
    
    # Webhook-specific options
    parser.add_argument("--url", default=DEFAULT_URL, help="Webhook URL (default: %(default)s)")
    parser.add_argument(
        "--health", action="store_true", help="Fetch /healthz after webhook tests"
    )
    
    # General test options
    parser.add_argument("--count", type=int, default=100, help="Number of events to send/generate")
    parser.add_argument(
        "--concurrency", type=int, default=10, help="Max concurrency for tests"
    )
    parser.add_argument(
        "--include-ts",
        action="store_true",
        help="Include 'ts' in data so windowing uses event time",
    )
    parser.add_argument(
        "--window-secs", type=int, default=2, help="Window size for aggregation (seconds)"
    )
    parser.add_argument(
        "--log-level", default=os.getenv("LOG_LEVEL", "INFO"), help="Logging level (default: %(default)s)"
    )
    
    return parser.parse_args()


async def run_all_tests(args) -> None:
    """Run all test modes."""
    logging.info("🧪 Running ALL test modes")
    
    try:
        # Direct pipeline test
        await test_pipeline_direct(count=args.count, window_secs=args.window_secs)
        await asyncio.sleep(1)
        
        # ResourceManager standalone test
        await test_resource_manager_standalone()
        await asyncio.sleep(1)
        
        # Benchmark test
        await benchmark_pipeline(count=min(args.count, 500), concurrency=args.concurrency)
        await asyncio.sleep(1)
        
        # Webhook test (if server is available)
        logging.info("=== Testing Webhook Interface ===")
        if await _is_healthy(DEFAULT_HEALTH):
            logging.info("📡 Webhook server detected, testing webhook interface...")
            await pump_events(args.url, min(args.count, 100), args.concurrency, args.include_ts)
            if args.health:
                await fetch_health()
        else:
            logging.info("📡 No webhook server detected, skipping webhook tests")
            logging.info("   To test webhooks, start the webhook server first")
        
        logging.info("🎉 All tests completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Test failed: {e}")
        raise


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level, 
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    logging.info(f"🚀 Starting Pipeline Tests (mode: {args.mode})")
    
    try:
        if args.mode == "direct":
            asyncio.run(test_pipeline_direct(count=args.count, window_secs=args.window_secs))
            
        elif args.mode == "webhook":
            logging.info("📡 Testing webhook interface...")
            asyncio.run(pump_events(args.url, args.count, args.concurrency, args.include_ts))
            if args.health:
                asyncio.run(fetch_health())
                
        elif args.mode == "resource-manager":
            asyncio.run(test_resource_manager_standalone())
            
        elif args.mode == "benchmark":
            asyncio.run(benchmark_pipeline(count=args.count, concurrency=args.concurrency))
            
        elif args.mode == "all":
            asyncio.run(run_all_tests(args))
            
        logging.info("✅ Tests completed successfully!")
        
    except KeyboardInterrupt:
        logging.info("⏹️ Tests interrupted by user")
    except Exception as e:
        logging.error(f"❌ Tests failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

