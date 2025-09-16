import asyncio
import argparse
import logging
import os
import random
import subprocess
import sys
import time
import json
import sqlite3
from typing import AsyncIterator, Dict, Any, List
import aiohttp

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etl_pipeline.pipeline import Pipeline
from task_scheduler.task_scheduler import TaskScheduler
from task_scheduler.task_types import Task, TaskPriority
from lazy_iterator import LazyIterator
from resource_manager import ResourceManager
from plugins import load_plugins, get_worker, get_all_plugins

DEFAULT_URL = os.getenv("PIPELINE_WEBHOOK_URL", "http://localhost:8080/webhook")
DEFAULT_HEALTH = os.getenv("PIPELINE_HEALTH_URL", "http://localhost:8080/healthz")


async def generate_test_data(count: int = 1000, include_ts: bool = False) -> AsyncIterator[Dict[str, Any]]:
    """ Generate synthetic events compatible with pipeline webhook handler """
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


def generate_large_dataset(size: int = 10000) -> List[Dict[str, Any]]:
    """Generate a large dataset for lazy iterator demonstration"""
    categories = ["electronics", "clothing", "books", "home", "sports", "automotive", "furniture"]
    regions = ["north", "south", "east", "west", "central"]
    
    data = []
    for i in range(size):
        data.append({
            "id": i,
            "category": random.choice(categories),
            "region": random.choice(regions),
            "value": random.uniform(10.0, 2000.0),
            "quantity": random.randint(1, 50),
            "timestamp": time.time() - random.randint(0, 86400),  # Last 24 hours
            "customer_id": f"customer_{random.randint(1, 1000)}",
            "product_id": f"product_{random.randint(1, 500)}"
        })
    return data


async def demo_etl_pipeline():
    """Demonstrate ETL pipeline capabilities"""
    print("\n" + "="*60)
    print("🚀 ETL PIPELINE DEMONSTRATION")
    print("="*60)
    
    # Create pipeline with resource management
    pipeline = Pipeline(
        queue_maxsize=100,
        window_secs=5,
        db_path="demo_aggregates.db",
        logger=logging.getLogger("demo.pipeline")
    )
    
    with pipeline:
        print("✅ Pipeline context manager activated - resources managed automatically")
        
        # Start workers
        print("🔄 Starting pipeline workers...")
        tasks = await pipeline.start_workers()
        print(f"✅ Started {len(tasks)} worker tasks")
        
        # Generate and send test events
        print("📊 Generating and sending test events...")
        test_events = [
            {"data": {"category": "electronics", "value": 150.0, "quantity": 2, "ts": time.time()}},
            {"data": {"category": "clothing", "value": 75.0, "quantity": 1, "ts": time.time()}},
            {"data": {"category": "books", "value": 25.0, "quantity": 3, "ts": time.time()}},
            {"data": {"category": "electronics", "value": 200.0, "quantity": 1, "ts": time.time()}},
            {"data": {"category": "home", "value": 300.0, "quantity": 2, "ts": time.time()}},
        ]
        
        for event in test_events:
            await pipeline.ingest_q.put(event)
        
        print(f"✅ Sent {len(test_events)} events to pipeline")
        
        # Let pipeline process
        print("⏳ Processing events (5 seconds)...")
        await asyncio.sleep(5)
        
        # Show queue status
        queue_info = pipeline.get_queue_info()
        print(f"📈 Queue status: {queue_info}")
        
        # Show resource metrics
        resource_metrics = pipeline.get_resource_metrics()
        print(f"📊 Resource metrics: {resource_metrics}")
        
        # Query aggregated results
        print("🔍 Querying aggregated results from database...")
        with sqlite3.connect("demo_aggregates.db") as conn:
            cursor = conn.execute("SELECT * FROM aggregates ORDER BY window_start DESC, category")
            results = cursor.fetchall()
            
            if results:
                print("📋 Aggregated Results:")
                for row in results:
                    print(f"   Window: {row[0]}, Category: {row[1]}, Total Value: {row[2]}, Count: {row[5]}")
            else:
                print("   No aggregated results yet")
        
        # Stop workers
        print("🛑 Stopping pipeline workers...")
        await pipeline.stop_workers()
        print("✅ Pipeline workers stopped")
    
    print("✅ Pipeline context exited - all resources cleaned up automatically")


# Task functions for scheduler demo (must be at module level for pickling)
def cpu_intensive_task(data):
    """Simulate CPU intensive work"""
    result = 0
    for i in range(100000):
        result += i * data
    return {"result": result, "processed_at": time.time()}


def io_task(filename):
    """Simulate I/O work"""
    time.sleep(0.1)  # Simulate I/O delay
    return {"filename": filename, "status": "processed", "timestamp": time.time()}


def data_analysis_task(data_list):
    """Simulate data analysis"""
    if not data_list:
        return {"analysis": "no_data"}
    
    total = sum(data_list)
    avg = total / len(data_list)
    return {
        "total": total,
        "average": avg,
        "count": len(data_list),
        "max": max(data_list),
        "min": min(data_list)
    }


async def demo_task_scheduler():
    """Demonstrate task scheduler capabilities"""
    print("\n" + "="*60)
    print("⚙️ TASK SCHEDULER DEMONSTRATION")
    print("="*60)
    
    # Create task scheduler
    scheduler = TaskScheduler(max_workers=3)
    
    print("🚀 Starting task scheduler...")
    await scheduler.start()
    print("✅ Task scheduler started")
    
    try:
        
        # Define task configurations
        task_configs = [
            {
                "name": "critical_task_1",
                "func": cpu_intensive_task,
                "args": (1000,),
                "priority": TaskPriority.CRITICAL,
                "timeout": 10.0
            },
            {
                "name": "high_priority_io",
                "func": io_task,
                "args": ("important_file.txt",),
                "priority": TaskPriority.HIGH,
                "timeout": 5.0
            },
            {
                "name": "normal_analysis",
                "func": data_analysis_task,
                "args": ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10],),
                "priority": TaskPriority.NORMAL,
                "timeout": 3.0
            },
            {
                "name": "low_priority_batch",
                "func": cpu_intensive_task,
                "args": (500,),
                "priority": TaskPriority.LOW,
                "timeout": 15.0
            },
            {
                "name": "another_high_task",
                "func": data_analysis_task,
                "args": ([100, 200, 300, 400, 500],),
                "priority": TaskPriority.HIGH,
                "timeout": 2.0
            }
        ]
        
        print(f"📋 Created {len(task_configs)} task configurations with different priorities")
        
        # Schedule all tasks
        print("🚀 Scheduling tasks...")
        task_ids = []
        for config in task_configs:
            task_id = await scheduler.submit_task(
                func=config["func"],
                args=config["args"],
                priority=config["priority"],
                timeout=config["timeout"]
            )
            task_ids.append((task_id, config["name"]))
            print(f"   ✅ Scheduled {config['name']} (ID: {task_id}) with priority {config['priority'].name}")
        
        # Wait for all tasks to complete
        print("⏳ Waiting for tasks to complete...")
        completed_count = 0
        max_wait_time = 30  # Maximum wait time in seconds
        start_time = time.time()
        
        while completed_count < len(task_ids) and (time.time() - start_time) < max_wait_time:
            for task_id, task_name in task_ids:
                status = await scheduler.get_task_status(task_id)
                if status and status.name in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"]:
                    result = await scheduler.get_task_result(task_id)
                    if result:
                        print(f"   ✅ Completed {task_name}: {result.status.name}")
                        if result.result:
                            print(f"      Output: {result.result}")
                        completed_count += 1
                    else:
                        print(f"   ❌ Task {task_name} completed but no result available")
                        completed_count += 1
            
            if completed_count < len(task_ids):
                await asyncio.sleep(0.5)  # Wait a bit before checking again
        
        if completed_count < len(task_ids):
            print(f"   ⚠️ Only {completed_count}/{len(task_ids)} tasks completed within timeout")
        
        # Show scheduler statistics
        stats = await scheduler.get_statistics()
        print(f"📊 Scheduler Statistics:")
        print(f"   Total tasks: {stats.get('total_tasks', 0)}")
        print(f"   Completed: {stats.get('completed_tasks', 0)}")
        print(f"   Failed: {stats.get('failed_tasks', 0)}")
        print(f"   Active workers: {stats.get('active_workers', 0)}")
    
    finally:
        print("🛑 Stopping task scheduler...")
        await scheduler.stop()
        print("✅ Task scheduler stopped - all workers cleaned up")


def demo_lazy_iterator():
    """Demonstrate lazy iterator capabilities"""
    print("\n" + "="*60)
    print("🔄 LAZY ITERATOR DEMONSTRATION")
    print("="*60)
    
    # Generate large dataset
    print("📊 Generating large dataset (10,000 records)...")
    large_dataset = generate_large_dataset(10000)
    print(f"✅ Generated {len(large_dataset)} records")
    
    # Create lazy iterator pipeline
    print("🔗 Creating lazy iterator pipeline...")
    lazy_pipeline = LazyIterator(large_dataset)
    
    # Complex data processing pipeline
    print("⚙️ Building complex data processing pipeline...")
    processed_data = (lazy_pipeline
        .filter(lambda x: x['value'] > 100)  # Filter high-value items
        .filter(lambda x: x['category'] in ['electronics', 'automotive'])  # Specific categories
        .map(lambda x: {**x, 'total_value': x['value'] * x['quantity']})  # Calculate total value
        .filter(lambda x: x['total_value'] > 500)  # High total value
        .map(lambda x: {**x, 'region_code': x['region'][:2].upper()})  # Add region code
        .take(20)  # Limit to 20 results
        .chunk(5)  # Group into chunks of 5
    )
    
    print("✅ Lazy pipeline created (no processing yet - memory efficient!)")
    
    # Execute the pipeline
    print("🚀 Executing lazy pipeline...")
    start_time = time.time()
    
    chunk_count = 0
    total_processed = 0
    
    for chunk in processed_data:
        chunk_count += 1
        total_processed += len(chunk)
        print(f"   Chunk {chunk_count}: {len(chunk)} items")
        
        # Show sample data from first chunk
        if chunk_count == 1:
            print("   Sample data from first chunk:")
            for item in chunk[:2]:  # Show first 2 items
                print(f"     {item['category']} - ${item['total_value']:.2f} ({item['region_code']})")
    
    end_time = time.time()
    
    print(f"✅ Pipeline execution completed:")
    print(f"   Processed: {total_processed} items in {chunk_count} chunks")
    print(f"   Execution time: {end_time - start_time:.3f} seconds")
    print(f"   Memory efficient: Only processed what was needed!")
    
    # Demonstrate reduce operation
    print("\n🔢 Demonstrating reduce operation...")
    numbers = LazyIterator(range(1, 1001))
    sum_of_squares = (numbers
        .filter(lambda x: x % 2 == 0)  # Even numbers only
        .map(lambda x: x * x)  # Square them
        .reduce(lambda acc, x: acc + x, 0)  # Sum them up
    )
    print(f"   Sum of squares of even numbers 1-1000: {sum_of_squares}")


async def demo_plugin_system():
    """Demonstrate plugin system capabilities"""
    print("\n" + "="*60)
    print("🔌 PLUGIN SYSTEM DEMONSTRATION")
    print("="*60)
    
    # Load plugins
    print("📦 Loading plugins...")
    load_plugins()
    
    # Show available plugins
    all_plugins = get_all_plugins()
    print(f"✅ Loaded {len(all_plugins)} plugins:")
    for plugin in all_plugins:
        print(f"   - {plugin.__name__}")
    
    # Create plugin instances
    print("\n🏭 Creating plugin worker instances...")
    data_processor = get_worker("data_processor", worker_id="demo_processor")
    analytics_worker = get_worker("analytics_worker", worker_id="demo_analytics")
    
    print(f"✅ Created workers:")
    print(f"   - {data_processor.name} (ID: {data_processor.worker_id})")
    print(f"   - {analytics_worker.name} (ID: {analytics_worker.worker_id})")
    
    # Test data processing plugin
    print("\n🔄 Testing data processing plugin...")
    test_data = {
        "category": "electronics",
        "value": 150.0,
        "quantity": 2,
        "description": "laptop"
    }
    
    # Create a mock task for the processor
    class MockTask:
        def __init__(self, task_id, args, kwargs):
            self.id = task_id
            self.args = args
            self.kwargs = kwargs
    
    # Test different operations
    operations = ["transform", "validate", "aggregate"]
    for operation in operations:
        task = MockTask(f"task_{operation}", (test_data,), {"operation": operation})
        result = data_processor.execute_task(task)
        print(f"   {operation.capitalize()}: {result}")
    
    # Test analytics plugin
    print("\n📊 Testing analytics plugin...")
    analytics_data = [
        {"value": 100, "quantity": 1},
        {"value": 200, "quantity": 2},
        {"value": 150, "quantity": 3},
        {"value": 300, "quantity": 1},
        {"value": 250, "quantity": 2}
    ]
    
    analysis_types = ["summary", "trends", "correlations", "basic_stats"]
    for analysis_type in analysis_types:
        task = MockTask(f"analytics_{analysis_type}", (analytics_data,), {"analysis_type": analysis_type})
        result = analytics_worker.execute_task(task)
        print(f"   {analysis_type.capitalize()}: {result}")
    
    print("✅ Plugin system demonstration completed")


async def demo_resource_manager():
    """Demonstrate resource manager capabilities"""
    print("\n" + "="*60)
    print("🛠️ RESOURCE MANAGER DEMONSTRATION")
    print("="*60)
    
    # Create resource manager
    print("🏗️ Creating resource manager...")
    manager = ResourceManager.for_pipeline(
        db_path="demo_resources.db",
        queue_maxsize=50,
        setup_schema=True,
        logger=logging.getLogger("demo.resources")
    )
    
    with manager:
        print("✅ Resource manager context activated")
        
        # Demonstrate database operations
        print("🗄️ Testing database operations...")
        db = manager.get_database()
        cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        print(f"   Available tables: {[table[0] for table in tables]}")
        
        # Insert some test data
        test_data = [
            (int(time.time()), "test_category", 100.0, 5, 1, 100.0),
            (int(time.time()), "demo_category", 200.0, 3, 1, 200.0)
        ]
        
        cursor.executemany("""
            INSERT OR REPLACE INTO aggregates 
            (window_start, category, total_value, total_quantity, record_count, avg_value)
            VALUES (?, ?, ?, ?, ?, ?)
        """, test_data)
        db.commit()
        print("   ✅ Inserted test data")
        
        # Query the data
        cursor = db.execute("SELECT * FROM aggregates")
        results = cursor.fetchall()
        print(f"   📊 Retrieved {len(results)} records from database")
        
        # Demonstrate queue operations
        print("\n📬 Testing queue operations...")
        queues = manager.get_message_queue()
        ingest_queue = queues["ingest"]
        print(f"   Queue maxsize: {ingest_queue.maxsize}")
        print(f"   Current queue size: {ingest_queue.qsize()}")
        
        # Add some items to queue
        test_events = [
            {"data": {"category": "queue_test", "value": 50.0, "quantity": 1}},
            {"data": {"category": "queue_test", "value": 75.0, "quantity": 2}}
        ]
        
        for event in test_events:
            await ingest_queue.put(event)
        
        print(f"   ✅ Added {len(test_events)} events to queue")
        print(f"   Queue size after adding: {ingest_queue.qsize()}")
        
        # Get resource metrics
        print("\n📈 Resource metrics:")
        metrics = manager.metrics_snapshot()
        for key, value in metrics.items():
            print(f"   {key}: {value}")
    
    print("✅ Resource manager context exited - all resources cleaned up automatically")


async def demo_webhook_api():
    """Demonstrate webhook API capabilities"""
    print("\n" + "="*60)
    print("🌐 WEBHOOK API DEMONSTRATION")
    print("="*60)
    
    # Start webhook server
    print("🚀 Starting webhook server...")
    server_path = os.path.join(os.path.dirname(__file__), "etl_pipeline", "webhook.py")
    server_proc = subprocess.Popen(
        [sys.executable, "-u", server_path],
        stdout=None,
        stderr=None,
        cwd=os.path.dirname(server_path),
        start_new_session=True,
        env=os.environ.copy(),
    )
    
    # Wait for server to be healthy
    health_url = "http://localhost:8080/healthz"
    if not await wait_for_health(health_url):
        print("❌ Server failed to become healthy")
        return
    
    print("✅ Webhook server is healthy")
    
    # Test webhook endpoints
    async with aiohttp.ClientSession() as session:
        # Test health endpoint
        print("\n🏥 Testing health endpoint...")
        async with session.get(health_url) as resp:
            health_data = await resp.json()
            print(f"   Health status: {resp.status}")
            print(f"   Response: {health_data}")
        
        # Test metrics endpoint
        print("\n📊 Testing metrics endpoint...")
        async with session.get("http://localhost:8080/metrics") as resp:
            if resp.status == 200:
                metrics_data = await resp.json()
                print(f"   Metrics: {metrics_data}")
            else:
                print(f"   Metrics endpoint returned: {resp.status}")
        
        # Test webhook data ingestion
        print("\n📤 Testing webhook data ingestion...")
        test_payloads = [
            {"data": {"category": "api_test", "value": 100.0, "quantity": 1}},
            {"data": {"category": "api_test", "value": 200.0, "quantity": 2}},
            {"data": {"category": "api_test", "value": 150.0, "quantity": 3}}
        ]
        
        for i, payload in enumerate(test_payloads):
            async with session.post("http://localhost:8080/webhook", json=payload) as resp:
                print(f"   Payload {i+1}: Status {resp.status}")
                if resp.status != 202:
                    text = await resp.text()
                    print(f"      Error: {text}")
        
        # Test aggregates endpoint
        print("\n📋 Testing aggregates endpoint...")
        async with session.get("http://localhost:8080/aggregates") as resp:
            if resp.status == 200:
                aggregates_data = await resp.json()
                print(f"   Aggregates: {aggregates_data}")
            else:
                print(f"   Aggregates endpoint returned: {resp.status}")
    
    # Stop server
    print("\n🛑 Stopping webhook server...")
    if server_proc.poll() is None:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=5)
            print("✅ Server stopped gracefully")
        except subprocess.TimeoutExpired:
            server_proc.kill()
            print("⚠️ Server killed after timeout")


async def demo_performance_monitoring():
    """Demonstrate performance monitoring capabilities"""
    print("\n" + "="*60)
    print("⚡ PERFORMANCE MONITORING DEMONSTRATION")
    print("="*60)
    
    # Create pipeline for performance testing
    pipeline = Pipeline(
        queue_maxsize=1000,
        window_secs=2,
        db_path="perf_demo.db",
        logger=logging.getLogger("demo.performance")
    )
    
    with pipeline:
        print("🚀 Starting performance test pipeline...")
        tasks = await pipeline.start_workers()
        
        # Generate and send events rapidly
        print("📊 Generating high-volume test data...")
        start_time = time.time()
        event_count = 1000
        
        # Send events in batches
        batch_size = 50
        for i in range(0, event_count, batch_size):
            batch = []
            for j in range(batch_size):
                event = {
                    "data": {
                        "category": random.choice(["perf_test", "load_test", "stress_test"]),
                        "value": random.uniform(10.0, 1000.0),
                        "quantity": random.randint(1, 10),
                        "ts": time.time()
                    }
                }
                batch.append(event)
            
            # Send batch
            for event in batch:
                await pipeline.ingest_q.put(event)
            
            if i % 200 == 0:
                print(f"   Sent {i + batch_size} events...")
        
        end_time = time.time()
        send_duration = end_time - start_time
        
        print(f"✅ Sent {event_count} events in {send_duration:.3f} seconds")
        print(f"   Rate: {event_count / send_duration:.0f} events/second")
        
        # Let pipeline process
        print("⏳ Processing events (10 seconds)...")
        await asyncio.sleep(10)
        
        # Show performance metrics
        print("\n📈 Performance Metrics:")
        queue_info = pipeline.get_queue_info()
        resource_metrics = pipeline.get_resource_metrics()
        
        print(f"   Queue status: {queue_info}")
        print(f"   Resource metrics: {resource_metrics}")
        
        # Check database performance
        with sqlite3.connect("perf_demo.db") as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM aggregates")
            agg_count = cursor.fetchone()[0]
            print(f"   Aggregated records: {agg_count}")
            
            cursor = conn.execute("SELECT category, COUNT(*) as count FROM aggregates GROUP BY category")
            category_stats = cursor.fetchall()
            print("   Category distribution:")
            for category, count in category_stats:
                print(f"     {category}: {count} aggregations")
        
        await pipeline.stop_workers()
    
    print("✅ Performance monitoring demonstration completed")


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
        batch_size: int,
        include_ts: bool,
        progress_every: int = 100,
    ) -> None:
    """ Send events in batches of size 'concurrency' using concurrent requests. """
    sent_ok = 0

    async with aiohttp.ClientSession() as session:
        batch: list[Dict[str, Any]] = []
        processed = 0

        async for event in generate_test_data(count=count, include_ts=include_ts):
            batch.append(event)
            if len(batch) >= batch_size:
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
    parser = argparse.ArgumentParser(description="Comprehensive demo of all system capabilities")
    
    # Demo mode selection
    parser.add_argument(
        "--mode", 
        choices=["all", "etl", "scheduler", "lazy", "plugins", "resources", "webhook", "performance", "original"],
        default="all",
        help="Demo mode to run (default: all)"
    )
    
    # Original webhook demo options
    parser.add_argument("--url", default=DEFAULT_URL, help="Webhook URL (default: %(default)s)")
    parser.add_argument("--count", type=int, default=1000, help="Number of events to send")
    parser.add_argument(
        "--batch-size", type=int, default=20, help="Max in-flight requests (soft, via semaphore)"
    )
    parser.add_argument(
        "--include-ts",
        action="store_true",
        help="Include 'ts' in data so windowing uses event time",
    )
    parser.add_argument(
        "--log-level", default=os.getenv("LOG_LEVEL", "INFO"), help="Logging level (default: %(default)s)"
    )
    parser.add_argument(
        "--health", action="store_true", help="Fetch /healthz after sending events"
    )
    
    # Performance demo options
    parser.add_argument(
        "--perf-events", type=int, default=1000, help="Number of events for performance demo"
    )
    parser.add_argument(
        "--perf-workers", type=int, default=4, help="Number of workers for performance demo"
    )
    
    return parser.parse_args()


async def run__demo():
    """Run a comprehensive demo showcasing all system capabilities"""
    print("🎯 ARION CHALLENGE - SYSTEM DEMO")
    print("=" * 80)
    print("This demo showcases all five core advanced Python programming patterns:")
    print("1. Memory-Efficient Data Pipeline (ETL)")
    print("2. Custom Context Manager (Resource Management)")
    print("3. Advanced Meta-Programming (Plugin System)")
    print("4. Custom Iterator with Lazy Evaluation")
    print("5. Distributed Task Scheduler")
    print("=" * 80)
    
    try:
        # Run all demonstrations
        await demo_etl_pipeline()
        await demo_task_scheduler()
        demo_lazy_iterator()
        await demo_plugin_system()
        await demo_resource_manager()
        await demo_webhook_api()
        await demo_performance_monitoring()
        
        print("\n" + "="*80)
        print("🎉 DEMO COMPLETED!")
        print("="*80)
        print("All system capabilities have been demonstrated:")
        print("✅ ETL Pipeline with real-time processing and windowed aggregations")
        print("✅ Task Scheduler with priority queues and multi-process execution")
        print("✅ Lazy Iterator with memory-efficient data transformations")
        print("✅ Plugin System with metaclass-based automatic registration")
        print("✅ Resource Manager with context management and cleanup")
        print("✅ Webhook API with health checks and metrics")
        print("✅ Performance Monitoring with high-volume processing")
        print("="*80)
        
    except Exception as e:
        print(f"\n❌ Demo failed with error: {e}")
        logging.exception("Demo execution failed")


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

    if args.mode == "all":
        # Run demo
        asyncio.run(run__demo())
        
    elif args.mode == "etl":
        asyncio.run(demo_etl_pipeline())
        
    elif args.mode == "scheduler":
        asyncio.run(demo_task_scheduler())
        
    elif args.mode == "lazy":
        demo_lazy_iterator()
        
    elif args.mode == "plugins":
        asyncio.run(demo_plugin_system())
        
    elif args.mode == "resources":
        asyncio.run(demo_resource_manager())
        
    elif args.mode == "webhook":
        asyncio.run(demo_webhook_api())
        
    elif args.mode == "performance":
        asyncio.run(demo_performance_monitoring())
        
    elif args.mode == "original":
        # Original webhook demo functionality
        # Start the server in a separate session
        server_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "etl_pipeline", "webhook.py")
        server_proc = subprocess.Popen(
            [sys.executable, "-u", server_path],
            stdout=None,
            stderr=None,
            cwd=os.path.dirname(server_path),
            start_new_session=True,
            env=os.environ.copy(),
        )

        # Wait for server to be healthy before sending events
        health_url = args.url.replace("/webhook", "/healthz")
        if not asyncio.run(wait_for_health(health_url)):
            logging.error("Server failed to become healthy within timeout")
            if server_proc.poll() is None:
                server_proc.terminate()
            return

        asyncio.run(pump_events(args.url, args.count, args.batch_size, args.include_ts))
        if args.health:
            asyncio.run(fetch_health())

        # Shutdown spawned server
        if server_proc is not None and server_proc.poll() is None:
            logging.info("Stopping server")
            server_proc.terminate()
            try:
                server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logging.warning("Server did not terminate, killing")
                server_proc.kill()


if __name__ == "__main__":
    print("🎯 Arion Challenge - ETL Pipeline Demo")
    print("=" * 70)
    print("Usage examples:")
    print("  python demo.py                    # Run full demo (all features)")
    print("  python demo.py --mode etl         # ETL pipeline only")
    print("  python demo.py --mode scheduler   # Task scheduler only")
    print("  python demo.py --mode lazy        # Lazy iterator only")
    print("  python demo.py --mode plugins     # Plugin system only")
    print("  python demo.py --mode resources   # Resource manager only")
    print("  python demo.py --mode webhook     # Webhook API only")
    print("  python demo.py --mode performance # Performance monitoring only")
    print("  python demo.py --mode original    # Original webhook demo")
    print("=" * 70)
    print()
    
    main()

