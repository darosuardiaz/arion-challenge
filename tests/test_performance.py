import os
import sys
import pytest
import asyncio
import tempfile
import time
import statistics
import concurrent.futures
from typing import List, Dict, Any

# Ensure repository root is on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from etl_pipeline.pipeline import Pipeline
from task_scheduler.task_scheduler import TaskScheduler
from task_scheduler.task_types import TaskPriority, TaskStatus
from lazy_iterator import LazyIterator
from resource_manager import ResourceManager


class TestPipelinePerformance:
    """Performance tests for Pipeline component."""
    
    @pytest.mark.asyncio
    async def test_pipeline_throughput_high_volume(self):
        """Test pipeline throughput with high volume of events."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            pipeline = Pipeline(
                queue_maxsize=1000,
                window_secs=5,  # Longer window for batching
                db_path=db_path
            )
            
            # Generate large number of test events
            event_count = 1000
            categories = ["electronics", "books", "clothing", "home", "sports"]
            
            events = list(
                LazyIterator(range(event_count))
                .map(lambda i: {
                    "data": {
                        "category": categories[i % len(categories)],
                        "value": float(10 + (i % 100)),
                        "quantity": (i % 5) + 1,
                        "ts": time.time() - (i * 0.001)  # Spread over 1 second
                    }
                })
            )
            
            with pipeline:
                try:
                    start_time = time.time()
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send events as fast as possible
                    send_start = time.time()
                    for event in events:
                        await pipeline.ingest_q.put(event)
                    send_time = time.time() - send_start
                    
                    # Wait for processing
                    processing_start = time.time()
                    await asyncio.sleep(7)  # Wait longer than window
                    processing_time = time.time() - processing_start
                    
                    total_time = time.time() - start_time
                    
                    # Verify all events were processed
                    db = pipeline.database
                    total_records = db.execute(
                        "SELECT SUM(record_count) as total FROM aggregates"
                    ).fetchone()['total'] or 0
                    
                    # Performance assertions
                    assert total_records == event_count, f"Expected {event_count}, got {total_records}"
                    
                    throughput = event_count / total_time
                    assert throughput > 100, f"Throughput too low: {throughput:.1f} events/sec"
                    
                    # Log performance metrics
                    print(f"\nPipeline Performance Metrics:")
                    print(f"  Events processed: {total_records}")
                    print(f"  Send time: {send_time:.3f}s")
                    print(f"  Processing time: {processing_time:.3f}s")
                    print(f"  Total time: {total_time:.3f}s")
                    print(f"  Throughput: {throughput:.1f} events/sec")
                    
                finally:
                    await pipeline.stop_workers(timeout=2.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass
    
    @pytest.mark.asyncio
    async def test_pipeline_memory_usage_large_windows(self):
        """Test pipeline memory usage with large aggregation windows."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Use larger window to test memory handling
            pipeline = Pipeline(
                queue_maxsize=500,
                window_secs=10,  # Large window
                db_path=db_path
            )
            
            # Generate events with many categories to stress memory
            event_count = 500
            categories = [f"category_{i}" for i in range(50)]  # 50 categories
            
            events = list(
                LazyIterator(range(event_count))
                .map(lambda i: {
                    "data": {
                        "category": categories[i % len(categories)],
                        "value": float(i % 1000),
                        "quantity": 1,
                        "ts": time.time()  # All in same window
                    }
                })
            )
            
            with pipeline:
                try:
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send all events to same window
                    for event in events:
                        await pipeline.ingest_q.put(event)
                    
                    # Wait for processing but not window completion
                    await asyncio.sleep(2)
                    
                    # Check memory usage (queue sizes)
                    queue_info = pipeline.get_queue_info()
                    
                    # Should handle large number of categories efficiently
                    assert queue_info["dead_letter_q"] == 0, "No events should be dropped"
                    
                    # Wait for window to complete
                    await asyncio.sleep(12)
                    
                    # Verify all categories were processed
                    db = pipeline.database
                    category_count = db.execute(
                        "SELECT COUNT(DISTINCT category) as count FROM aggregates"
                    ).fetchone()['count']
                    
                    assert category_count == len(categories), f"Expected {len(categories)}, got {category_count}"
                    
                finally:
                    await pipeline.stop_workers(timeout=2.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass
    
    @pytest.mark.asyncio
    async def test_pipeline_concurrent_windows(self):
        """Test pipeline performance with multiple concurrent windows."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            pipeline = Pipeline(
                queue_maxsize=200,
                window_secs=2,  # Short windows
                db_path=db_path
            )
            
            # Generate events spanning multiple windows
            event_count = 300
            categories = ["electronics", "books", "clothing"]
            window_span = 6  # Span 3 windows
            
            base_time = time.time()
            events = list(
                LazyIterator(range(event_count))
                .map(lambda i: {
                    "data": {
                        "category": categories[i % len(categories)],
                        "value": float(10 + (i % 50)),
                        "quantity": 1,
                        "ts": base_time - window_span + (i / event_count) * window_span
                    }
                })
            )
            
            with pipeline:
                try:
                    start_time = time.time()
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send events across multiple windows
                    for event in events:
                        await pipeline.ingest_q.put(event)
                    
                    # Wait for all windows to complete
                    await asyncio.sleep(8)
                    
                    # Verify multiple windows were created
                    db = pipeline.database
                    window_count = db.execute(
                        "SELECT COUNT(DISTINCT window_start) as count FROM aggregates"
                    ).fetchone()['count']
                    
                    total_records = db.execute(
                        "SELECT SUM(record_count) as total FROM aggregates"
                    ).fetchone()['total'] or 0
                    
                    assert window_count >= 2, f"Expected multiple windows, got {window_count}"
                    assert total_records == event_count, f"Expected {event_count}, got {total_records}"
                    
                    processing_time = time.time() - start_time
                    print(f"\nConcurrent Windows Performance:")
                    print(f"  Windows created: {window_count}")
                    print(f"  Events processed: {total_records}")
                    print(f"  Processing time: {processing_time:.3f}s")
                    
                finally:
                    await pipeline.stop_workers(timeout=2.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


class TestSchedulerPerformance:
    """Performance tests for TaskScheduler component."""
    
    @pytest.mark.asyncio
    async def test_scheduler_high_task_volume(self):
        """Test scheduler with high volume of tasks."""
        scheduler = TaskScheduler(max_workers=4, max_queue_size=1000)
        
        def cpu_task(n: int):
            """CPU-intensive task for testing."""
            total = 0
            for i in range(n):
                total += i * i
            return total
        
        try:
            await scheduler.start()
            
            # Submit large number of tasks
            task_count = 200
            task_size = 1000  # Computation size
            
            start_time = time.time()
            
            # Submit tasks
            task_ids = []
            submit_start = time.time()
            for i in range(task_count):
                task_id = await scheduler.submit_task(
                    func=cpu_task,
                    args=(task_size,),
                    priority=TaskPriority.NORMAL,
                    metadata={"task_index": i}
                )
                task_ids.append(task_id)
            submit_time = time.time() - submit_start
            
            # Wait for completion
            completion_start = time.time()
            completed = 0
            while completed < task_count:
                completed = 0
                for task_id in task_ids:
                    status = await scheduler.get_task_status(task_id)
                    if status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                        completed += 1
                await asyncio.sleep(0.1)
            completion_time = time.time() - completion_start
            
            total_time = time.time() - start_time
            
            # Performance assertions
            task_throughput = task_count / total_time
            assert task_throughput > 10, f"Task throughput too low: {task_throughput:.1f} tasks/sec"
            
            # Get statistics
            stats = await scheduler.get_statistics()
            assert stats['completed_tasks'] == task_count
            
            print(f"\nScheduler Performance Metrics:")
            print(f"  Tasks submitted: {task_count}")
            print(f"  Submit time: {submit_time:.3f}s")
            print(f"  Completion time: {completion_time:.3f}s")
            print(f"  Total time: {total_time:.3f}s")
            print(f"  Task throughput: {task_throughput:.1f} tasks/sec")
            print(f"  Average execution time: {stats['average_execution_time']:.3f}s")
            
        finally:
            await scheduler.stop()
    
    @pytest.mark.asyncio
    async def test_scheduler_mixed_priority_performance(self):
        """Test scheduler performance with mixed priority tasks."""
        scheduler = TaskScheduler(max_workers=3, max_queue_size=500)
        
        def variable_task(duration: float, task_type: str):
            """Task with variable duration."""
            time.sleep(duration)
            return f"completed_{task_type}"
        
        try:
            await scheduler.start()
            
            # Create mixed priority tasks
            high_priority_count = 20
            normal_priority_count = 50
            low_priority_count = 30
            
            task_ids = []
            start_time = time.time()
            
            # Submit high priority tasks (short duration)
            for i in range(high_priority_count):
                task_id = await scheduler.submit_task(
                    func=variable_task,
                    args=(0.01, "high"),  # 10ms tasks
                    priority=TaskPriority.HIGH,
                    metadata={"priority": "high", "index": i}
                )
                task_ids.append(task_id)
            
            # Submit normal priority tasks (medium duration)
            for i in range(normal_priority_count):
                task_id = await scheduler.submit_task(
                    func=variable_task,
                    args=(0.05, "normal"),  # 50ms tasks
                    priority=TaskPriority.NORMAL,
                    metadata={"priority": "normal", "index": i}
                )
                task_ids.append(task_id)
            
            # Submit low priority tasks (longer duration)
            for i in range(low_priority_count):
                task_id = await scheduler.submit_task(
                    func=variable_task,
                    args=(0.1, "low"),  # 100ms tasks
                    priority=TaskPriority.LOW,
                    metadata={"priority": "low", "index": i}
                )
                task_ids.append(task_id)
            
            # Track completion order
            completion_order = []
            total_tasks = len(task_ids)
            completed = 0
            
            while completed < total_tasks:
                for task_id in task_ids:
                    status = await scheduler.get_task_status(task_id)
                    if status == TaskStatus.COMPLETED and task_id not in completion_order:
                        result = await scheduler.get_task_result(task_id)
                        completion_order.append(result.result)
                        completed += 1
                await asyncio.sleep(0.01)
            
            total_time = time.time() - start_time
            
            # Analyze priority handling
            high_priority_positions = [
                i for i, result in enumerate(completion_order) 
                if result == "completed_high"
            ]
            
            # High priority tasks should generally complete earlier
            avg_high_priority_position = statistics.mean(high_priority_positions) if high_priority_positions else 0
            expected_high_priority_position = high_priority_count / 2
            
            assert avg_high_priority_position <= expected_high_priority_position * 1.5, \
                f"High priority tasks not prioritized effectively: avg position {avg_high_priority_position}"
            
            print(f"\nMixed Priority Performance:")
            print(f"  Total tasks: {total_tasks}")
            print(f"  Total time: {total_time:.3f}s")
            print(f"  High priority avg position: {avg_high_priority_position:.1f}")
            print(f"  Expected high priority position: {expected_high_priority_position:.1f}")
            
        finally:
            await scheduler.stop()


class TestLazyIteratorPerformance:
    """Performance tests for LazyIterator component."""
    
    def test_lazy_iterator_large_dataset_performance(self):
        """Test LazyIterator performance with large datasets."""
        # Test with 100k elements
        dataset_size = 100_000
        
        start_time = time.time()
        
        # Chain multiple operations
        result = list(
            LazyIterator(range(dataset_size))
            .filter(lambda x: x % 10 == 0)  # Keep every 10th element
            .map(lambda x: x * x)           # Square them
            .filter(lambda x: x % 100 == 0) # Keep every 100th squared
            .take(1000)                     # Take first 1000
        )
        
        processing_time = time.time() - start_time
        
        # Should be reasonably fast and return expected count
        assert len(result) == 1000
        assert processing_time < 1.0, f"Processing too slow: {processing_time:.3f}s"
        
        # Verify correctness
        assert result[0] == 0  # 0^2 = 0
        assert result[1] == 10000  # 100^2 = 10000
        
        print(f"\nLazyIterator Large Dataset Performance:")
        print(f"  Dataset size: {dataset_size:,}")
        print(f"  Results: {len(result)}")
        print(f"  Processing time: {processing_time:.3f}s")
        print(f"  Throughput: {dataset_size/processing_time:,.0f} elements/sec")
    
    def test_lazy_iterator_memory_efficiency(self):
        """Test LazyIterator memory efficiency with generators."""
        def infinite_sequence():
            """Generator that could theoretically run forever."""
            i = 0
            while True:
                yield i
                i += 1
        
        start_time = time.time()
        
        # Should be able to handle infinite sequences efficiently
        result = list(
            LazyIterator(infinite_sequence())
            .filter(lambda x: x % 1000 == 0)  # Every 1000th number
            .map(lambda x: x // 1000)         # Normalize
            .take(100)                        # Only take 100
        )
        
        processing_time = time.time() - start_time
        
        assert len(result) == 100
        assert result == list(range(100))  # Should be [0, 1, 2, ..., 99]
        assert processing_time < 0.1, f"Should be very fast: {processing_time:.3f}s"
        
        print(f"\nLazyIterator Memory Efficiency:")
        print(f"  Results from infinite sequence: {len(result)}")
        print(f"  Processing time: {processing_time:.6f}s")
    
    def test_lazy_iterator_chunking_performance(self):
        """Test LazyIterator chunking performance."""
        dataset_size = 50_000
        chunk_size = 100
        
        start_time = time.time()
        
        chunks = list(
            LazyIterator(range(dataset_size))
            .filter(lambda x: x % 2 == 0)  # Even numbers only
            .chunk(size=chunk_size, include_partial=True)
        )
        
        processing_time = time.time() - start_time
        
        # Verify chunking
        expected_chunks = (dataset_size // 2) // chunk_size + (1 if (dataset_size // 2) % chunk_size else 0)
        assert len(chunks) == expected_chunks
        
        # Verify chunk sizes
        for i, chunk in enumerate(chunks[:-1]):  # All but last should be full
            assert len(chunk) == chunk_size
        
        print(f"\nLazyIterator Chunking Performance:")
        print(f"  Dataset size: {dataset_size:,}")
        print(f"  Chunk size: {chunk_size}")
        print(f"  Chunks created: {len(chunks)}")
        print(f"  Processing time: {processing_time:.3f}s")


class TestResourceManagerPerformance:
    """Performance tests for ResourceManager component."""
    
    @pytest.mark.asyncio
    async def test_resource_manager_concurrent_access(self):
        """Test ResourceManager performance under concurrent access."""
        
        def create_mock_db():
            import sqlite3
            return sqlite3.connect(":memory:")
        
        def create_mock_mq():
            return {"queue": asyncio.Queue(maxsize=100)}
        
        rm = ResourceManager(
            db_factory=create_mock_db,
            mq_factory=create_mock_mq
        )
        
        async def worker(worker_id: int, iterations: int):
            """Worker that repeatedly accesses resources."""
            access_times = []
            
            for i in range(iterations):
                start = time.perf_counter()
                
                # Access database
                db = rm.get_database()
                
                # Access message queue
                mq = rm.get_message_queue()
                
                # Do some work
                await asyncio.sleep(0.001)  # Simulate work
                
                access_time = time.perf_counter() - start
                access_times.append(access_time)
            
            return {
                "worker_id": worker_id,
                "access_times": access_times,
                "avg_access_time": statistics.mean(access_times)
            }
        
        with rm:
            start_time = time.time()
            
            # Run multiple concurrent workers
            worker_count = 10
            iterations_per_worker = 50
            
            tasks = [
                worker(i, iterations_per_worker)
                for i in range(worker_count)
            ]
            
            results = await asyncio.gather(*tasks)
            
            total_time = time.time() - start_time
            
            # Analyze performance
            all_access_times = []
            for result in results:
                all_access_times.extend(result["access_times"])
            
            avg_access_time = statistics.mean(all_access_times)
            max_access_time = max(all_access_times)
            
            # Performance assertions
            assert avg_access_time < 0.01, f"Average access time too high: {avg_access_time:.6f}s"
            assert max_access_time < 0.1, f"Max access time too high: {max_access_time:.6f}s"
            
            total_accesses = worker_count * iterations_per_worker
            throughput = total_accesses / total_time
            
            print(f"\nResourceManager Concurrent Access Performance:")
            print(f"  Workers: {worker_count}")
            print(f"  Accesses per worker: {iterations_per_worker}")
            print(f"  Total accesses: {total_accesses}")
            print(f"  Total time: {total_time:.3f}s")
            print(f"  Throughput: {throughput:.1f} accesses/sec")
            print(f"  Average access time: {avg_access_time:.6f}s")
            print(f"  Max access time: {max_access_time:.6f}s")
    
    def test_resource_manager_context_overhead(self):
        """Test ResourceManager context management overhead."""
        
        def simple_factory():
            return {"created": time.time()}
        
        # Test creation overhead
        creation_times = []
        context_times = []
        
        for i in range(100):
            # Measure creation time
            start = time.perf_counter()
            rm = ResourceManager(
                db_factory=simple_factory,
                mq_factory=simple_factory
            )
            creation_time = time.perf_counter() - start
            creation_times.append(creation_time)
            
            # Measure context overhead
            start = time.perf_counter()
            with rm:
                rm.get_database()
                rm.get_message_queue()
            context_time = time.perf_counter() - start
            context_times.append(context_time)
        
        avg_creation_time = statistics.mean(creation_times)
        avg_context_time = statistics.mean(context_times)
        
        # Overhead should be minimal
        assert avg_creation_time < 0.001, f"Creation overhead too high: {avg_creation_time:.6f}s"
        assert avg_context_time < 0.01, f"Context overhead too high: {avg_context_time:.6f}s"
        
        print(f"\nResourceManager Overhead Performance:")
        print(f"  Average creation time: {avg_creation_time:.6f}s")
        print(f"  Average context time: {avg_context_time:.6f}s")
        print(f"  Iterations: {len(creation_times)}")


class TestSystemStressTests:
    """System-wide stress tests."""
    
    @pytest.mark.asyncio
    async def test_full_system_stress(self):
        """Stress test the entire system under load."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Create components with higher limits
            pipeline = Pipeline(
                queue_maxsize=2000,
                window_secs=3,
                db_path=db_path
            )
            scheduler = TaskScheduler(max_workers=5, max_queue_size=1000)
            
            # Stress test parameters
            event_count = 1000
            task_count = 200
            categories = [f"category_{i}" for i in range(20)]
            
            results = {
                "events_sent": 0,
                "events_processed": 0,
                "tasks_completed": 0,
                "errors": []
            }
            
            def stress_task(task_id: int):
                """CPU-intensive task for stress testing."""
                total = 0
                for i in range(1000):
                    total += i * i
                return f"task_{task_id}_completed"
            
            with pipeline:
                await scheduler.start()
                
                try:
                    # Start pipeline workers
                    pipeline_tasks = await pipeline.start_workers()
                    
                    start_time = time.time()
                    
                    # Concurrent event sending and task submission
                    async def send_events():
                        try:
                            for i in range(event_count):
                                event = {
                                    "data": {
                                        "category": categories[i % len(categories)],
                                        "value": float(10 + (i % 100)),
                                        "quantity": (i % 5) + 1,
                                        "ts": time.time() - (i * 0.001)
                                    }
                                }
                                await pipeline.ingest_q.put(event)
                                results["events_sent"] += 1
                                
                                # Add small delay to avoid overwhelming
                                if i % 100 == 0:
                                    await asyncio.sleep(0.01)
                        except Exception as e:
                            results["errors"].append(f"Event sending error: {e}")
                    
                    async def submit_tasks():
                        try:
                            for i in range(task_count):
                                await scheduler.submit_task(
                                    func=stress_task,
                                    args=(i,),
                                    priority=TaskPriority.NORMAL,
                                    metadata={"stress_test": True, "task_id": i}
                                )
                                
                                # Add small delay
                                if i % 50 == 0:
                                    await asyncio.sleep(0.01)
                        except Exception as e:
                            results["errors"].append(f"Task submission error: {e}")
                    
                    # Run concurrently
                    await asyncio.gather(
                        send_events(),
                        submit_tasks()
                    )
                    
                    # Wait for processing
                    await asyncio.sleep(5)
                    
                    # Count completed tasks
                    completed_tasks = 0
                    for i in range(task_count):
                        try:
                            status = await scheduler.get_task_status(f"task_{i}")
                            if status == TaskStatus.COMPLETED:
                                completed_tasks += 1
                        except:
                            pass  # Task might not exist if submission failed
                    
                    results["tasks_completed"] = completed_tasks
                    
                    # Check pipeline processing
                    db = pipeline.database
                    processed_events = db.execute(
                        "SELECT SUM(record_count) as total FROM aggregates"
                    ).fetchone()['total'] or 0
                    
                    results["events_processed"] = processed_events
                    
                    total_time = time.time() - start_time
                    
                    # Stress test assertions (more lenient than performance tests)
                    assert len(results["errors"]) < 10, f"Too many errors: {results['errors']}"
                    assert results["events_processed"] >= results["events_sent"] * 0.8, \
                        f"Too many events lost: {results['events_processed']}/{results['events_sent']}"
                    assert results["tasks_completed"] >= task_count * 0.8, \
                        f"Too many tasks failed: {results['tasks_completed']}/{task_count}"
                    
                    print(f"\nSystem Stress Test Results:")
                    print(f"  Total time: {total_time:.3f}s")
                    print(f"  Events sent: {results['events_sent']}")
                    print(f"  Events processed: {results['events_processed']}")
                    print(f"  Tasks completed: {results['tasks_completed']}/{task_count}")
                    print(f"  Errors: {len(results['errors'])}")
                    print(f"  Event throughput: {results['events_processed']/total_time:.1f} events/sec")
                    print(f"  Task throughput: {results['tasks_completed']/total_time:.1f} tasks/sec")
                    
                finally:
                    await pipeline.stop_workers(timeout=3.0)
                    await scheduler.stop()
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass