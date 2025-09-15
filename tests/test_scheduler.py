import asyncio
import logging
import time
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from task_scheduler.task_scheduler import TaskScheduler
from task_scheduler.task_types import TaskPriority, TaskStatus
from task_scheduler.task_functions import cpu_intensive_task, io_task, failing_task

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_backpressure_control():
    """Test backpressure control with bounded queues"""
    print("\n=== Testing Backpressure Control ===")
    
    # Create scheduler with small queue size
    scheduler = TaskScheduler(max_workers=2, max_queue_size=3)
    
    try:
        await scheduler.start()
        await asyncio.sleep(1)
        
        # Try to submit more tasks than queue can hold
        task_ids = []
        for i in range(5):
            try:
                task_id = await scheduler.submit_task(
                    func=cpu_intensive_task,
                    args=(10000,),
                    priority=TaskPriority.NORMAL,
                    metadata={"batch": i}
                )
                task_ids.append(task_id)
                print(f"Submitted task {i+1}/5: {task_id}")
            except RuntimeError as e:
                print(f"Failed to submit task {i+1}: {e}")
                break
        
        print(f"Successfully submitted {len(task_ids)} tasks")
        
        # Wait for completion
        for task_id in task_ids:
            while True:
                status = await scheduler.get_task_status(task_id)
                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    result = await scheduler.get_task_result(task_id)
                    print(f"Task {task_id}: {status.name}")
                    break
                await asyncio.sleep(0.1)
        
    finally:
        await scheduler.stop()


def print_task_name(name):
    """Simple function for testing priority ordering"""
    print(f"Executing {name}")
    return f"Completed {name}"

async def test_priority_ordering():
    """Test priority-based task ordering"""
    print("\n=== Testing Priority Ordering ===")
    
    scheduler = TaskScheduler(max_workers=1, max_queue_size=10)
    
    try:
        await scheduler.start()
        await asyncio.sleep(1)
        
        # Submit tasks in reverse priority order
        tasks = [
            (TaskPriority.LOW, "low_priority_task"),
            (TaskPriority.HIGH, "high_priority_task"),
            (TaskPriority.NORMAL, "normal_priority_task"),
            (TaskPriority.CRITICAL, "critical_priority_task"),
        ]
        
        task_ids = []
        for priority, name in tasks:
            task_id = await scheduler.submit_task(
                func=print_task_name,
                args=(name,),
                priority=priority,
                metadata={"name": name}
            )
            task_ids.append(task_id)
            print(f"Submitted {name} with priority {priority.name}")
        
        # Wait for all tasks to complete and track order
        execution_order = []
        for task_id in task_ids:
            while True:
                status = await scheduler.get_task_status(task_id)
                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    # Get task to see which one completed
                    task = await scheduler.task_queue.get_task(task_id)
                    if task and task.metadata.get("name"):
                        execution_order.append(task.metadata["name"])
                    break
                await asyncio.sleep(0.1)
        
        print(f"Completion order: {execution_order}")
        
    finally:
        await scheduler.stop()


async def test_worker_scaling():
    """Test dynamic worker scaling"""
    print("\n=== Testing Worker Scaling ===")
    
    scheduler = TaskScheduler(max_workers=1, max_queue_size=20)
    
    try:
        await scheduler.start()
        await asyncio.sleep(1)
        
        # Check initial worker count
        stats = await scheduler.get_statistics()
        print(f"Initial workers: {stats['total_workers']}")
        
        # Scale up
        await scheduler.scale_workers(4)
        await asyncio.sleep(1)
        
        stats = await scheduler.get_statistics()
        print(f"After scaling up: {stats['total_workers']}")
        
        # Submit multiple tasks to see parallel execution
        task_ids = []
        for i in range(8):
            task_id = await scheduler.submit_task(
                func=cpu_intensive_task,
                args=(50000,),
                priority=TaskPriority.NORMAL,
                metadata={"task_num": i}
            )
            task_ids.append(task_id)
        
        print(f"Submitted {len(task_ids)} tasks")
        
        # Monitor execution
        start_time = time.time()
        completed = 0
        while completed < len(task_ids):
            completed = 0
            for task_id in task_ids:
                status = await scheduler.get_task_status(task_id)
                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    completed += 1
            await asyncio.sleep(0.1)
        
        execution_time = time.time() - start_time
        print(f"All tasks completed in {execution_time:.2f}s")
        
        # Scale down
        await scheduler.scale_workers(2)
        await asyncio.sleep(1)
        
        stats = await scheduler.get_statistics()
        print(f"After scaling down: {stats['total_workers']}")
        
    finally:
        await scheduler.stop()


async def test_mixed_task_types():
    """Test mixed CPU and I/O intensive tasks"""
    print("\n=== Testing Mixed Task Types ===")
    
    scheduler = TaskScheduler(max_workers=3, max_queue_size=15)
    
    try:
        await scheduler.start()
        await asyncio.sleep(1)
        
        # Submit mix of CPU and I/O tasks
        task_ids = []
        
        # CPU intensive tasks
        for i in range(3):
            task_id = await scheduler.submit_task(
                func=cpu_intensive_task,
                args=(75000,),
                priority=TaskPriority.HIGH,
                metadata={"type": "cpu", "id": i}
            )
            task_ids.append(task_id)
        
        # I/O intensive tasks
        for i in range(3):
            task_id = await scheduler.submit_task(
                func=io_task,
                args=(1.0,),
                priority=TaskPriority.NORMAL,
                metadata={"type": "io", "id": i}
            )
            task_ids.append(task_id)
        
        # Failing task
        task_id = await scheduler.submit_task(
            func=failing_task,
            priority=TaskPriority.LOW,
            metadata={"type": "failing"}
        )
        task_ids.append(task_id)
        
        print(f"Submitted {len(task_ids)} mixed tasks")
        
        # Monitor execution
        results = {}
        for task_id in task_ids:
            while True:
                status = await scheduler.get_task_status(task_id)
                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    result = await scheduler.get_task_result(task_id)
                    results[task_id] = {
                        'status': status,
                        'result': result.result if result else None,
                        'execution_time': result.execution_time if result else 0,
                        'error': result.error if result else None
                    }
                    break
                await asyncio.sleep(0.1)
        
        # Print results
        for task_id, result in results.items():
            print(f"Task {task_id}: {result['status'].name} "
                  f"({result['execution_time']:.2f}s)")
            if result['error']:
                print(f"  Error: {result['error']}")
        
        # Print final statistics
        stats = await scheduler.get_statistics()
        print(f"\nFinal Statistics:")
        print(f"  Total tasks: {stats['total_tasks']}")
        print(f"  Completed: {stats['completed_tasks']}")
        print(f"  Failed: {stats['failed_tasks']}")
        print(f"  Average execution time: {stats['average_execution_time']:.2f}s")
        print(f"  Active workers: {stats['active_workers']}")
        print(f"  Queue size: {stats['queue_size']}")
        
    finally:
        await scheduler.stop()


async def test_task_dependencies():
    """Test task dependencies"""
    print("\n=== Testing Task Dependencies ===")
    
    scheduler = TaskScheduler(max_workers=2, max_queue_size=10)
    
    try:
        await scheduler.start()
        await asyncio.sleep(0.5)
        
        # Test successful dependency chain
        print("\n--- Testing successful dependency chain ---")
        task_a_id = await scheduler.submit_task(
            func=io_task, args=(0.2,), metadata={"name": "A"}
        )
        task_b_id = await scheduler.submit_task(
            func=io_task, args=(0.2,), dependencies={task_a_id}, metadata={"name": "B"}
        )
        task_c_id = await scheduler.submit_task(
            func=io_task, args=(0.2,), dependencies={task_b_id}, metadata={"name": "C"}
        )

        all_ids = [task_a_id, task_b_id, task_c_id]
        results = {}
        for task_id in all_ids:
            while True:
                status = await scheduler.get_task_status(task_id)
                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    result = await scheduler.get_task_result(task_id)
                    results[task_id] = result
                    task = await scheduler.task_queue.get_task(task_id)
                    print(f"Task {task.metadata.get('name')} finished with status {status.name}")
                    break
                await asyncio.sleep(0.1)

        task_a = await scheduler.task_queue.get_task(task_a_id)
        task_b = await scheduler.task_queue.get_task(task_b_id)
        task_c = await scheduler.task_queue.get_task(task_c_id)
        
        assert results[task_a_id].status == TaskStatus.COMPLETED
        assert results[task_b_id].status == TaskStatus.COMPLETED
        assert results[task_c_id].status == TaskStatus.COMPLETED
        
        assert task_a.completed_at <= task_b.started_at
        assert task_b.completed_at <= task_c.started_at
        print("Successful dependency chain test passed.")

        # Test failing dependency
        print("\n--- Testing failing dependency ---")
        failing_task_id = await scheduler.submit_task(
            func=failing_task, metadata={"name": "Failing"}
        )
        dependent_task_id = await scheduler.submit_task(
            func=io_task, args=(0.1,), dependencies={failing_task_id}, metadata={"name": "Dependent"}
        )
        
        fail_ids = [failing_task_id, dependent_task_id]
        for task_id in fail_ids:
             while True:
                status = await scheduler.get_task_status(task_id)
                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    task = await scheduler.task_queue.get_task(task_id)
                    print(f"Task {task.metadata.get('name')} finished with status {status.name}")
                    break
                await asyncio.sleep(0.1)

        assert await scheduler.get_task_status(failing_task_id) == TaskStatus.FAILED
        assert await scheduler.get_task_status(dependent_task_id) == TaskStatus.CANCELLED
        print("Failing dependency test passed.")

    finally:
        await scheduler.stop()


async def main():
    """Run all tests"""
    print("Async Distributed Task Scheduler Tests")
    print("=" * 50)
    
    try:
        await test_backpressure_control()
        await test_priority_ordering()
        await test_worker_scaling()
        await test_mixed_task_types()
        await test_task_dependencies()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())