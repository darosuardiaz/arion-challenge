import asyncio
import logging
import time
import multiprocessing as mp
import uuid
import queue
from typing import Dict, Any, List, Optional, Callable, Set
from .task_types import Task, TaskStatus, TaskPriority, WorkerInfo, TaskResult
from .task_worker import WorkerProcess

logger = logging.getLogger(__name__)


class TaskQueue:
    """Async task queue with bounded size and priority support"""
    
    def __init__(self, maxsize: int = 1000):
        self.maxsize = maxsize
        self._queues = {
            TaskPriority.CRITICAL: asyncio.Queue(maxsize=maxsize),
            TaskPriority.HIGH: asyncio.Queue(maxsize=maxsize),
            TaskPriority.NORMAL: asyncio.Queue(maxsize=maxsize),
            TaskPriority.LOW: asyncio.Queue(maxsize=maxsize)
        }
        self._task_map: Dict[str, Task] = {}
        self._lock = asyncio.Lock()
    
    async def add_task(self, task: Task) -> bool:
        """Add a task to the appropriate priority queue with backpressure control"""
        async with self._lock:
            try:
                self._task_map[task.id] = task
                self._queues[task.priority].put_nowait(task)
                logger.info(f"Added task {task.id} with priority {task.priority.name}")
                return True
            except asyncio.QueueFull:
                logger.warning(f"Queue full, cannot add task {task.id}")
                return False
    
    async def get_next_task(self) -> Optional[Task]:
        """Get the next task to execute (highest priority first)"""
        for priority in [
            TaskPriority.CRITICAL,
            TaskPriority.HIGH,
            TaskPriority.NORMAL,
            TaskPriority.LOW
        ]:
            try:
                task = self._queues[priority].get_nowait()
                return task
            except asyncio.QueueEmpty:
                continue
        return None
    
    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID"""
        async with self._lock:
            return self._task_map.get(task_id)
    
    async def update_task_status(self, task_id: str, status: TaskStatus, 
                               result: Any = None, error: Exception = None):
        """Update task status and result"""
        async with self._lock:
            if task_id in self._task_map:
                task = self._task_map[task_id]
                task.status = status
                task.result = result
                task.error = error
                
                if status == TaskStatus.RUNNING:
                    task.started_at = time.time()
                elif status in [TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED]:
                    task.completed_at = time.time()
                
                logger.info(f"Updated task {task_id} status to {status.name}")
    
    async def remove_task(self, task_id: str):
        """Remove a task from the queue"""
        async with self._lock:
            if task_id in self._task_map:
                del self._task_map[task_id]
                logger.info(f"Removed task {task_id}")
    
    async def get_pending_tasks(self) -> List[Task]:
        """Get all pending tasks"""
        async with self._lock:
            return [task for task in self._task_map.values() 
                   if task.status == TaskStatus.PENDING]
    
    async def get_tasks_by_status(self, status: TaskStatus) -> List[Task]:
        """Get tasks by status"""
        async with self._lock:
            return [task for task in self._task_map.values() if task.status == status]


class TaskScheduler:
    """
    Distributed Task Scheduler with hybrid architecture:
    - Asyncio for coordination and I/O operations
    - Multiprocessing for CPU-bound task execution
    """
    
    def __init__(self, max_workers: int = None, worker_timeout: float = 30.0, 
                 max_queue_size: int = 1000):
        self.max_workers = max_workers or mp.cpu_count()
        self.worker_timeout = worker_timeout
        self.max_queue_size = max_queue_size
        
        # Task management
        self.task_queue = TaskQueue(max_queue_size)
        self.workers: Dict[str, WorkerInfo] = {}
        self.results: Dict[str, TaskResult] = {}
        
        # Orchestration
        self._worker_queue = mp.Queue(maxsize=max_queue_size)
        self._result_queue_mp = mp.Queue(maxsize=max_queue_size)
        self._stop_event = mp.Event()
        
        # Runtime
        self._running = False
        self._monitor = None
        self._distributor = None
        
        # Statistics
        self.stats = {
            'total_tasks': 0,
            'completed_tasks': 0,
            'failed_tasks': 0,
            'cancelled_tasks': 0,
            'total_execution_time': 0.0,
            'average_execution_time': 0.0
        }
    
    async def _start_worker(self, worker_id: str):
        """Start a new worker process"""
        
        worker_process = WorkerProcess(
            worker_id=worker_id,
            task_queue=self._worker_queue,
            result_queue=self._result_queue_mp,
            stop_event=self._stop_event
        )
        
        process = mp.Process(target=worker_process.run)
        process.start()
        
        self.workers[worker_id] = WorkerInfo(
            id=worker_id,
            process=process
        )
        
        logger.info(f"Started worker {worker_id}")
        
    async def _handle_task_result(self, result: TaskResult):
        """Handle a completed task result"""
        self.results[result.task_id] = result
        
        # Update task status
        await self.task_queue.update_task_status(
            result.task_id, 
            result.status, 
            result.result, 
            result.error
        )
        
        # Update statistics
        self.stats['total_tasks'] += 1
        if result.status == TaskStatus.COMPLETED:
            self.stats['completed_tasks'] += 1
        elif result.status == TaskStatus.FAILED:
            self.stats['failed_tasks'] += 1
        elif result.status == TaskStatus.CANCELLED:
            self.stats['cancelled_tasks'] += 1
        
        # Update execution time statistics
        self.stats['total_execution_time'] += result.execution_time
        self.stats['average_execution_time'] = (
            self.stats['total_execution_time'] / self.stats['total_tasks']
        )
        
        logger.info(f"Handled result for task {result.task_id}: {result.status.name}")

        # Retry failures
        if result.status == TaskStatus.FAILED:
            task = await self.task_queue.get_task(result.task_id)
            if task and task.retries < task.max_retries:
                task.retries += 1
                task.status = TaskStatus.PENDING
                logger.info(f"Retrying task {task.id} ({task.retries}/{task.max_retries})")
                await self.task_queue.add_task(task)
    
    async def _check_worker_health(self):
        """Check worker process health"""
        dead_workers = []
        
        for worker_id, worker_info in list(self.workers.items()):
            if not worker_info.process.is_alive():
                dead_workers.append(worker_id)
                logger.warning(f"Worker {worker_id} died, restarting...")
        
        for worker_id in dead_workers:
            self.workers.pop(worker_id, None)
            await self._start_worker(worker_id)
     
    async def _monitor_workers(self):
        """Worker monitor - handles results and worker health"""
        logger.info("Worker monitor started")
        
        while self._running:
            try:
                # Check for results with timeout
                try:
                    result = await asyncio.to_thread(self._result_queue_mp.get, True, 1.0)
                    await self._handle_task_result(result)
                except queue.Empty:
                    pass  # No results available
                
                # Check worker health
                await self._check_worker_health()
                
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker monitor error: {e}")
                await asyncio.sleep(1.0)
        
        logger.info("Worker monitor stopped")
    
    async def _dispatch(self):
        """Dispatch tasks from queue to workers"""
        logger.info("Task distributor started")
        
        while self._running:
            try:
                task = await self.task_queue.get_next_task()
                
                if task:
                    # Dependency check
                    if task.dependencies:
                        bad = {TaskStatus.FAILED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT}
                        dep_statuses = []
                        for dep_id in task.dependencies:
                            r = self.results.get(dep_id)
                            dep_statuses.append(r.status if r else None)
                        if any(s in bad for s in dep_statuses if s is not None):
                            await self.task_queue.update_task_status(
                                task.id, TaskStatus.CANCELLED, error=Exception("Dependency failed")
                            )
                            continue
                        # Deps not completed yet
                        if not all(s == TaskStatus.COMPLETED for s in dep_statuses if s is not None) or any(s is None for s in dep_statuses):
                            # Add back to the queue
                            await self.task_queue._queues[task.priority].put(task)
                            await asyncio.sleep(0.05)
                            continue

                    # Send to worker process queue
                    try:
                        await asyncio.to_thread(self._worker_queue.put, task, True, 1.0)
                        await self.task_queue.update_task_status(task.id, TaskStatus.RUNNING)
                        logger.debug(f"Distributed task {task.id} to worker queue")
                    except Exception as e:
                        logger.error(f"Failed to distribute task {task.id}: {e}")
                        # Add back to the queue
                        await self.task_queue.add_task(task)
                else:
                    # No tasks available, wait
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Task distributor error: {e}")
                await asyncio.sleep(1.0)
        
        logger.info("Task distributor stopped")
    

    async def start(self):
        """Start Scheduler and Workers"""
        self._running = True
        self._stop_event.clear()

        for i in range(self.max_workers):
            await self._start_worker(f"{i}")
        
        self._monitor = asyncio.create_task(self._monitor_workers())
        self._distributor = asyncio.create_task(self._dispatch())
        
        logger.info(f"Started Task Scheduler with {self.max_workers} workers")
    
    async def stop(self):
        """Stop Scheduler and Workers"""
        self._running = False
        self._stop_event.set()

        # Cancel Monitor and Distributor
        if self._monitor:
            self._monitor.cancel()
        if self._distributor:
            self._distributor.cancel()
        
        # Stop all workers
        for worker_info in self.workers.values():
            worker_info.process.terminate()
            worker_info.process.join(timeout=5.0)
            if worker_info.process.is_alive():
                worker_info.process.kill()
        
        self.workers.clear()
        logger.info("Async scheduler stopped")
     
    async def submit_task(
        self,
        func: Callable,
        args: tuple = (),
        kwargs: dict = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        timeout: float = None,
        retries: int = 3,
        dependencies: Set[str] = None,
        metadata: Dict[str, Any] = None
        ) -> str:
        """Submit a task for execution"""
        if not self._running:
            raise RuntimeError("Scheduler not running")
        
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            priority=priority,
            timeout=timeout,
            max_retries=retries,
            dependencies=dependencies or set(),
            metadata=metadata or {}
        )
        
        # Add to async queue with backpressure
        success = await self.task_queue.add_task(task)
        if not success:
            raise RuntimeError(f"Task queue full, cannot submit task {task_id}")
        
        logger.info(f"Submitted task {task_id}")
        return task_id
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task"""
        task = await self.task_queue.get_task(task_id)
        if task and task.status == TaskStatus.PENDING:
            task.status = TaskStatus.CANCELLED
            await self.task_queue.update_task_status(task_id, TaskStatus.CANCELLED)
            return True
        return False

    async def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the status of a task"""
        task = await self.task_queue.get_task(task_id)
        return task.status if task else None
    
    async def get_task_result(self, task_id: str) -> Optional[TaskResult]:
        """Get the result of a completed task"""
        return self.results.get(task_id)

    async def get_statistics(self) -> Dict[str, Any]:
        """Get scheduler statistics"""
        return {
            **self.stats,
            'total_workers': len(self.workers),
            'active_workers': len([w for w in self.workers.values() if w.process.is_alive()]),
            'queue_size': sum(q.qsize() for q in self.task_queue._queues.values())
        }
    
    async def scale_workers(self, new_count: int):
        """Scale the number of workers"""
        current_count = len(self.workers)
        
        if new_count > current_count:
            # Add workers
            for i in range(current_count, new_count):
                await self._start_worker(f"worker-{i}")
        elif new_count < current_count:
            # Remove workers
            workers_to_remove = list(self.workers.keys())[new_count:]
            for worker_id in workers_to_remove:
                worker_info = self.workers[worker_id]
                worker_info.process.terminate()
                worker_info.process.join(timeout=5.0)
                del self.workers[worker_id]
        
        logger.info(f"Scaled workers from {current_count} to {new_count}")
