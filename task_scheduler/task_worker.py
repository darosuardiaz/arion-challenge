import logging
import time
import queue
import multiprocessing as mp
from plugins import BaseWorker, unregister_worker_instance
from .task_types import Task, TaskStatus, TaskResult




class WorkerProcess(BaseWorker):
    """Worker process for executing tasks"""
    name = "SchedulerWorker"
    
    def __init__(self, worker_id: str, task_queue: mp.Queue, result_queue: mp.Queue,
                 stop_event: mp.Event, max_tasks: int = 1000):
        self.worker_id = worker_id
        self.name = f"Worker-{worker_id}"
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stop_event = stop_event
        self.max_tasks = max_tasks
        self.tasks_processed = 0
        self.logger = logging.getLogger(self.name)
    
    def run(self):
        """Main worker loop"""
        self.logger.info(f"{self.name} started")
        
        while not self.stop_event.is_set() and self.tasks_processed < self.max_tasks:
            try:
                task = self.task_queue.get(timeout=1.0)
                if task is None:
                    break
                self.execute_task(task)
                
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"{self.name} error: {e}")
                break
        
        self.logger.info(f"{self.name} stopped after {self.tasks_processed} tasks")
        try:
            composite_name = f"{type(self).name}-{self.worker_id}" if getattr(type(self), 'name', None) else self.name
            unregister_worker_instance(composite_name)
        except Exception:
            pass
    
    def execute_task(self, task: Task):
        """Execute a single task"""
        start_time = time.time()
        self.logger.info(f"Executing task {task.id}")
        
        try:
            task.status = TaskStatus.RUNNING
            task.started_at = time.time()
            task.worker_id = self.worker_id
            
            result = task.func(*task.args, **task.kwargs)
            execution_time = time.time() - start_time

            task_result = TaskResult(
                task_id=task.id,
                status=TaskStatus.COMPLETED,
                result=result,
                execution_time=execution_time
            )
            
            self.result_queue.put(task_result)
            self.tasks_processed += 1
            
            self.logger.info(f"Task {task.id} completed in {execution_time:.2f}s")
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Task {task.id} failed: {e}")
            
            task_result = TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                error=e,
                execution_time=execution_time
            )
            
            self.result_queue.put(task_result)
            self.tasks_processed += 1
