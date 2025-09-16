"""
Example data processing plugin for the demo.
"""

import time
import logging
from typing import Any, Dict
from . import BaseWorker
from task_scheduler.task_types import Task

logger = logging.getLogger(__name__)


class DataProcessor(BaseWorker):
    """Processes data with various transformations"""
    
    name = "data_processor"
    
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or f"processor_{int(time.time())}"
        self.processed_count = 0
    
    def run(self):
        """Main worker loop"""
        logger.info(f"DataProcessor {self.worker_id} started")
        return True
    
    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Execute a data processing task"""
        try:
            data = task.args[0] if task.args else {}
            operation = task.kwargs.get('operation', 'transform')
            
            result = {"original": data, "processed_at": time.time()}
            
            if operation == "transform":
                result["transformed"] = self._transform_data(data)
            elif operation == "validate":
                result["valid"] = self._validate_data(data)
            elif operation == "aggregate":
                result["aggregated"] = self._aggregate_data(data)
            else:
                result["processed"] = data
            
            self.processed_count += 1
            result["processed_count"] = self.processed_count
            
            logger.info(f"Processed task {task.id} with operation {operation}")
            return result
            
        except Exception as e:
            logger.error(f"Error processing task {task.id}: {e}")
            raise
    
    def _transform_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data by normalizing values"""
        if isinstance(data, dict):
            return {
                k: v.upper() if isinstance(v, str) else v * 2 if isinstance(v, (int, float)) else v
                for k, v in data.items()
            }
        return data
    
    def _validate_data(self, data: Dict[str, Any]) -> bool:
        """Validate data structure"""
        if not isinstance(data, dict):
            return False
        required_fields = ['category', 'value']
        return all(field in data for field in required_fields)
    
    def _aggregate_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate data by category"""
        if isinstance(data, dict) and 'category' in data:
            return {
                'category': data['category'],
                'total_value': data.get('value', 0) * data.get('quantity', 1),
                'count': 1
            }
        return data