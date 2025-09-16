"""
Analytics worker plugin for the demo.
"""

import time
import logging
import statistics
from typing import Any, Dict, List
from . import BaseWorker

# Import Task type for type hints
try:
    from task_scheduler.task_types import Task
except ImportError:
    # Fallback for when running standalone
    class Task:
        def __init__(self, id, args, kwargs):
            self.id = id
            self.args = args
            self.kwargs = kwargs

logger = logging.getLogger(__name__)


class AnalyticsWorker(BaseWorker):
    """Performs analytics calculations on data"""
    
    name = "analytics_worker"
    
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or f"analytics_{int(time.time())}"
        self.analytics_cache = {}
    
    def run(self):
        """Main worker loop"""
        logger.info(f"AnalyticsWorker {self.worker_id} started")
        return True
    
    def execute_task(self, task: Task) -> Dict[str, Any]:
        """Execute an analytics task"""
        try:
            data = task.args[0] if task.args else []
            analysis_type = task.kwargs.get('analysis_type', 'summary')
            
            result = {"analysis_type": analysis_type, "timestamp": time.time()}
            
            if analysis_type == "summary":
                result["summary"] = self._calculate_summary(data)
            elif analysis_type == "trends":
                result["trends"] = self._calculate_trends(data)
            elif analysis_type == "correlations":
                result["correlations"] = self._calculate_correlations(data)
            else:
                result["basic_stats"] = self._basic_statistics(data)
            
            logger.info(f"Completed analytics task {task.id} for {analysis_type}")
            return result
            
        except Exception as e:
            logger.error(f"Error in analytics task {task.id}: {e}")
            raise
    
    def _calculate_summary(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate summary statistics"""
        if not data:
            return {"count": 0}
        
        values = [item.get('value', 0) for item in data if isinstance(item, dict)]
        quantities = [item.get('quantity', 0) for item in data if isinstance(item, dict)]
        
        return {
            "count": len(data),
            "total_value": sum(values),
            "total_quantity": sum(quantities),
            "avg_value": statistics.mean(values) if values else 0,
            "max_value": max(values) if values else 0,
            "min_value": min(values) if values else 0
        }
    
    def _calculate_trends(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate trend analysis"""
        if len(data) < 2:
            return {"trend": "insufficient_data"}
        
        values = [item.get('value', 0) for item in data if isinstance(item, dict)]
        if len(values) < 2:
            return {"trend": "insufficient_data"}
        
        # Simple linear trend calculation
        n = len(values)
        x = list(range(n))
        y = values
        
        # Calculate slope
        x_mean = sum(x) / n
        y_mean = sum(y) / n
        
        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        
        slope = numerator / denominator if denominator != 0 else 0
        
        return {
            "slope": slope,
            "trend": "increasing" if slope > 0 else "decreasing" if slope < 0 else "stable",
            "data_points": n
        }
    
    def _calculate_correlations(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate correlations between different fields"""
        if len(data) < 2:
            return {"correlation": "insufficient_data"}
        
        values = [item.get('value', 0) for item in data if isinstance(item, dict)]
        quantities = [item.get('quantity', 0) for item in data if isinstance(item, dict)]
        
        if len(values) != len(quantities):
            return {"correlation": "data_mismatch"}
        
        try:
            correlation = statistics.correlation(values, quantities)
            return {
                "value_quantity_correlation": correlation,
                "strength": "strong" if abs(correlation) > 0.7 else "moderate" if abs(correlation) > 0.3 else "weak"
            }
        except:
            return {"correlation": "calculation_error"}
    
    def _basic_statistics(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate basic statistics"""
        if not data:
            return {"count": 0}
        
        values = [item.get('value', 0) for item in data if isinstance(item, dict)]
        
        return {
            "count": len(data),
            "mean": statistics.mean(values) if values else 0,
            "median": statistics.median(values) if values else 0,
            "mode": statistics.mode(values) if values and len(set(values)) < len(values) else None,
            "stdev": statistics.stdev(values) if len(values) > 1 else 0
        }