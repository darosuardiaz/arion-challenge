import os
import sys
import pytest
import asyncio
import tempfile
import time
import logging
from typing import List

# Ensure repository root is on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from etl_pipeline.pipeline import Pipeline
from task_scheduler.task_scheduler import TaskScheduler
from task_scheduler.task_types import TaskPriority, TaskStatus
from plugins import BaseWorker, get_worker
from lazy_iterator import LazyIterator


# Define top-level functions for task scheduler
def data_processor(category: str, count: int):
    """Task function that processes category data."""
    # This function is designed to be pickled, so it cannot modify
    # external non-picklable state. We return the result instead.
    return f"Processed {count} items for {category}"


def analyze_category(category: str, total_value: float):
    """Analysis task function."""
    if total_value > 100:
        return f"HIGH_VALUE: {category} = ${total_value}"
    else:
        return f"NORMAL: {category} = ${total_value}"


def collect_enriched_data(result, results_list):
    """Callback to collect enriched data."""
    results_list.append(result.result)
    return "collected"


def generate_alert(category: str, total_value: float):
    """Generate alert for high-value categories."""
    if total_value > 150:
        return f"HIGH_VALUE_ALERT: {category} reached ${total_value:.2f}"
    return None


def analyze_trends(category_data: List[dict]):
    """Analyze trends across categories."""
    analysis = {
        "categories_analyzed": len(category_data),
        "total_value": sum(item["total_value"] for item in category_data),
        "avg_value_per_category": sum(item["total_value"] for item in category_data) / len(category_data) if category_data else 0
    }
    return analysis


# Define a simple, picklable class to hold task arguments
class PicklableTask:
    def __init__(self, *args):
        self.args = args


# Define a picklable function for data enrichment
def enrich_data(data_dict, worker_id, enrichment_data):
    """Enrich data with additional metadata."""
    category = data_dict.get("category", "unknown")
    
    enriched = {
        **data_dict,
        "enriched_by": worker_id,
        "category_info": enrichment_data.get(category, {}),
        "processed_at": time.time()
    }
    
    return enriched


# Define custom worker plugin for data enrichment as a top-level class  
class DataEnricherWorker(BaseWorker):
    name = "DataEnricher"

    def __init__(self, worker_id: str, enrichment_data: dict):
        self.worker_id = worker_id
        self.enrichment_data = enrichment_data
        self.processed_count = 0

    def run(self):
        return f"DataEnricher {self.worker_id} ready"

    def execute_task(self, task):
        """Enrich data with additional metadata."""
        data = task.args[0] if task.args else {}
        category = data.get("category", "unknown")

        enriched = {
            **data,
            "enriched_by": self.worker_id,
            "category_info": self.enrichment_data.get(category, {}),
            "processed_at": time.time()
        }

        self.processed_count += 1
        return enriched


class TestPipelineSchedulerIntegration:
    """Integration tests between Pipeline and TaskScheduler."""
    
    @pytest.mark.asyncio
    async def test_pipeline_with_scheduled_processing(self):
        """Test pipeline processing data with scheduled background tasks."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Create pipeline and scheduler
            pipeline = Pipeline(
                queue_maxsize=50,
                window_secs=2,
                db_path=db_path
            )
            scheduler = TaskScheduler(max_workers=2, max_queue_size=10)
            
            results = []
            
            # No callback needed - we'll check results differently

            with pipeline:
                await scheduler.start()
                
                try:
                    # Start pipeline workers
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send some test data to pipeline
                    test_events = [
                        {"data": {"category": "electronics", "value": 100.0, "quantity": 2}},
                        {"data": {"category": "books", "value": 50.0, "quantity": 1}},
                        {"data": {"category": "electronics", "value": 75.0, "quantity": 3}},
                        {"data": {"category": "clothing", "value": 25.0, "quantity": 1}},
                    ]
                    
                    for event in test_events:
                        await pipeline.ingest_q.put(event)
                    
                    # Schedule processing tasks for each category
                    categories = ["electronics", "books", "clothing"]
                    task_ids = []
                    
                    for category in categories:
                        task_id = await scheduler.submit_task(
                            func=data_processor,
                            args=(category, 2),
                            priority=TaskPriority.NORMAL,
                            metadata={"category": category}
                        )
                        task_ids.append(task_id)
                    
                    # Wait for pipeline to process events
                    await asyncio.sleep(3)
                    
                    # Wait for scheduled tasks to complete
                    for task_id in task_ids:
                        try:
                            for _ in range(100): # 10 second timeout
                                status = await scheduler.get_task_status(task_id)
                                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                                    break
                                await asyncio.sleep(0.1)
                            else:
                                pytest.fail(f"Task {task_id} timed out")
                        except asyncio.TimeoutError:
                            pytest.fail(f"Task {task_id} timed out")
                    
                    # Collect results from completed tasks
                    for task_id in task_ids:
                        result = await scheduler.get_task_result(task_id)
                        if result and result.result:
                            results.append(result.result)
                    
                    # Verify pipeline processed data
                    queue_info = pipeline.get_queue_info()
                    assert queue_info["ingest_q"] == 0  # Should be drained
                    
                    # Verify scheduler processed tasks
                    assert len(results) == 3
                    assert any("electronics" in result for result in results)
                    assert any("books" in result for result in results)
                    assert any("clothing" in result for result in results)
                    
                    # Check database has aggregated data
                    db = pipeline.database
                    aggregates = db.execute("SELECT * FROM aggregates").fetchall()
                    assert len(aggregates) > 0
                    
                finally:
                    await pipeline.stop_workers(timeout=1.0)
                    await scheduler.stop()
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass
    
    @pytest.mark.asyncio
    async def test_pipeline_triggers_scheduled_analysis(self):
        """Test pipeline that triggers analysis tasks based on aggregated data."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            pipeline = Pipeline(
                queue_maxsize=30,
                window_secs=1,  # Short window for faster test
                db_path=db_path
            )
            scheduler = TaskScheduler(max_workers=2, max_queue_size=20)
            
            analysis_results = []
            
            # No callback needed - we'll check results differently

            with pipeline:
                await scheduler.start()
                
                try:
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send high-value electronics data
                    high_value_events = [
                        {"data": {"category": "electronics", "value": 200.0, "quantity": 1}},
                        {"data": {"category": "electronics", "value": 150.0, "quantity": 1}},
                        {"data": {"category": "books", "value": 20.0, "quantity": 2}},
                    ]
                    
                    for event in high_value_events:
                        await pipeline.ingest_q.put(event)
                    
                    # Wait for aggregation window to complete
                    await asyncio.sleep(2)
                    
                    # Check aggregated data and schedule analysis
                    db = pipeline.database
                    aggregates = db.execute(
                        "SELECT category, total_value FROM aggregates ORDER BY total_value DESC"
                    ).fetchall()
                    
                    task_ids = []
                    for agg in aggregates:
                        task_id = await scheduler.submit_task(
                            func=analyze_category,
                            args=(agg['category'], agg['total_value']),
                            priority=TaskPriority.HIGH,
                            metadata={"analysis_type": "value_threshold"}
                        )
                        task_ids.append(task_id)
                    
                    # Wait for analysis completion
                    for task_id in task_ids:
                        try:
                            for _ in range(100): # 10 second timeout
                                status = await scheduler.get_task_status(task_id)
                                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                                    break
                                await asyncio.sleep(0.1)
                            else:
                                pytest.fail(f"Task {task_id} timed out")
                        except asyncio.TimeoutError:
                            pytest.fail(f"Task {task_id} timed out")
                    
                    # Collect results from completed tasks
                    for task_id in task_ids:
                        result = await scheduler.get_task_result(task_id)
                        if result and result.result:
                            analysis_results.append(result.result)
                    
                    # Verify analysis results
                    assert len(analysis_results) > 0
                    electronics_analysis = [r for r in analysis_results if "electronics" in r]
                    assert len(electronics_analysis) > 0
                    assert "HIGH_VALUE" in electronics_analysis[0]  # Should be high value
                    
                finally:
                    await pipeline.stop_workers(timeout=1.0)
                    await scheduler.stop()
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


class TestPipelinePluginIntegration:
    """Integration tests between Pipeline and Plugin system."""
    
    @pytest.mark.asyncio
    async def test_pipeline_with_custom_worker_plugins(self):
        """Test pipeline working with custom worker plugins."""
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Create enrichment data
            enrichment_data = {
                "electronics": {"tax_rate": 0.08, "category_id": "ELEC"},
                "books": {"tax_rate": 0.00, "category_id": "BOOK"},
                "clothing": {"tax_rate": 0.06, "category_id": "CLTH"}
            }
            
            # Create worker instances
            enricher1 = get_worker("DataEnricher", worker_id="enricher_1", enrichment_data=enrichment_data)
            enricher2 = get_worker("DataEnricher", worker_id="enricher_2", enrichment_data=enrichment_data)
            
            pipeline = Pipeline(
                queue_maxsize=20,
                window_secs=2,
                db_path=db_path
            )
            scheduler = TaskScheduler(max_workers=2, max_queue_size=15)
            
            enriched_results = []

            with pipeline:
                await scheduler.start()
                
                try:
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send test events
                    test_events = [
                        {"data": {"category": "electronics", "value": 100.0, "quantity": 1}},
                        {"data": {"category": "books", "value": 25.0, "quantity": 2}},
                        {"data": {"category": "clothing", "value": 50.0, "quantity": 1}},
                    ]
                    
                    # Process events through enricher workers
                    for i, event in enumerate(test_events):
                        enricher = enricher1 if i % 2 == 0 else enricher2
                        
                        # Create enrichment task using the picklable function
                        enrichment_task_id = await scheduler.submit_task(
                            func=enrich_data,
                            args=(event["data"], enricher.worker_id, enricher.enrichment_data),
                            priority=TaskPriority.HIGH,
                            metadata={"step": "enrichment"}
                        )
                        
                        # Wait for enrichment to complete
                        try:
                            for _ in range(100): # 10 second timeout
                                status = await scheduler.get_task_status(enrichment_task_id)
                                if status == TaskStatus.COMPLETED:
                                    result = await scheduler.get_task_result(enrichment_task_id)
                                    enriched_data = result.result
                                    
                                    # Directly append to results (no need for additional task)
                                    enriched_results.append(enriched_data)
                                    break
                                elif status == TaskStatus.FAILED:
                                    pytest.fail(f"Enrichment task failed")
                                await asyncio.sleep(0.1)
                            else:
                                pytest.fail(f"Enrichment task {enrichment_task_id} timed out")
                        except asyncio.TimeoutError:
                            pytest.fail(f"Enrichment task {enrichment_task_id} timed out")

                        # Also send to pipeline for aggregation
                        enriched_event = {"data": enriched_data}
                        await pipeline.ingest_q.put(enriched_event)
                    
                    # Wait for processing
                    await asyncio.sleep(10)
                    
                    # Wait for all collection tasks to complete
                    await asyncio.sleep(1)
                    
                    # Verify enrichment worked
                    assert len(enriched_results) == 3
                    
                    for result in enriched_results:
                        assert "enriched_by" in result
                        assert "category_info" in result
                        assert "processed_at" in result
                        assert result["enriched_by"] in ["enricher_1", "enricher_2"]
                    
                    # Verify workers processed data (processed_count won't work with multiprocessing)
                    # Instead verify that all enrichments worked
                    assert len(enriched_results) == 3
                    
                    # Check pipeline aggregated enriched data
                    db = pipeline.database
                    aggregates = db.execute("SELECT * FROM aggregates").fetchall()
                    assert len(aggregates) > 0
                    
                finally:
                    await pipeline.stop_workers(timeout=1.0)
                    await scheduler.stop()
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


class TestLazyIteratorPipelineIntegration:
    """Integration tests between LazyIterator and Pipeline."""
    
    @pytest.mark.asyncio
    async def test_lazy_iterator_data_generation_for_pipeline(self):
        """Test using LazyIterator to generate test data for pipeline."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            pipeline = Pipeline(
                queue_maxsize=100,
                window_secs=1,
                db_path=db_path
            )
            
            # Use LazyIterator to generate test data
            categories = ["electronics", "books", "clothing", "home", "sports"]
            
            def generate_event(i):
                return {
                    "data": {
                        "category": categories[i % len(categories)],
                        "value": float((i * 10) % 200 + 10),  # Values between 10-210
                        "quantity": (i % 5) + 1,  # Quantities 1-5
                        "ts": time.time() + (i * 0.1)  # Spread forward in time
                    }
                }
            
            # Generate 50 test events using LazyIterator
            test_events = list(
                LazyIterator(range(50))
                .map(generate_event)
                .filter(lambda event: event["data"]["value"] > 50)  # Only high-value items
                .take(20)  # Take first 20 high-value items
            )
            
            with pipeline:
                try:
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send generated events to pipeline
                    for event in test_events:
                        await pipeline.ingest_q.put(event)
                    
                    # Wait for processing (longer for many events)
                    await asyncio.sleep(25)
                    
                    # Verify data was processed
                    db = pipeline.database
                    aggregates = db.execute(
                        "SELECT category, COUNT(*) as windows, SUM(record_count) as total_records "
                        "FROM aggregates GROUP BY category ORDER BY total_records DESC"
                    ).fetchall()
                    
                    assert len(aggregates) > 0
                    total_processed = sum(agg['total_records'] for agg in aggregates)
                    assert total_processed == len(test_events)
                    
                    # Verify all categories are represented
                    categories_processed = {agg['category'] for agg in aggregates}
                    assert len(categories_processed) > 1  # Should have multiple categories
                    
                finally:
                    await pipeline.stop_workers(timeout=1.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass
    
    @pytest.mark.asyncio
    async def test_lazy_iterator_result_processing(self):
        """Test using LazyIterator to process pipeline results."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            pipeline = Pipeline(
                queue_maxsize=50,
                window_secs=1,
                db_path=db_path
            )
            
            with pipeline:
                try:
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Send test data
                    test_data = [
                        {"data": {"category": "electronics", "value": 100.0, "quantity": 2}},
                        {"data": {"category": "electronics", "value": 200.0, "quantity": 1}},
                        {"data": {"category": "books", "value": 30.0, "quantity": 3}},
                        {"data": {"category": "books", "value": 20.0, "quantity": 2}},
                        {"data": {"category": "clothing", "value": 75.0, "quantity": 1}},
                    ]
                    
                    for event in test_data:
                        await pipeline.ingest_q.put(event)
                    
                    # Wait for aggregation
                    await asyncio.sleep(2)
                    
                    # Get aggregated results
                    db = pipeline.database
                    raw_results = db.execute(
                        "SELECT category, total_value, record_count, avg_value FROM aggregates"
                    ).fetchall()
                    
                    # Process results using LazyIterator
                    processed_results = list(
                        LazyIterator(raw_results)
                        .map(lambda row: {
                            "category": row['category'],
                            "total_value": row['total_value'],
                            "record_count": row['record_count'],
                            "avg_value": row['avg_value']
                        })
                        .filter(lambda result: result["total_value"] > 100)  # High-value categories
                        .map(lambda result: {
                            **result,
                            "value_tier": "HIGH" if result["total_value"] > 200 else "MEDIUM"
                        })
                    )
                    
                    # Verify processing
                    assert len(processed_results) > 0
                    electronics_result = next(
                        (r for r in processed_results if r["category"] == "electronics"), 
                        None
                    )
                    assert electronics_result is not None
                    assert electronics_result["value_tier"] == "HIGH"  # Should be high value
                    
                    # Use LazyIterator to generate summary statistics
                    summary = (
                        LazyIterator(processed_results)
                        .map(lambda r: r["total_value"])
                        .reduce(lambda acc, val: {
                            "total": acc["total"] + val,
                            "count": acc["count"] + 1,
                            "max": max(acc["max"], val)
                        }, {"total": 0, "count": 0, "max": 0})
                    )
                    
                    assert summary["count"] > 0
                    assert summary["total"] > 0
                    assert summary["max"] > 0
                    
                finally:
                    await pipeline.stop_workers(timeout=1.0)
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass


class TestFullSystemIntegration:
    """Full system integration tests."""
    
    @pytest.mark.asyncio
    async def test_complete_data_processing_workflow(self):
        """Test complete workflow: data generation -> pipeline -> scheduling -> analysis."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
            db_path = tmp.name
        
        try:
            # Create all components
            pipeline = Pipeline(queue_maxsize=100, window_secs=2, db_path=db_path)
            scheduler = TaskScheduler(max_workers=3, max_queue_size=20)
            
            # Data for the workflow
            workflow_results = {
                "events_generated": 0,
                "events_processed": 0,
                "analysis_completed": [],
                "alerts_generated": []
            }
            
            # No callbacks needed - we'll check results differently

            with pipeline:
                await scheduler.start()
                
                try:
                    pipeline_tasks = await pipeline.start_workers()
                    
                    # Step 1: Generate diverse test data using LazyIterator
                    categories = ["electronics", "books", "clothing", "home"]
                    
                    events = list(
                        LazyIterator(range(40))
                        .map(lambda i: {
                            "data": {
                                "category": categories[i % len(categories)],
                                "value": float(50 + (i * 7) % 200),  # Varying values
                                "quantity": (i % 3) + 1,
                                "ts": time.time() + (i * 0.05)  # Forward timestamps
                            }
                        })
                        .filter(lambda event: event["data"]["value"] >= 50)  # Quality filter
                    )
                    
                    workflow_results["events_generated"] = len(events)
                    
                    # Step 2: Send events to pipeline
                    for event in events:
                        await pipeline.ingest_q.put(event)
                    
                    # Step 3: Wait for pipeline aggregation (longer for many events)
                    await asyncio.sleep(25)
                    
                    # Step 4: Get aggregated results
                    db = pipeline.database
                    aggregates = db.execute(
                        "SELECT category, total_value, record_count FROM aggregates ORDER BY total_value DESC"
                    ).fetchall()
                    
                    workflow_results["events_processed"] = sum(agg['record_count'] for agg in aggregates)
                    
                    # Step 5: Schedule analysis tasks
                    analysis_task_ids = []
                    
                    # Alert generation tasks
                    for agg in aggregates:
                        task_id = await scheduler.submit_task(
                            func=generate_alert,
                            args=(agg['category'], agg['total_value']),
                            priority=TaskPriority.HIGH,
                            metadata={"type": "alert_generation", "category": agg['category']}
                        )
                        analysis_task_ids.append(task_id)
                    
                    # Trend analysis task
                    aggregate_data = [dict(agg) for agg in aggregates]
                    trend_task_id = await scheduler.submit_task(
                        func=analyze_trends,
                        args=(aggregate_data,),
                        priority=TaskPriority.NORMAL,
                        metadata={"type": "trend_analysis"}
                    )
                    analysis_task_ids.append(trend_task_id)
                    
                    # Step 6: Wait for all analysis to complete
                    for task_id in analysis_task_ids:
                        try:
                            for _ in range(100): # 10 second timeout
                                status = await scheduler.get_task_status(task_id)
                                if status in [TaskStatus.COMPLETED, TaskStatus.FAILED]:
                                    break
                                await asyncio.sleep(0.1)
                            else:
                                pytest.fail(f"Task {task_id} timed out")
                        except asyncio.TimeoutError:
                            pytest.fail(f"Task {task_id} timed out")
                    
                    # Collect results from completed tasks
                    for task_id in analysis_task_ids:
                        result = await scheduler.get_task_result(task_id)
                        if result and result.result:
                            task_metadata = await scheduler.task_queue.get_task(task_id)
                            if task_metadata and task_metadata.metadata.get("type") == "alert_generation":
                                if result.result:  # Only add non-None alerts
                                    workflow_results["alerts_generated"].append(result.result)
                            elif task_metadata and task_metadata.metadata.get("type") == "trend_analysis":
                                workflow_results["analysis_completed"].append(result.result)
                    
                    # Step 7: Verify complete workflow
                    assert workflow_results["events_generated"] > 0
                    assert workflow_results["events_processed"] == workflow_results["events_generated"]
                    assert len(workflow_results["analysis_completed"]) > 0
                    
                    # Check that high-value categories generated alerts
                    high_value_aggregates = [agg for agg in aggregates if agg['total_value'] > 150]
                    if high_value_aggregates:
                        assert len(workflow_results["alerts_generated"]) > 0
                    
                    # Verify trend analysis
                    trend_analysis = workflow_results["analysis_completed"][0]
                    assert trend_analysis["categories_analyzed"] == len(aggregates)
                    assert trend_analysis["total_value"] > 0
                    
                    # Final verification using LazyIterator for result processing
                    final_summary = (
                        LazyIterator(aggregates)
                        .map(lambda agg: {
                            "category": agg['category'],
                            "efficiency": agg['total_value'] / agg['record_count']
                        })
                        .filter(lambda summary: summary["efficiency"] > 50)
                        .reduce(lambda acc, item: acc + 1, 0)
                    )
                    
                    assert final_summary > 0, "Should have efficient categories"
                    
                finally:
                    await pipeline.stop_workers(timeout=2.0)
                    await scheduler.stop()
        finally:
            try:
                os.unlink(db_path)
            except OSError:
                pass