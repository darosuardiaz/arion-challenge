import asyncio
import logging
import os
import time
from typing import Dict, Any
from aiohttp import web
from pipeline import Pipeline

PORT = int(os.getenv("PORT", "8080"))
logger = logging.getLogger("etl_pipeline")


def _validate_payload(payload: Dict[str, Any]) -> str | None:
    if not isinstance(payload, dict) or "data" not in payload or not isinstance(payload["data"], dict):
        return "Invalid payload structure"
    data = payload["data"]
    if "category" not in data or not isinstance(data["category"], str):
        return "Missing or invalid category"
    if "value" in data and not isinstance(data["value"], (int, float)):
        return "Invalid value type"
    if "quantity" in data and not isinstance(data["quantity"], int):
        return "Invalid quantity type"
    return None

# ------------------------------------------------------------------
# API endpoints
# ------------------------------------------------------------------

async def handle_webhook(request: web.Request) -> web.Response:
    try:
        payload = await request.json()
    except Exception:
        return web.Response(status=400, text="Invalid JSON")

    err = _validate_payload(payload)
    if err:
        return web.Response(status=422, text=err)

    pipeline: Pipeline = request.app["pipeline_instance"]
    try:
        await asyncio.wait_for(pipeline.ingest_q.put(payload), timeout=0.25)
    except asyncio.TimeoutError:
        return web.Response(status=429, text="Busy, try again soon")

    return web.Response(status=202, text="Accepted")


async def healthz(request: web.Request) -> web.Response:
    """Health check endpoint with queue and resource information."""
    pipeline: Pipeline = request.app["pipeline_instance"]
    
    # Get queue information
    queue_info = pipeline.get_queue_info()
    
    # Get resource metrics
    resource_metrics = pipeline.get_resource_metrics()
    
    # Combine into comprehensive health status
    health_data = {
        "status": "healthy" if resource_metrics.get("active", False) else "unhealthy",
        "queues": queue_info,
        "resources": {
            "active": resource_metrics.get("active", False),
            "db_acquire_ms": resource_metrics.get("acquire_ms", {}).get("db"),
            "mq_acquire_ms": resource_metrics.get("acquire_ms", {}).get("mq"),
            "context_ms": resource_metrics.get("context_ms"),
            "attached_contexts": len(resource_metrics.get("attached", {}))
        },
        "workers": {
            "task_count": len(pipeline._tasks),
            "running_tasks": sum(1 for task in pipeline._tasks if not task.done())
        }
    }
    
    # Return appropriate status code
    status_code = 200 if health_data["status"] == "healthy" else 503
    return web.json_response(health_data, status=status_code)


async def metrics(request: web.Request) -> web.Response:
    """Metrics endpoint for ResourceManager and Pipeline performance."""
    pipeline: Pipeline = request.app["pipeline_instance"]
    resource_metrics = pipeline.get_resource_metrics()
    queue_info = pipeline.get_queue_info()
    
    metrics_data = {
        "timestamp": time.time(),
        "resource_manager": resource_metrics,
        "queues": queue_info,
        "pipeline": {
            "window_secs": pipeline.window_secs,
            "queue_maxsize": pipeline.queue_maxsize,
            "db_path": pipeline.db_path
        },
        "workers": {
            "tasks": [
                {
                    "name": task.get_name(),
                    "done": task.done(),
                    "cancelled": task.cancelled()
                } for task in pipeline._tasks
            ]
        }
    }
    
    return web.json_response(metrics_data)

# ------------------------------------------------------------------
# Lifecycle handlers
# ------------------------------------------------------------------

async def on_startup(app: web.Application):
    """Initialize pipeline with ResourceManager context."""
    # Create pipeline instance with ResourceManager integration
    pipeline = Pipeline(
        queue_maxsize=int(os.getenv("QUEUE_MAXSIZE", "1000")),
        window_secs=int(os.getenv("WINDOW_SECS", "10")),
        db_path=os.getenv("DB_PATH", "aggregates.db"),
        logger=logging.getLogger("webhook.pipeline")
    )
    
    # Enter the ResourceManager context
    pipeline.__enter__()
    app["pipeline_instance"] = pipeline
    
    try:
        # Start pipeline workers
        app["pipeline_tasks"] = await pipeline.start_workers()
        logger.info("Pipeline started with ResourceManager integration")
        logger.info("Server ready on port %d", PORT)
        
        # Log initial metrics
        metrics = pipeline.get_resource_metrics()
        logger.info("Initial resource metrics: DB=%sms, MQ=%sms", 
                   metrics['acquire_ms']['db'], metrics['acquire_ms']['mq'])
        
    except Exception as e:
        # If startup fails, clean up the context
        pipeline.__exit__(type(e), e, e.__traceback__)
        raise


async def on_cleanup(app: web.Application):
    """Gracefully shutdown pipeline and ResourceManager."""
    pipeline: Pipeline = app.get("pipeline_instance")
    if pipeline:
        logger.info("Stopping pipeline workers...")
        
        try:
            # Stop pipeline workers with timeout
            await pipeline.stop_workers(timeout=5.0)
            logger.info("Pipeline workers stopped")
            
            # Get final metrics before cleanup
            metrics = pipeline.get_resource_metrics()
            logger.info("Final resource metrics: context=%sms", metrics.get('context_ms'))
            
        except asyncio.TimeoutError:
            logger.warning("Pipeline worker shutdown timeout")
            # Force cancel remaining tasks
            for task in pipeline._tasks:
                if not task.done():
                    task.cancel()
        except Exception as e:
            logger.error("Error during pipeline shutdown: %s", e)
        finally:
            # Always exit the ResourceManager context
            try:
                pipeline.__exit__(None, None, None)
                logger.info("ResourceManager context exited")
            except Exception as e:
                logger.error("Error exiting ResourceManager context: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app = web.Application(client_max_size=1024**2)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/metrics", metrics)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    web.run_app(app, host="0.0.0.0", port=PORT)