import asyncio
import contextlib
import json
import logging
import os
import time
import uuid
from typing import Dict, Any

from aiohttp import web

from pipeline import Pipeline

PORT = int(os.getenv("PORT", "8080"))
logger = logging.getLogger("pipeline.windowed")


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
    pipeline: Pipeline = request.app["pipeline_instance"]
    info = pipeline.get_queue_info()
    return web.json_response(info)


async def on_startup(app: web.Application):
    # Create pipeline instance
    pipeline = Pipeline()
    app["pipeline_instance"] = pipeline
    
    # Start pipeline workers
    app["pipeline_tasks"] = await pipeline.start_workers()
    logger.info("Server ready on port %d", PORT)


async def on_cleanup(app: web.Application):
    # Stop pipeline workers gracefully
    pipeline: Pipeline = app.get("pipeline_instance")
    if pipeline:
        await pipeline.stop_workers()



def create_app() -> web.Application:
    """Create and configure the web application."""
    app = web.Application(client_max_size=1024**2)
    app.router.add_post("/webhook", handle_webhook)
    app.router.add_get("/healthz", healthz)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)