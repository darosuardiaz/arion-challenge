import asyncio
import argparse
import logging
import os
import random
import subprocess
import sys
import time
from typing import AsyncIterator, Dict, Any
import aiohttp


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
    parser = argparse.ArgumentParser(description="Send synthetic events to the pipeline webhook")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(message)s")

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
    main()

