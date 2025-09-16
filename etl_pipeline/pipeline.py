import asyncio, os, time, sqlite3, logging, uuid, contextlib, json
from typing import AsyncIterator, Dict, Any, Optional
from collections import defaultdict


logger = logging.getLogger("pipeline.windowed")


class Pipeline:
    def __init__(
        self,
        queue_maxsize: int = None,
        window_secs: int = None,
        db_path: str = None
    ):
        self.queue_maxsize = queue_maxsize or int(os.getenv("QUEUE_MAXSIZE", "1000"))
        self.window_secs = window_secs or int(os.getenv("WINDOW_SECS", "10"))
        self.db_path = db_path or os.getenv("DB_PATH", "aggregates.db")
        
        # Initialize queues
        self.ingest_q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self.db_q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self.outbound_q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self.db_failures_q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_maxsize)
        self.dead_letter_q: asyncio.Queue = asyncio.Queue(maxsize=self.queue_maxsize)
        
        # Track running tasks
        self._tasks: list[asyncio.Task] = []

    def _db_init(self):
        """Initialize the database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS aggregates(
                    window_start   INTEGER NOT NULL,
                    category       TEXT    NOT NULL,
                    total_value    REAL    NOT NULL,
                    total_quantity INTEGER NOT NULL,
                    record_count   INTEGER NOT NULL,
                    avg_value      REAL    NOT NULL,
                    PRIMARY KEY(window_start, category)
                );
            """)
            conn.commit()

    async def db_writer(self, batch_size: int = 256, batch_wait: float = 0.050):
        """Batches aggregate rows and writes them in a single transaction off the event loop."""
        conn: sqlite3.Connection = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        sql = """
        INSERT INTO aggregates(window_start, category, total_value, total_quantity, record_count, avg_value)
        VALUES (:window_start, :category, :total_value, :total_quantity, :record_count, :avg_value)
        ON CONFLICT(window_start, category) DO UPDATE SET
            total_value    = excluded.total_value,
            total_quantity = excluded.total_quantity,
            record_count   = excluded.record_count,
            avg_value      = excluded.avg_value
        """

        try:
            while True:
                first = await self.db_q.get()
                batch = [first]
                start = time.monotonic()

                # try to coalesce more items for short periods
                while len(batch) < batch_size and (time.monotonic() - start) < batch_wait:
                    try:
                        batch.append(self.db_q.get_nowait())
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(0)
                        break

                def _write_batch():
                    with conn:
                        conn.executemany(sql, batch)

                await asyncio.to_thread(_write_batch)
                for _ in batch:
                    self.db_q.task_done()
        except asyncio.CancelledError:
            # drain rows
            pending = []
            while True:
                try:
                    pending.append(self.db_q.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if pending:
                def _write_rest():
                    with conn:
                        conn.executemany(sql, pending)
                await asyncio.to_thread(_write_rest)
            raise
        finally:
            conn.close()

    async def outbound_worker(self):
        """Dummy publisher so outbound_q doesn't back up."""
        try:
            while True:
                msg = await self.outbound_q.get()
                try:
                    logger.info("Published to %s: %s", msg["queue_name"], json.dumps(msg["data"]))
                finally:
                    self.outbound_q.task_done()
        except asyncio.CancelledError:
            raise

    # Helpers
    async def _send_to_mq(self, queue_name: str, message: Dict[str, Any]) -> None:
        """Enqueue to in-process outbound queue; a worker will 'publish'."""
        payload = {
            "queue_name": queue_name,
            "data": message,
            "timestamp": time.time(),
            "message_id": str(uuid.uuid4()),
        }
        try:
            await asyncio.wait_for(self.outbound_q.put(payload), timeout=1.0)
        except asyncio.TimeoutError:
            logger.error("Timeout sending to outbound_q -> DLQ")
            await self.dead_letter_q.put(payload)

    async def _send_to_db(self, agg: Dict[str, Any]) -> None:
        try:
            await self.db_q.put(agg)
        except asyncio.QueueFull:
            logger.error("db_q full; sending to db_failures_q")
            await self.db_failures_q.put(agg)

    def _to_epoch(self, ts) -> float:
        if isinstance(ts, (int, float)):
            return float(ts)
        if isinstance(ts, str):
            try:
                from datetime import datetime
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except Exception:
                return time.time()
        return time.time()

    def _align_window(self, t: float) -> int:
        it = int(t)
        return it - (it % self.window_secs)

    # Stages 
    async def fetch(self, queue: asyncio.Queue) -> AsyncIterator[Dict[str, Any]]:
        while True:
            item = await queue.get()
            try:
                if item is None:
                    break
                yield item
            finally:
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.task_done()

    async def transform(self, stream: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        async for event in stream:
            data = event.get("data", {})
            value = float(data.get("value", 0.0))
            qty = int(data.get("quantity", 1))
            event["total_value"] = value * qty
            event["processed_at"] = time.time()
            yield event

    async def aggregate(self, stream: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """Aggregates per-category within a window."""
        current_ws = None
        groups = defaultdict(lambda: {"total_value": 0.0, "total_quantity": 0, "record_count": 0})

        while True:
            timeout = max(0.0, (current_ws + self.window_secs) - time.time()) if current_ws is not None else None
            try:
                nxt = stream.__anext__()
                event = await asyncio.wait_for(nxt, timeout=timeout)
            except asyncio.TimeoutError:
                # end idle window
                if current_ws is not None and groups:
                    for category, agg in groups.items():
                        rc = agg["record_count"]
                        yield {
                            "window_start": current_ws,
                            "category": category,
                            "total_value": agg["total_value"],
                            "total_quantity": agg["total_quantity"],
                            "record_count": rc,
                            "avg_value": (agg["total_value"] / rc) if rc else 0.0,
                        }
                    groups.clear()
                    current_ws += self.window_secs
                continue
            except StopAsyncIteration:
                break

            data = event.get("data", {})
            evt_ts = data.get("ts", event.get("processed_at", time.time()))
            ws = self._align_window(self._to_epoch(evt_ts))

            if current_ws is None:
                current_ws = ws

            # flush successive windows
            while ws > current_ws:
                if groups:
                    for category, agg in groups.items():
                        rc = agg["record_count"]
                        yield {
                            "window_start": current_ws,
                            "category": category,
                            "total_value": agg["total_value"],
                            "total_quantity": agg["total_quantity"],
                            "record_count": rc,
                            "avg_value": (agg["total_value"] / rc) if rc else 0.0,
                        }
                    groups.clear()
                current_ws += self.window_secs

            # send late event to DLQ
            if ws < current_ws:
                try:
                    await self.dead_letter_q.put({"reason": "late_event", "event": event, "target_window": ws})
                except asyncio.QueueFull:
                    logger.warning("DLQ full; dropping late event")
                continue

            cat = data.get("category")
            if not cat:
                continue
            groups[cat]["total_value"] += float(event.get("total_value", 0.0))
            groups[cat]["total_quantity"] += int(data.get("quantity", 1))
            groups[cat]["record_count"] += 1

        # flush on stream close
        if current_ws is not None and groups:
            for category, agg in groups.items():
                rc = agg["record_count"]
                yield {
                    "window_start": current_ws,
                    "category": category,
                    "total_value": agg["total_value"],
                    "total_quantity": agg["total_quantity"],
                    "record_count": rc,
                    "avg_value": (agg["total_value"] / rc) if rc else 0.0,
                }

    async def sink(self, stream: AsyncIterator[Dict[str, Any]]) -> None:
        async for agg in stream:
            logger.debug(f"Sinking result: {agg}")
            await asyncio.gather(
                self._send_to_db(agg),
                self._send_to_mq("aggregates", agg),
            )

    # Execution
    async def run_pipeline(self):
        stream = self.fetch(self.ingest_q)
        stream = self.transform(stream)
        stream = self.aggregate(stream)
        await self.sink(stream)

    async def start_workers(self) -> list[asyncio.Task]:
        """Start all background workers and return their tasks."""
        await asyncio.to_thread(self._db_init)
        
        tasks = [
            asyncio.create_task(self.run_pipeline()),
            asyncio.create_task(self.db_writer()),
            asyncio.create_task(self.outbound_worker()),
        ]
        self._tasks.extend(tasks)
        return tasks

    async def stop_workers(self, timeout: float = 5.0):
        """Stop all workers gracefully."""
        # Signal pipeline to stop
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.ingest_q.put(None), timeout=0.2)

        # Wait for pipeline to finish naturally (flushes final window)
        pipeline_task = next((t for t in self._tasks if "run_pipeline" in str(t.get_coro())), None)
        if pipeline_task:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(pipeline_task, timeout=timeout)

        # Cancel remaining perpetual workers
        for task in self._tasks:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

        self._tasks.clear()

    def get_queue_info(self) -> Dict[str, int]:
        """Get current queue sizes for health monitoring."""
        return {
            "ingest_q": self.ingest_q.qsize(),
            "db_q": self.db_q.qsize(),
            "outbound_q": self.outbound_q.qsize(),
            "db_failures_q": self.db_failures_q.qsize(),
            "dead_letter_q": self.dead_letter_q.qsize(),
        }
