from __future__ import annotations

import asyncio
import contextlib
import logging
import sqlite3
import time
from contextlib import AbstractContextManager, ExitStack
from typing import Any, Callable, Optional, Dict, Tuple


class DatabaseManager:
    def __init__(self, db_path: str, *, setup_schema: bool = True):
        self.db_path = db_path
        self.setup_schema = setup_schema
        self.connection: sqlite3.Connection = None
    
    def __enter__(self):
        """Create database connection."""
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA journal_mode=WAL;")
        self.connection.execute("PRAGMA synchronous=NORMAL;")
        
        if self.setup_schema:
            self.connection.execute("""
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
            self.connection.commit()
        
        return self.connection
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
        return False

class QueueManager:
    def __init__(self, queue_maxsize: int = 1000, *, queue_names: Optional[list[str]] = None):
        self.queue_maxsize = queue_maxsize
        self.queue_names = queue_names or ["ingest", "db", "outbound", "db_failures", "dead_letter"]
        self.queues: Dict[str, asyncio.Queue] = {}
    
    def __enter__(self):
        """Create all queues."""
        self.queues = {
            name: asyncio.Queue(maxsize=self.queue_maxsize) 
            for name in self.queue_names
        }
        return self.queues
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up queues (asyncio.Queue doesn't need explicit cleanup)."""
        self.queues.clear()
        return False


class ResourceManager(AbstractContextManager):
    def __init__(
            self,
            db_factory: Callable[[], Any],
            mq_factory: Callable[[], Any],
            *,
            logger: Optional[logging.Logger] = None,
        ):
        self._db_factory = db_factory
        self._mq_factory = mq_factory

        self._db: Any | None = None
        self._mq: Any | None = None

        # Per-resource substacks enable safe early release
        self._db_stack: ExitStack | None = None
        self._mq_stack: ExitStack | None = None

        # Top-level stack owns all substacks (including attachments)
        self._stack = ExitStack()
        self._active = False

        # Arbitrary attached contexts by name
        self._attached: Dict[str, Tuple[ExitStack, Any]] = {}
        self._name_counter = 0

        self._logger = logger or logging.getLogger("ResourceManager")
        self._t0: float | None = None

        self._metrics = {
            "acquire_ms": {"db": None, "mq": None},
            "release_ms": {"db": None, "mq": None},
            "attached": {},
            "context_ms": None,
        }

    # ------------------------------------------------------------------
    # Context manager protocol
    # ------------------------------------------------------------------
    def __enter__(self) -> "ResourceManager":
        self._active = True
        self._t0 = time.perf_counter()
        self._log("context.enter")
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        if exc_type:
            self._log("context.error", error=f"{exc_type.__name__}: {exc}")

        ok = self._stack.__exit__(exc_type, exc, tb)

        # Reset local handles (ExitStack already closed any open resources)
        self._db = None
        self._mq = None
        self._db_stack = None
        self._mq_stack = None
        self._attached.clear()
        self._active = False

        ctx_ms = (time.perf_counter() - (self._t0 or time.perf_counter())) * 1000.0
        self._metrics["context_ms"] = round(ctx_ms, 3)
        self._log("context.exit", ms=self._metrics["context_ms"])
        return ok  # False -> propagate exceptions

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_database(self) -> Any:
        if self._db is None:
            self._db = self._acquire("db", self._db_factory)
        return self._db

    def get_message_queue(self) -> Any:
        if self._mq is None:
            self._mq = self._acquire("mq", self._mq_factory)
        return self._mq

    def release_database(self) -> bool:
        return self._release("db")

    def release_message_queue(self) -> bool:
        return self._release("mq")

    def attach_context(self, cm, *, name: str | None = None, description: str = "") -> Any:
        """Attach any extra context manager (e.g., tempfile.TemporaryDirectory()).
        Returns the value from cm.__enter__().
        """
        self._ensure_active()
        res_name = name or self._auto_name(cm)
        self._log("attach.start", resource=res_name, description=description or type(cm).__name__)

        start = time.perf_counter()
        sub = ExitStack()
        self._stack.enter_context(sub)
        obj = sub.enter_context(cm)
        elapsed = (time.perf_counter() - start) * 1000.0

        self._attached[res_name] = (sub, obj)
        self._metrics["attached"][res_name] = {"acquire_ms": round(elapsed, 3), "release_ms": None}
        self._log("attach.ok", resource=res_name, ms=self._metrics["attached"][res_name]["acquire_ms"])
        return obj

    def release_context(self, name: str) -> bool:
        """Early release of an attached context by name."""
        self._ensure_active()
        entry = self._attached.get(name)
        if not entry:
            return False
        sub, _obj = entry
        self._log("release.start", resource=name)
        start = time.perf_counter()
        sub.close()  # idempotent
        elapsed = (time.perf_counter() - start) * 1000.0
        self._metrics["attached"][name]["release_ms"] = round(elapsed, 3)
        del self._attached[name]
        self._log("release.ok", resource=name, ms=self._metrics["attached"][name]["release_ms"])
        return True

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------
    def metrics_snapshot(self) -> dict:
        return {
            "acquire_ms": dict(self._metrics["acquire_ms"]),
            "release_ms": dict(self._metrics["release_ms"]),
            "attached": {k: dict(v) for k, v in self._metrics["attached"].items()},
            "context_ms": self._metrics["context_ms"],
            "active": self._active,
        }

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _acquire(self, which: str, factory: Callable[[], Any]) -> Any:
        self._ensure_active()
        self._log("acquire.start", resource=which)
        start = time.perf_counter()

        sub = ExitStack()
        self._stack.enter_context(sub)

        try:
            obj = factory()
            if hasattr(obj, "__enter__") and hasattr(obj, "__exit__"):
                obj = sub.enter_context(obj)  # context-manager resource
            else:
                closer = (
                    getattr(obj, "close", None)
                    or getattr(obj, "shutdown", None)
                    or getattr(obj, "disconnect", None)
                )
                if callable(closer):
                    sub.callback(closer)
                else:
                    raise TypeError(
                        f"{which} factory returned a non-context object without a close/shutdown/disconnect."
                    )

            elapsed = (time.perf_counter() - start) * 1000.0
            self._metrics["acquire_ms"][which] = round(elapsed, 3)
            self._log("acquire.ok", resource=which, ms=self._metrics["acquire_ms"][which])

            if which == "db":
                self._db_stack = sub
            else:
                self._mq_stack = sub
            return obj

        except Exception as e:
            try:
                sub.close()
            finally:
                pass
            self._log("acquire.error", resource=which, error=f"{type(e).__name__}: {e}")
            raise

    def _release(self, which: str) -> bool:
        self._ensure_active()
        stack_attr = "_db_stack" if which == "db" else "_mq_stack"
        obj_attr = "_db" if which == "db" else "_mq"

        sub: ExitStack | None = getattr(self, stack_attr)
        if not sub:
            return False

        self._log("release.start", resource=which)
        start = time.perf_counter()
        sub.close()
        setattr(self, stack_attr, None)
        setattr(self, obj_attr, None)

        elapsed = (time.perf_counter() - start) * 1000.0
        self._metrics["release_ms"][which] = round(elapsed, 3)
        self._log("release.ok", resource=which, ms=self._metrics["release_ms"][which])
        return True

    def _auto_name(self, cm: Any) -> str:
        self._name_counter += 1
        return f"ctx:{type(cm).__name__}:{self._name_counter}"

    def _ensure_active(self) -> None:
        if not self._active:
            raise RuntimeError("Use ResourceManager inside a 'with' block before acquiring resources.")

    def _log(self, event: str, **fields: Any) -> None:
        msg = f"{event} " + " ".join(f"{k}={v}" for k, v in fields.items())
        self._logger.info(msg)
    
    # Factory methods for pipeline
    @classmethod
    def for_pipeline(
            cls,
            db_path: str,
            queue_maxsize: int = 1000,
            *,
            setup_schema: bool = True,
            logger: Optional[logging.Logger] = None
        ) -> "ResourceManager":
        """Create a ResourceManager for ETL pipeline with database and queues."""
        def db_factory():
            return DatabaseManager(db_path, setup_schema=setup_schema)
        
        def mq_factory():
            return QueueManager(
                queue_maxsize, 
                queue_names=["ingest", "db", "outbound", "db_failures", "dead_letter"]
            )
        
        return cls(db_factory=db_factory, mq_factory=mq_factory, logger=logger)
