import os
import sys
import types
import importlib
from pathlib import Path
import tempfile
import textwrap
import contextlib

# Ensure repository root is on path
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from plugins import (
    BaseWorker,
    load_plugins,
    get_worker,
    get_worker_instance,
    unregister_worker_instance,
    WorkerPluginMeta,
)
from task_scheduler.task_types import Task


def _snapshot_and_clear_registry():
    saved_registry = dict(WorkerPluginMeta.registry)
    saved_instances = dict(WorkerPluginMeta.instances)
    WorkerPluginMeta.registry.clear()
    WorkerPluginMeta.instances.clear()
    return saved_registry, saved_instances


def _restore_registry(saved_registry, saved_instances):
    WorkerPluginMeta.registry.clear()
    WorkerPluginMeta.registry.update(saved_registry)
    WorkerPluginMeta.instances.clear()
    WorkerPluginMeta.instances.update(saved_instances)


def test_registration_and_get_worker():
    saved_registry, saved_instances = _snapshot_and_clear_registry()
    try:
        class SimpleWorker(BaseWorker):
            name = "SimpleWorker"
            def __init__(self, worker_id: str):
                self.worker_id = worker_id
            def run(self):
                return "running"
            def execute_task(self, task: Task):
                return task

        # Class should be auto-registered
        assert "SimpleWorker" in WorkerPluginMeta.registry
        # get_worker returns a new instance
        w = get_worker("SimpleWorker", worker_id="w1")
        assert isinstance(w, SimpleWorker)
        assert w.worker_id == "w1"
        # Instance should be tracked by instances registry
        assert f"SimpleWorker-w1" in WorkerPluginMeta.instances
    finally:
        _restore_registry(saved_registry, saved_instances)


def test_instance_tracking_and_unregister():
    saved_registry, saved_instances = _snapshot_and_clear_registry()
    try:
        class TrackWorker(BaseWorker):
            name = "TrackWorker"
            def __init__(self, worker_id: str):
                self.worker_id = worker_id
            def run(self):
                pass
            def execute_task(self, task: Task):
                pass

        w = get_worker("TrackWorker", worker_id="abc")
        inst_name = f"TrackWorker-abc"
        assert get_worker_instance(inst_name) is w
        # Unregister and verify removal
        assert unregister_worker_instance(inst_name) is True
        # Second unregister is a no-op
        assert unregister_worker_instance(inst_name) is False
    finally:
        _restore_registry(saved_registry, saved_instances)


def test_duplicate_plugin_name_raises():
    saved_registry, saved_instances = _snapshot_and_clear_registry()
    try:
        class One(BaseWorker):
            name = "DupName"
            def __init__(self, worker_id: str):
                self.worker_id = worker_id
            def run(self):
                pass
            def execute_task(self, task: Task):
                pass

        # Defining a second class with same name should raise
        import pytest
        with pytest.raises(ValueError):
            class Two(BaseWorker):
                name = "DupName"
                def __init__(self, worker_id: str):
                    self.worker_id = worker_id
                def run(self):
                    pass
                def execute_task(self, task: Task):
                    pass
    finally:
        _restore_registry(saved_registry, saved_instances)


def test_validation_errors_for_bad_plugins():
    saved_registry, saved_instances = _snapshot_and_clear_registry()
    try:
        import pytest
        # Missing name
        with pytest.raises(TypeError):
            class NoName(BaseWorker):
                def __init__(self, worker_id: str):
                    self.worker_id = worker_id
                def run(self):
                    pass
                def execute_task(self, task: Task):
                    pass

        # Bad run signature
        with pytest.raises(TypeError):
            class BadRun(BaseWorker):
                name = "BadRun"
                def __init__(self, worker_id: str):
                    self.worker_id = worker_id
                def run(self, extra):
                    pass
                def execute_task(self, task: Task):
                    pass

        # execute_task with wrong signature (too few params)
        with pytest.raises(TypeError):
            class BadExecParams(BaseWorker):
                name = "BadExecParams"
                def __init__(self, worker_id: str):
                    self.worker_id = worker_id
                def run(self):
                    pass
                def execute_task(self):  # Missing task parameter
                    pass


        # Wrong type annotation for task parameter
        with pytest.raises(TypeError):
            class BadAnnot(BaseWorker):
                name = "BadAnnot"
                def __init__(self, worker_id: str):
                    self.worker_id = worker_id
                def run(self):
                    pass
                def execute_task(self, task: int):
                    pass

        # Instance validation: bad instance attributes
        class Good(BaseWorker):
            name = "Good"
            def __init__(self, worker_id: str):
                self.worker_id = worker_id
            def run(self):
                pass
            def execute_task(self, task: Task):
                pass

        # Creating instance with empty worker_id should raise from instance validation
        with pytest.raises(TypeError):
            Good("")
    finally:
        _restore_registry(saved_registry, saved_instances)


def test_load_plugins_discovers_modules(tmp_path):
    saved_registry, saved_instances = _snapshot_and_clear_registry()
    try:
        # Create a temporary plugin module inside the real plugins package directory
        # so load_plugins can discover it via pkgutil.iter_modules
        plugins_dir = Path(REPO_ROOT) / "plugins"
        temp_module = plugins_dir / "temp_test_plugin.py"
        temp_module.write_text(textwrap.dedent(
            """
            import sys, os
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            from task_scheduler.task_types import Task
            from . import BaseWorker

            class TempTestWorker(BaseWorker):
                name = "TempTestWorker"
                def __init__(self, worker_id: str):
                    self.worker_id = worker_id
                def run(self):
                    pass
                def execute_task(self, task: Task):
                    return None
            """
        ))
        try:
            # Ensure module isn't already imported and remove from registry if it exists
            module_name = "plugins.temp_test_plugin"
            if module_name in sys.modules:
                del sys.modules[module_name]
            if "TempTestWorker" in WorkerPluginMeta.registry:
                del WorkerPluginMeta.registry["TempTestWorker"]
            
            # Load plugins - should import our temp module
            load_plugins(reload=False)  # Use reload=False to avoid issues
            assert "TempTestWorker" in WorkerPluginMeta.registry
            w = get_worker("TempTestWorker", worker_id="xyz")
            assert f"TempTestWorker-xyz" in WorkerPluginMeta.instances
        finally:
            # Clean up module from sys.modules
            if "plugins.temp_test_plugin" in sys.modules:
                del sys.modules["plugins.temp_test_plugin"]
            with contextlib.suppress(Exception):
                temp_module.unlink()
            # Remove compiled artifacts if created
            pycache = plugins_dir / "__pycache__"
            if pycache.exists():
                for f in pycache.glob("temp_test_plugin*"):
                    with contextlib.suppress(Exception):
                        f.unlink()
    finally:
        _restore_registry(saved_registry, saved_instances)