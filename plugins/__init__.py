import importlib
import inspect
import pkgutil
from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path
from typing import Dict, Type, List

import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from task_scheduler.task_types import Task


def _validate_class(cls: Type["BaseWorker"]) -> None:
    """Validate worker class attributes and method signatures at runtime.

    Ensures:
    - class attribute 'name' is a non-empty string
    - 'run(self)' exists and only takes 'self'
    - 'execute_task(self, task)' exists and takes at least (self, task)
      and, if annotated, the 'task' parameter is annotated as Task
    """
    errors: list[str] = []

    plugin_name = getattr(cls, "name", None)
    if not isinstance(plugin_name, str) or not plugin_name.strip():
        errors.append("Class attribute 'name' must be a non-empty string")

    run = getattr(cls, "run", None)
    if not callable(run):
        errors.append("Missing required method 'run(self)'")
    else:
        sig = inspect.signature(run)
        params = [p for p in sig.parameters.values()
                  if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
        if len(params) != 1:
            errors.append("Method 'run(self)' must accept only 'self'")

    execute_task = getattr(cls, "execute_task", None)
    if not callable(execute_task):
        errors.append("Missing required method 'execute_task(self, task)'")
    else:
        sig = inspect.signature(execute_task)
        params = [p for p in sig.parameters.values()
                  if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
        if len(params) < 2:
            errors.append("Method 'execute_task(self, task)' must accept at least (self, task)")
        else:
            task_param = params[1]
            if task_param.annotation is not inspect._empty and task_param.annotation is not Task:
                errors.append("Second parameter of 'execute_task' should be annotated as Task")

    if errors:
        raise TypeError(
            f"Invalid worker plugin '{cls.__name__}': " + "; ".join(errors)
        )


def _validate_instance(instance: "BaseWorker") -> None:
    """Validate a worker instance has required runtime attributes/methods."""
    errors: list[str] = []
    if not hasattr(instance, "name") or not isinstance(instance.name, str) or not instance.name.strip():
        errors.append("Instance attribute 'name' must be a non-empty string")
    if not hasattr(instance, "worker_id") or not isinstance(instance.worker_id, str) or not instance.worker_id.strip():
        errors.append("Instance attribute 'worker_id' must be a non-empty string")
    if not callable(getattr(instance, "run", None)):
        errors.append("Instance missing callable 'run'")
    if not callable(getattr(instance, "execute_task", None)):
        errors.append("Instance missing callable 'execute_task'")

    if errors:
        raise TypeError(
            f"Invalid worker instance for '{type(instance).__name__}': " + "; ".join(errors)
        )

class WorkerPluginMeta(ABCMeta):
    """ Metaclass that automatically registers concrete subclasses """
    registry: Dict[str, Type["BaseWorker"]] = {}
    instances: Dict[str, "BaseWorker"] = {}

    def __new__(mcls, name, bases, namespace, **kwargs):
        cls = super().__new__(mcls, name, bases, dict(namespace))

        # Skip abstract classes
        if inspect.isabstract(cls):
            return cls

        plugin_name = getattr(cls, "name", None)
        
        _validate_class(cls)

        prev = mcls.registry.get(plugin_name)
        if prev is not None and prev is not cls:
            raise ValueError(
                f"Duplicate plugin name '{plugin_name}': {prev.__name__} already registered"
            )

        mcls.registry[plugin_name] = cls
        return cls

    def __call__(cls, *args, **kwargs):
        instance = super().__call__(*args, **kwargs)
        
        _validate_instance(instance)
        
        instance_name = getattr(instance, "name", None)
        worker_id = getattr(instance, "worker_id", None)
        registry_name = f"{instance_name}-{worker_id}"
        if registry_name:
            WorkerPluginMeta.instances[registry_name] = instance
        return instance


def load_plugins(reload: bool = False) -> None:
    """ Import all plugin modules in this package """
    package_path = Path(__file__).parent
    package_name = __name__
    for module_info in pkgutil.iter_modules([str(package_path)]):
        if module_info.name == "base":
            continue
        module_name = f"{package_name}.{module_info.name}"
        module = importlib.import_module(module_name)
        if reload:
            importlib.reload(module)


def get_worker(name: str, *args, **kwargs) -> "BaseWorker":
    """ Return a worker instance for `name` """
    try:
        cls = WorkerPluginMeta.registry[name]
    except KeyError:
        available = ", ".join(sorted(WorkerPluginMeta.registry)) or "<none>"
        raise KeyError(f"No worker named '{name}'. Available: {available}")
    return cls(*args, **kwargs)


def get_worker_instance(name: str) -> "BaseWorker":
    """Return an existing worker instance registered by runtime name."""
    try:
        return WorkerPluginMeta.instances[name]
    except KeyError:
        available = ", ".join(sorted(WorkerPluginMeta.instances)) or "<none>"
        raise KeyError(f"No worker instance named '{name}'. Available: {available}")


def unregister_worker_instance(name: str) -> bool:
    """Unregister a worker instance by runtime name. Returns True if removed."""
    return WorkerPluginMeta.instances.pop(name, None) is not None


def get_all_plugins() -> List[Type["BaseWorker"]]:
    """Return all registered plugin classes."""
    return list(WorkerPluginMeta.registry.values())


class BaseWorker(ABC, metaclass=WorkerPluginMeta):
    """ Abstract base class for workers """
    name: str | None = None

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @abstractmethod
    def execute_task(self, task: Task):
        raise NotImplementedError


__all__ = [
    "BaseWorker",
    "load_plugins",
    "get_worker",
    "get_worker_instance",
    "unregister_worker_instance",
    "get_all_plugins",
    "WorkerPluginMeta",
]
