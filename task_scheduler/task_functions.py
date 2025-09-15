import time
import random


def cpu_intensive_task(n: int) -> int:
    """CPU intensive task"""
    result = 0
    for i in range(n):
        result += i ** 2
    return result


def io_task(duration: float) -> str:
    """IO intensive task"""
    time.sleep(duration)
    return f"IO task completed after {duration}s"


def failing_task() -> str:
    """Task that fails"""
    raise ValueError("This task always fails")


def random_delay_task(delay_range: tuple = (0.5, 2.0)) -> str:
    """Task with random delay"""
    delay = random.uniform(*delay_range)
    time.sleep(delay)
    return f"Random delay task completed after {delay:.2f}s"


def data_processing_task(data: list) -> dict:
    """Data processing task"""
    result = {
        'sum': sum(data),
        'average': sum(data) / len(data) if data else 0,
        'max': max(data) if data else None,
        'min': min(data) if data else None,
        'count': len(data)
    }
    return result