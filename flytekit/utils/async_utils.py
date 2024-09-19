import asyncio
import contextvars
import functools
from concurrent.futures import ThreadPoolExecutor
from types import CoroutineType
import sys

from typing_extensions import Any, Callable

from flytekit.loggers import logger

AsyncFuncType = Callable[[Any], CoroutineType]
Synced = Callable[[Any], Any]


def ensure_no_loop(error_msg: str):
    try:
        asyncio.get_running_loop()
        raise AssertionError(error_msg)
    except RuntimeError as e:
        if "no running event loop" not in str(e):
            logger.error(f"Unknown RuntimeError {str(e)}")
            raise


def get_or_create_loop(use_windows: bool = False) -> asyncio.AbstractEventLoop:
    try:
        running_loop = asyncio.get_running_loop()
        return running_loop
    except RuntimeError as e:
        if "no running event loop" not in str(e):
            logger.error(f"Unknown RuntimeError when getting loop {str(e)}")
            raise

    if sys.platform == "win32" and use_windows:
        loop = asyncio.WindowsSelectorEventLoopPolicy().new_event_loop()
    else:
        loop = asyncio.new_event_loop()
    # Intentionally not calling asyncio.set_event_loop(loop)
    # maybe add signal handlers
    return loop


def top_level_sync(func: AsyncFuncType, *args, **kwargs):
    """
    Make sure there is no current loop. Then run the func in a new loop
    """
    ensure_no_loop(f"Calling {func.__name__} when event loop active not allowed")
    coro = func(*args, **kwargs)
    return asyncio.run(coro)


def top_level_sync_wrapper(func: AsyncFuncType) -> Synced:
    """Given a function, make so can be called in async or blocking contexts

    Leave obj=None if defining within a class. Pass the instance if attaching
    as an attribute of the instance.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return top_level_sync(func, *args, **kwargs)

    return wrapper


class ContextExecutor(ThreadPoolExecutor):
    def __init__(self):
        self.context = contextvars.copy_context()
        super().__init__(initializer=self._set_child_context)

    def _set_child_context(self):
        for var, value in self.context.items():
            var.set(value)
