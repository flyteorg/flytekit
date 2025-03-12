"""Manages an async event loop on another thread. Developers should only require to call
sync to use the managed loop:

from flytekit.tools.asyn import run_sync

async def async_add(a: int, b: int) -> int:
    return a + b

result = run_sync(async_add, a=10, b=12)
"""

import asyncio
import atexit
import functools
import os
import threading
from contextlib import contextmanager
from typing import Any, Awaitable, Callable, TypeVar

from typing_extensions import ParamSpec

from flytekit.loggers import logger

T = TypeVar("T")

P = ParamSpec("P")


@contextmanager
def _selector_policy():
    original_policy = asyncio.get_event_loop_policy()
    try:
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        yield
    finally:
        asyncio.set_event_loop_policy(original_policy)


class _TaskRunner:
    """A task runner that runs an asyncio event loop on a background thread."""

    def __init__(self) -> None:
        self.__loop: asyncio.AbstractEventLoop | None = None
        self.__runner_thread: threading.Thread | None = None
        self.__lock = threading.Lock()
        atexit.register(self._close)

    def _close(self) -> None:
        if self.__loop:
            self.__loop.stop()

    def _execute(self) -> None:
        loop = self.__loop
        assert loop is not None
        try:
            loop.run_forever()
        finally:
            loop.close()

    def get_exc_handler(self):
        def exc_handler(loop, context):
            logger.error(
                f"Taskrunner for {self.__runner_thread.name if self.__runner_thread else 'no thread'} caught"
                f" exception in {loop}: {context}"
            )

        return exc_handler

    def run(self, coro: Any) -> Any:
        """Synchronously run a coroutine on a background thread."""
        name = f"{threading.current_thread().name} : loop-runner"
        with self.__lock:
            if self.__loop is None:
                with _selector_policy():
                    self.__loop = asyncio.new_event_loop()

                exc_handler = self.get_exc_handler()
                self.__loop.set_exception_handler(exc_handler)
                self.__runner_thread = threading.Thread(target=self._execute, daemon=True, name=name)
                self.__runner_thread.start()
        fut = asyncio.run_coroutine_threadsafe(coro, self.__loop)

        res = fut.result(None)

        return res


class _AsyncLoopManager:
    def __init__(self):
        self._runner_map: dict[str, _TaskRunner] = {}

    def run_sync(self, coro_func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """
        This should be called from synchronous functions to run an async function.
        """
        name = threading.current_thread().name + f"PID:{os.getpid()}"
        coro = coro_func(*args, **kwargs)
        if name not in self._runner_map:
            if len(self._runner_map) > 500:
                logger.warning(
                    "More than 500 event loop runners created!!! This could be a case of runaway recursion..."
                )
            self._runner_map[name] = _TaskRunner()
        return self._runner_map[name].run(coro)

    def synced(self, coro_func: Callable[P, Awaitable[T]]) -> Callable[P, T]:
        """Make loop run coroutine until it returns. Runs in other thread"""

        @functools.wraps(coro_func)
        def wrapped(*args: Any, **kwargs: Any) -> T:
            return self.run_sync(coro_func, *args, **kwargs)

        return wrapped


loop_manager = _AsyncLoopManager()
run_sync = loop_manager.run_sync
