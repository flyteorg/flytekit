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
        print(f"Created TaskRunner instance {id(self)} on PID {os.getpid()}!")

    def _close(self) -> None:
        if self.__loop:
            self.__loop.stop()

    def _execute(self) -> None:
        print(f"TaskRunner::_execute 1 in {os.getpid()}!")
        loop = self.__loop
        print(f"TaskRunner::_execute 2 in {os.getpid()}!")
        assert loop is not None
        print(f"TaskRunner::_execute 3 in {os.getpid()}!")
        try:
            print(f"Creating loop in {os.getpid()} !!!!!!!!!!!!!!!!!!!!!!!!!!!")
            loop.run_forever()
        finally:
            print(f"Closing loop!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            loop.close()

    def run(self, coro: Any) -> Any:
        """Synchronously run a coroutine on a background thread."""
        name = f"{threading.current_thread().name} :{os.getpid()}: loop-runner"
        print(f"Run in run funct !!!! {os.getpid()}")
        with self.__lock:
            print(f"Run in run funct 2 ")
            if self.__loop is None:
                print(f"Run in run funct - loop is none")
                with _selector_policy():
                    self.__loop = asyncio.new_event_loop()
                print(f"Run in run funct - about to create thread")
                self.__runner_thread = threading.Thread(target=self._execute, daemon=True, name=name)
                print(f"About to start thread in {os.getpid()} {name=}")
                self.__runner_thread.start()
            else:
                print(f"Run in run funct - loop is not none {id(self)} {self.__loop=}")
        print(f"Run in run funct 3")
        fut = asyncio.run_coroutine_threadsafe(coro, self.__loop)

        res = fut.result(None)
        print(f"Run in run funct 4")
        return res


class _AsyncLoopManager:
    def __init__(self):
        self._runner_map: dict[str, _TaskRunner] = {}

    def run_sync(self, coro_func: Callable[..., Awaitable[T]], *args, **kwargs) -> T:
        """
        This should be called from synchronous functions to run an async function.
        """
        name = threading.current_thread().name + f"pid:{os.getpid()}"
        print(f"Running {coro_func} in {name}")
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
