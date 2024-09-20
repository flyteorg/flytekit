import asyncio
import atexit
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from types import CoroutineType
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar

from flytekit.loggers import logger

AsyncFuncType = Callable[[Any], CoroutineType]
Synced = Callable[[Any], Any]
T = TypeVar("T")


def get_running_loop_if_exists() -> Optional[asyncio.AbstractEventLoop]:
    try:
        loop = asyncio.get_running_loop()
        return loop
    except RuntimeError as e:
        if "no running event loop" not in str(e):
            logger.error(f"Unknown RuntimeError {str(e)}")
            raise
    return None


def get_or_create_loop(use_windows: bool = False) -> asyncio.AbstractEventLoop:
    # todo: what happens if we remove this? never rely on the running loop
    #   something to test. import flytekit, inside an async function, what happens?
    #   import flytekit, inside a jupyter notebook (which sets its own loop)
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
    #   Unclear what the downside of this is. But should be better in the Jupyter case where it seems to
    #   co-opt set_event_loop somehow

    # maybe add signal handlers in the future

    return loop


class _CoroRunner:
    """
    Runs a coroutine and a loop for it on a background thread, in a blocking manner
    """

    def __init__(self) -> None:
        self.__io_loop: asyncio.AbstractEventLoop | None = None
        self.__runner_thread: threading.Thread | None = None
        self.__lock = threading.Lock()
        atexit.register(self._close)

    def _close(self) -> None:
        if self.__io_loop:
            self.__io_loop.stop()

    def _runner(self) -> None:
        loop = self.__io_loop
        assert loop is not None
        try:
            loop.run_forever()
        finally:
            loop.close()

    def run(self, coro: Any) -> Any:
        """
        This is a blocking function.
        Synchronously runs the coroutine on a background thread.
        """
        name = f"{threading.current_thread().name} - runner"
        with self.__lock:
            # remove before merging
            if f"{threading.current_thread().name} - runner" != name:
                raise AssertionError
            if self.__io_loop is None:
                self.__io_loop = asyncio.new_event_loop()
                self.__runner_thread = threading.Thread(target=self._runner, daemon=True, name=name)
                self.__runner_thread.start()
        logger.debug(f"Runner thread started {name}")
        fut = asyncio.run_coroutine_threadsafe(coro, self.__io_loop)
        res = fut.result(None)

        return res


_runner_map: dict[str, _CoroRunner] = {}


class MyContext(object):
    def __init__(self, vals: Optional[Dict[str, int]] = None):
        self.vals = vals


dummy_context: ContextVar[List[MyContext]] = ContextVar("dummy_context", default=[])
dummy_context.set([MyContext(vals={"depth": 0})])


def run_sync_new_thread(coro_function: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """
    Decorator to run a coroutine function with a loop that runs in a different thread.
    Always run in a new thread, even if no current thread is running.

    :param coro_function: A coroutine function
    """
    # if not inspect.iscoroutinefunction(coro_function):
    #     raise AssertionError

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        name = threading.current_thread().name
        logger.debug(f"Invoking coro_f synchronously in thread: {threading.current_thread().name}")
        inner = coro_function(*args, **kwargs)
        if name not in _runner_map:
            _runner_map[name] = _CoroRunner()
        return _runner_map[name].run(inner)

    wrapped.__doc__ = coro_function.__doc__
    return wrapped


class ContextExecutor(ThreadPoolExecutor):
    def __init__(self):
        self.context = copy_context()
        super().__init__(initializer=self._set_child_context)

    def _set_child_context(self):
        for var, value in self.context.items():
            var.set(value)
