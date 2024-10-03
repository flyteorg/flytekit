"""Manages an async event loop on another thread. Developers should only require to call
sync to use the managed loop:

from flytekit.tools.asyn import sync

async def async_add(a: int, b: int) -> int:
    return a + b

result = sync(async_add, a=10, b=12)
"""

import asyncio
import os
import threading
from contextlib import contextmanager


async def _runner(event, coro, result):
    try:
        result[0] = await coro
    except Exception as ex:
        result[0] = ex
    finally:
        event.set()


@contextmanager
def _selector_policy():
    original_policy = asyncio.get_event_loop_policy()
    try:
        if os.name == "nt" and hasattr(asyncio, "WindowsSelectorEventLoopPolicy"):
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

        yield
    finally:
        asyncio.set_event_loop_policy(original_policy)


class _AsyncLoopManager:
    def __init__(self):
        self._loop = None  # global event loop for any non-async instance
        self._lock = None  # global lock placeholder
        self._iothread = None

    @property
    def lock(self):
        if not self._lock:
            self._lock = threading.Lock()
        return self._lock

    @property
    def loop(self):
        if self._loop is None:
            with self.lock:
                # repeat the check just in case the loop got filled between the
                # previous two calls from another thread
                if self._loop is None:
                    with _selector_policy():
                        self._loop = asyncio.new_event_loop()
                    th = threading.Thread(target=self.loop.run_forever, name="flytekit-async")
                    th.daemon = True
                    th.start()
                    self._iothread = th
        return self._loop

    def sync(self, func, *args, **kwargs):
        """Make loop run coroutine until it returns. Runs in other thread"""
        if self.loop is None or self.loop.is_closed():
            raise RuntimeError("Loop is not running")
        try:
            loop0 = asyncio.events.get_running_loop()
            if loop0 is self.loop:
                raise NotImplementedError("Calling sync() from within a running loop")
        except NotImplementedError:
            raise
        except RuntimeError:
            pass
        coro = func(*args, **kwargs)
        result = [None]
        event = threading.Event()
        asyncio.run_coroutine_threadsafe(_runner(event, coro, result), self.loop)
        while True:
            # this loops allows thread to get interrupted
            if event.wait(1):
                break

        return_result = result[0]
        if isinstance(return_result, BaseException):
            raise return_result
        else:
            return return_result


_loop_manager = _AsyncLoopManager()
sync = _loop_manager.sync
