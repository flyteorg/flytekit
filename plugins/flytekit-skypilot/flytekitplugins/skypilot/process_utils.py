import asyncio
import concurrent.futures as cf
import multiprocessing
import traceback
import types
from contextlib import asynccontextmanager
from typing import Any, Callable, Optional

import psutil
from flytekitplugins.skypilot.constants import COROUTINE_INTERVAL, MINIMUM_SLEEP

from flytekit import logger


async def try_cancel_task(task: asyncio.Task):
    if task.cancel():
        try:
            await task
        except asyncio.exceptions.CancelledError:
            pass


class SubtaskManager:
    def __init__(self):
        self.tasks: list[asyncio.Task] = []

    def create_task(self, coro: types.CoroutineType) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self.tasks.append(task)
        return task

    async def cancel_all(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()

        # Wait for all tasks to complete their cancellation
        await asyncio.gather(*self.tasks, return_exceptions=True)

    def remove_done_tasks(self):
        self.tasks = [task for task in self.tasks if not task.done()]

    async def wait_first_done(self):
        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            await try_cancel_task(task)
        await asyncio.sleep(MINIMUM_SLEEP)
        return done, pending

    async def wait_all_done(self):
        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.ALL_COMPLETED)
        for task in pending:
            await try_cancel_task(task)
        await asyncio.sleep(MINIMUM_SLEEP)
        return done


@asynccontextmanager
async def manage_subtasks():
    manager = SubtaskManager()
    try:
        yield manager
    finally:
        await manager.cancel_all()


class EventHandler(object):
    def __init__(self) -> None:
        self._cancel_event = asyncio.Event()
        self._failed_event = asyncio.Event()
        self._launch_done_event = asyncio.Event()
        self._finished_event = asyncio.Event()

    @property
    def cancel_event(self):
        return self._cancel_event

    @property
    def failed_event(self):
        return self._failed_event

    @property
    def launch_done_event(self):
        return self._launch_done_event

    @property
    def finished_event(self):
        return self._finished_event

    def is_terminal(self):
        return self.cancel_event.is_set() or self.failed_event.is_set() or self.finished_event.is_set()

    async def async_terminal(self):
        async with manage_subtasks() as manager:
            manager.create_task(self.cancel_event.wait())
            manager.create_task(self.failed_event.wait())
            manager.create_task(self.finished_event.wait())
            await manager.wait_first_done()
            return True

    def __repr__(self) -> str:
        return (
            f"EventHandler(cancel_event={self.cancel_event.is_set()},"
            f"failed_event={self.failed_event.is_set()},"
            f"launch_done_event={self.launch_done_event.is_set()},"
            f"finished_event={self.finished_event.is_set()})"
        )


class ClusterEventHandler(EventHandler):
    def __init__(self) -> None:
        super().__init__()
        self.task_handlers: list[EventHandler] = []
        self._cluster_handler = EventHandler()

    @property
    def cancel_event(self):
        return self._cluster_handler.cancel_event

    @property
    def failed_event(self):
        return self._cluster_handler.failed_event

    @property
    def launch_done_event(self):
        return self._cluster_handler.launch_done_event

    @property
    def finished_event(self):
        return self._cluster_handler.finished_event

    def is_terminal(self):
        return self.cancel_event.is_set() or self.failed_event.is_set() or self.all_task_terminal()

    async def async_terminal(self):
        async with manage_subtasks() as manager:
            manager.create_task(self.cancel_event.wait())
            manager.create_task(self.failed_event.wait())
            manager.create_task(self.async_all_task_terminal())
            await manager.wait_first_done()
            return True

    def __repr__(self) -> str:
        return (
            f"ClusterEventHandler(cancel_event={self.cancel_event.is_set()},"
            f"failed_event={self.failed_event.is_set()},"
            f"launch_done_event={self.launch_done_event.is_set()},"
            f"finished_event={self.finished_event.is_set()},"
        )

    def register_task_handler(self, task_handler: EventHandler):
        self.task_handlers.append(task_handler)

    def all_task_terminal(self):
        for task_handler in self.task_handlers:
            if not task_handler.is_terminal():
                return False
        return True

    async def async_all_task_terminal(self):
        async with manage_subtasks() as manager:
            for task_handler in self.task_handlers:
                manager.create_task(task_handler.async_terminal())
            await manager.wait_all_done()
            return True

    def reset(self):
        self._cluster_handler.cancel_event.clear()
        self._cluster_handler.failed_event.clear()
        self._cluster_handler.launch_done_event.clear()
        self._cluster_handler.finished_event.clear()


class WrappedProcess(multiprocessing.Process):
    """
    Wrapper for multiprocessing.Process to catch exceptions in the target function
    """

    def __init__(self, *args, **kwargs) -> None:
        multiprocessing.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = multiprocessing.Pipe()
        self._exception = None

    def run(self):
        try:
            multiprocessing.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb))
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


class BaseProcessHandler:
    def __init__(self, fn: Callable, event_handler: EventHandler = None, name: str = None) -> None:
        if event_handler is None:
            event_handler = EventHandler()
        self._check_interval = COROUTINE_INTERVAL
        self._event_handler = event_handler
        self._task: Optional[asyncio.Task] = None
        self._name = name

    async def status_poller(self, extra_events: list[Callable] = None, timeout: int = None) -> tuple[bool, Any]:
        raise NotImplementedError

    def create_task(self, extra_events: list[Callable] = None, timeout: Optional[int] = None):
        self._task = asyncio.create_task(self.status_poller(extra_events=extra_events, timeout=timeout))

    def clean_up(self):
        raise NotImplementedError

    def time_exceeded(self, process_time: int, timeout: Optional[int]):
        if not timeout:
            return False
        return process_time >= timeout

    async def cancel(self):
        if self._task and not self._task.done():
            await try_cancel_task(self._task)
            await asyncio.sleep(MINIMUM_SLEEP)
            self.clean_up()

    def done(self):
        return self._task and self._task.done()


class BlockingProcessHandler(BaseProcessHandler):
    """
    function launcher in a separate process, suitable for functions without return value and class functions where the class is not picklable
    """

    def __init__(self, fn: Callable, event_handler: EventHandler = None, name: str = None) -> None:
        super().__init__(fn, event_handler, name)
        self._process = WrappedProcess(target=fn)
        self._process.start()

    async def status_poller(self, extra_events: list[Callable] = None, timeout: int = None) -> tuple[bool, Any]:
        if extra_events is None:
            extra_events = [self._event_handler.is_terminal]
        else:
            extra_events.append(self._event_handler.is_terminal)
        process_time = 0
        while True:
            try:
                if self._process.exitcode is not None:
                    break
            except ValueError:
                return False, None
            logger.warning(f"{self._name} is stuck")
            for event in extra_events:
                if event():
                    self.clean_up()
                    return False, None
            if self.time_exceeded(process_time, timeout):
                self.clean_up()
                raise asyncio.exceptions.TimeoutError(f"{self._name} time exceeded")
            await asyncio.sleep(self._check_interval)
            process_time += self._check_interval

        launch_exception = None
        if self._process.exception is not None:
            launch_exception = self._process.exception
        else:
            self._event_handler.launch_done_event.set()
        self.clean_up()
        if launch_exception is not None:
            raise Exception(launch_exception)
        return True, None

    def clean_up(self):
        self._process.terminate()
        pid_process = psutil.Process(self._process.pid)
        pid_process.kill()


class ConcurrentProcessHandler(BaseProcessHandler):
    """
    function launcher in a separate process, suitable for functions with return value
    """

    def __init__(self, fn: Callable, event_handler: EventHandler = None, name: str = None) -> None:
        super().__init__(fn, event_handler, name)
        self._fn = fn

    async def status_poller(self, timeout: int = None) -> tuple[bool, Any]:
        success, task_result, launch_exception = None, None, None
        timeout = timeout or (1 << 31 - 1)
        loop = asyncio.get_event_loop()
        with cf.ProcessPoolExecutor() as executor:
            fn_task = loop.run_in_executor(executor, self._fn)
            event_task = asyncio.create_task(self._event_handler.async_terminal())
            done, pending = await asyncio.wait(
                [fn_task, event_task], timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
            for undone in pending:
                await try_cancel_task(undone)
                if fn_task == undone:
                    if not event_task.done():
                        launch_exception = asyncio.exceptions.TimeoutError(f"{self._name} time exceeded")
                    success = False

            await asyncio.sleep(MINIMUM_SLEEP)
            if fn_task in done:
                try:
                    task_result = fn_task.result()
                    success = True
                except Exception as e:
                    launch_exception = e
                    success = False
            if not success:
                self.clean_up(executor)

        if success:
            self._event_handler.launch_done_event.set()
            return success, task_result

        if launch_exception is not None:
            raise launch_exception

        return success, launch_exception

    def clean_up(self, executor: cf.ProcessPoolExecutor):
        if executor._processes:
            for _, process in executor._processes.items():
                pid_process = psutil.Process(process.pid)
                # here we use SIGKILL bcus somehow SIGTERM is captured = =
                pid_process.kill()


class SkySubProcessError(Exception):
    pass
