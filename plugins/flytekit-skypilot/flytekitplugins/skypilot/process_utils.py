import asyncio
import multiprocessing
import traceback
from typing import Callable, Optional

from flytekitplugins.skypilot.constants import COROUTINE_INTERVAL

from flytekit import logger


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


class BlockingProcessHandler:
    def __init__(self, fn: Callable, event_handler: EventHandler = None, name: str = None) -> None:
        if event_handler is None:
            event_handler = EventHandler()
        self._process = WrappedProcess(target=fn)
        self._process.start()
        self._check_interval = COROUTINE_INTERVAL
        self._event_handler = event_handler
        self._task = None
        self._name = name

    async def status_poller(self, extra_events: list[Callable] = None, timeout: int = None) -> bool:
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
                return False
            logger.warning(f"{self._name} is stuck")
            for event in extra_events:
                if event():
                    self.clean_up()
                    return False
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
        return True

    def create_task(self, extra_events: list[Callable] = None, timeout: Optional[int] = None):
        self._task = asyncio.create_task(self.status_poller(extra_events=extra_events, timeout=timeout))

    def clean_up(self):
        self._process.terminate()
        self._process.join()
        self._process.close()

    def time_exceeded(self, process_time: int, timeout: Optional[int]):
        if not timeout:
            return False
        return process_time >= timeout

    def cancel(self):
        if self._task and not self._task.done():
            self.clean_up()

    def done(self):
        return self._task and self._task.done()
