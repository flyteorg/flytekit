import asyncio
import multiprocessing
from typing import Callable
import traceback
from flytekitplugins.skypilot.constants import COROUTINE_INTERVAL


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
            f", launch_done_event={self.launch_done_event.is_set()},"
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
            f", launch_done_event={self.launch_done_event.is_set()},"
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
    def __init__(self, fn: Callable) -> None:
        self._process = WrappedProcess(target=fn)
        self._process.start()
        self._check_interval = COROUTINE_INTERVAL

    async def status_poller(self, event_handler: EventHandler):
        while self._process.exitcode is None:
            await asyncio.sleep(self._check_interval)
            if event_handler.is_terminal():
                self.clean_up()
                return
        launch_exception = None
        if self._process.exception is not None:
            launch_exception = self._process.exception
        else:
            event_handler.launch_done_event.set()
        self.clean_up()
        if launch_exception is not None:
            raise Exception(launch_exception)

    def get_task(self, event_handler: EventHandler):
        task = asyncio.create_task(self.status_poller(event_handler))
        return task

    def clean_up(self):
        self._process.terminate()
        self._process.join()
        self._process.close()

