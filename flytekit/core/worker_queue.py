from __future__ import annotations

import asyncio
import threading
import time
import typing
from dataclasses import dataclass

from flytekit.loggers import logger

if typing.TYPE_CHECKING:
    from flytekit.clients.friendly import SynchronousFlyteClient
    from flytekit.core.base_task import PythonTask
    from flytekit.core.reference_entity import ReferenceEntity
    from flytekit.core.workflow import WorkflowBase

    RunnableEntity = typing.Union[WorkflowBase, PythonTask, ReferenceEntity]


@dataclass
class WorkItem:
    loop: asyncio.AbstractEventLoop
    entity: RunnableEntity
    input_kwargs: dict[str, typing.Any]
    fut: asyncio.Future


def internal_function(**kwargs) -> str:
    msg = f"Internal function called with {kwargs}"
    print(msg)
    return f"Length is {len(msg)}"


def internal_function_int(**kwargs) -> int:
    msg = f"Internal int function called with {kwargs}"
    print(f"Length {len(msg)}: {msg}")
    return len(msg)


class WorkerQueue:
    """
    Global singleton class (maybe) that handles kicking things off.
    todo:async make a protocol out of this and update type in context manager
    """

    def __init__(self, client: SynchronousFlyteClient):
        self.client = client
        self.queue: typing.List[WorkItem] = []
        self.__lock = threading.Lock()
        self.stop_condition = threading.Event()
        self.__runner_thread = threading.Thread(target=self._process, name="WorkerQueue")
        self.__runner_thread.start()

    def _process(self):
        try:
            while True:
                if self.stop_condition.is_set():
                    break
                print(f"WorkerQueue looping on {len(self.queue)}...", flush=True)
                time.sleep(0.1)
                queue = []
                for work_item in self.queue:
                    if work_item.fut.done():
                        continue
                    queue.append(work_item)
                    print(f"Processing future: {work_item.fut}")
                    result = internal_function_int(**work_item.input_kwargs)
                    # Calling f.set_result directly from this thread causes the await to hang
                    work_item.fut._loop.call_soon_threadsafe(work_item.fut.set_result, result)

                with self.__lock:
                    self.queue = queue
        except Exception as e:
            logger.error(f"WorkerQueue raised error. Error: {e}")
        finally:
            self.stop_condition.set()
            logger.warning(f"WorkerQueue stopped unexpectedly. Queue items: {self.queue}")

    def add(
        self, loop: asyncio.AbstractEventLoop, entity: RunnableEntity, input_kwargs: dict[str, typing.Any]
    ) -> asyncio.Future:
        fut = loop.create_future()
        i = WorkItem(loop=loop, entity=entity, input_kwargs=input_kwargs, fut=fut)
        with self.__lock:
            self.queue.append(i)

        return fut

    def stop(self):
        self.stop_condition.set()
