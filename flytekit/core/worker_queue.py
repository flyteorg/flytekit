from __future__ import annotations

import typing
import asyncio
import threading
import time
from dataclasses import dataclass

if typing.TYPE_CHECKING:
    from flytekit.clients.friendly import SynchronousFlyteClient
    from flytekit.core.workflow import WorkflowBase
    from flytekit.core.base_task import PythonTask
    from flytekit.core.reference_entity import ReferenceEntity

    RunnableEntity = typing.Union[WorkflowBase, PythonTask, ReferenceEntity]


@dataclass
class WorkItem:
    loop: asyncio.AbstractEventLoop
    entity: RunnableEntity
    input_kwargs: dict[str, any]
    fut: asyncio.Future


def internal_function(**kwargs) -> str:
    msg = f"Internal function called with and {kwargs}"
    print(msg)
    return f"Length is {len(msg)}"


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
        self.__runner_thread = threading.Thread(target=self._process, daemon=True, name="WorkerQueue")
        self.__runner_thread.start()

    def _process(self):
        while True:
            if self.stop_condition.is_set():
                break
            print(f"WorkerQueue looping on {len(self.queue)}...")
            time.sleep(1)
            queue = []
            for work_item in self.queue:
                if work_item.fut.done():
                    continue
                queue.append(work_item)
                print(f"Processing future: {f}")
                result = internal_function(**work_item.input_kwargs)
                # Calling f.set_result directly from this thread causes the await to hang
                work_item.fut._loop.call_soon_threadsafe(work_item.fut.set_result, result)

            with self.__lock:
                self.queue = queue

    def add(self, loop: asyncio.AbstractEventLoop, entity: RunnableEntity, input_kwargs: dict[str, any]):
        i = WorkItem(loop=loop, entity=entity, input_kwargs=input_args, fut=loop.create_future())
        with self.__lock:
            self.queue.append(i)

    def stop(self):
        self.stop_condition.set()
