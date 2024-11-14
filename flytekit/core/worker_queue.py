from __future__ import annotations

import asyncio
import atexit
import threading
import typing
from dataclasses import dataclass

from flytekit.loggers import logger
from flytekit.models.core.execution import WorkflowExecutionPhase

if typing.TYPE_CHECKING:
    from flytekit.configuration import SerializationSettings
    from flytekit.core.base_task import PythonTask
    from flytekit.core.launch_plan import LaunchPlan
    from flytekit.core.reference_entity import ReferenceEntity
    from flytekit.core.workflow import WorkflowBase
    from flytekit.remote.remote_callable import RemoteEntity

    RunnableEntity = typing.Union[WorkflowBase, LaunchPlan, PythonTask, ReferenceEntity, RemoteEntity]
    from flytekit.remote import FlyteRemote, FlyteWorkflowExecution


@dataclass
class WorkItem:
    entity: RunnableEntity
    input_kwargs: dict[str, typing.Any]
    fut: asyncio.Future
    result: typing.Any = None
    error: typing.Optional[BaseException] = None

    wf_exec: typing.Optional[FlyteWorkflowExecution] = None

    def set_result(self, result: typing.Any):
        print(f"Setting result in State for {self.wf_exec.id.name} on thread {threading.current_thread().name}")
        self.result = result
        # need to convert from literals resolver to literals and then to python native.
        self.fut._loop.call_soon_threadsafe(self.fut.set_result, result)

    def set_error(self, e: BaseException):
        print(f"Setting error in State for {self.wf_exec.id.name} on thread {threading.current_thread().name} to {e}")
        self.error = e
        self.fut.set_exception(e)

    def set_exec(self, wf_exec: FlyteWorkflowExecution):
        self.wf_exec = wf_exec


class Informer(typing.Protocol):
    def watch(self, work: WorkItem): ...


class PollingInformer:
    def __init__(self, remote: FlyteRemote, loop: asyncio.AbstractEventLoop):
        self.remote: FlyteRemote = remote
        self.__loop = loop

    async def watch_one(self, work: WorkItem):
        print(f"Starting watching execution {work.wf_exec.id} on {threading.current_thread().name}")
        while True:
            # not really async but pretend it is for now, change to to_thread in the future.
            print(f"Looping on {work.wf_exec.id.name}")
            self.remote.sync_execution(work.wf_exec)
            if work.wf_exec.is_done:
                print(f"Execution {work.wf_exec.id.name} is done.")
                break
            await asyncio.sleep(2)

        # set results
        if work.wf_exec.closure.phase == WorkflowExecutionPhase.SUCCEEDED:
            # need to convert from literals resolver to literals and then to python native.
            results = work.wf_exec.outputs
            work.set_result(results)
        else:
            # set an exception on the future, read the error from the execution object if any.
            work.set_error(Exception("Execution failed"))

    def watch(self, work: WorkItem):
        coro = self.watch_one(work)
        # both of these seem to work, but I think run_coroutine_threadsafe makes more sense in case *this* thread is
        # ever different than the thread that self.__loop is running on.
        # t = self.__loop.create_task(coro)
        f = asyncio.run_coroutine_threadsafe(coro, self.__loop)
        # Just print a message with the future, no need to return it anywhere, unless we want to pass it back to
        # launch_and_start_watch and save this also in the State object somewhere.
        print(f"Started watch with future {f}")


class Controller:
    def __init__(self, remote: FlyteRemote, ss: SerializationSettings):
        # Set up things for this controller to operate
        self.__lock = threading.Lock()
        self.stop_condition = threading.Event()
        # add selector policy to loop selection
        self.__loop = asyncio.new_event_loop()
        self.__runner_thread: threading.Thread = threading.Thread(
            target=self._execute, daemon=True, name="controller-loop-runner"
        )
        self.__runner_thread.start()
        atexit.register(self._close)

        # Things for actually kicking off and monitoring
        self.entries: typing.Dict[str, typing.List[WorkItem]] = {}
        self.informer = PollingInformer(remote=remote, loop=self.__loop)
        self.remote = remote
        self.ss = ss

    def _close(self) -> None:
        if self.__loop:
            self.__loop.stop()

    def _execute(self) -> None:
        loop = self.__loop
        try:
            loop.run_forever()
        finally:
            loop.close()

    def launch_and_start_watch(self, key: str, idx):
        """state reconciliation, not sure if this is really reconciliation"""
        print(f"In launch and watch, {key=}, on {threading.current_thread().name}\n")

        # add lock maybe when accessing self.entries
        if key not in self.entries:
            logger.error(f"Key {key} not found in entries")
            return

        state = self.entries[key][idx]
        if state.result is None and state.error is None:
            # need to add try/catch block since the execution might already exist
            # if the execution already exists we should fetch the inputs. If the inputs are the same, then we should
            # start watching it.
            # need to integrate consistent naming of the execution. will need to be some combination of the parent
            # eager task's execution name, with order which these sub-entities are called, and the input_kwargs
            # need a consistent tag so it's searchable
            wf_exec = self.remote.execute(
                entity=state.entity,
                inputs=state.input_kwargs,
                version=self.ss.version,
                image_config=self.ss.image_config,
            )
            print(f"Successfully started execution {wf_exec.id.name} on {threading.current_thread().name}")
            state.set_exec(wf_exec)

            # if successful then start watch on the execution
            self.informer.watch(state)

    def add(
        self, task_loop: asyncio.AbstractEventLoop, entity: RunnableEntity, input_kwargs: dict[str, typing.Any]
    ) -> asyncio.Future:
        """
        same as the current WorkerQueue.add
        """
        # when we do the persisting part, need to check to see if we've already run this execution (will have to match
        # on entity and inputs or something). solve later.

        # need to also check to see if the entity has already been registered, and if not, register it.
        fut = task_loop.create_future()
        i = WorkItem(entity=entity, input_kwargs=input_kwargs, fut=fut)
        # add lock?
        # also, even if we don't do persisting to disk, should make this data structure more capable.
        # had to make the value a list because the same entity can be called multiple times (as in the case in helper)
        # Also don't really need to keep track of it, but need it for the Deck/observability
        # and maybe below
        if entity.name not in self.entries:
            self.entries[entity.name] = []
        self.entries[entity.name].append(i)
        idx = len(self.entries[entity.name]) - 1

        # trigger a run of the launching function.
        # below: it may be better to pass the state object here by using primitives, name and idx, rather than the
        # state object itself. think about it.
        self.__loop.call_soon_threadsafe(self.launch_and_start_watch, entity.name, idx)

        return fut
