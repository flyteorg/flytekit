from __future__ import annotations

import asyncio
import atexit
import re
import threading
import typing
from dataclasses import dataclass

from flytekit.loggers import developer_logger, logger
from flytekit.models.core.execution import WorkflowExecutionPhase

from flytekit.core.base_task import PythonTask
from flytekit.configuration import SerializationSettings
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.reference_entity import ReferenceEntity
from flytekit.core.workflow import WorkflowBase

if typing.TYPE_CHECKING:
    from flytekit.remote.remote_callable import RemoteEntity
    RunnableEntity = typing.Union[WorkflowBase, LaunchPlan, PythonTask, ReferenceEntity, RemoteEntity]
    from flytekit.remote import FlyteRemote, FlyteWorkflowExecution

standard_output_format = re.compile(r"^o\d+$")

NODE_HTML_TEMPLATE = """
<style>
    #flyte-frame-container > div.active {{font-family: Open sans;}}
</style>

<style>
    #flyte-frame-container div.input-output {{
        font-family: monospace;
        background: #f0f0f0;
        padding: 10px 15px;
        margin: 15px 0;
    }}
</style>

<h3>{entity_type}: {entity_name}</h3>

<p>
    <strong>Execution:</strong>
    <a target="_blank" href="{url}">{execution_name}</a>
</p>

<details>
<summary>Inputs</summary>
<div class="input-output">{inputs}</div>
</details>

<details>
<summary>Outputs</summary>
<div class="input-output">{outputs}</div>
</details>

<hr>
"""


@dataclass
class WorkItem:
    entity: RunnableEntity
    input_kwargs: dict[str, typing.Any]
    fut: asyncio.Future
    result: typing.Any = None
    error: typing.Optional[BaseException] = None

    wf_exec: typing.Optional[FlyteWorkflowExecution] = None

    def set_result(self, result: typing.Any):
        developer_logger.debug(f"Setting result for {self.wf_exec.id.name} on thread {threading.current_thread().name}")
        self.result = result
        # need to convert from literals resolver to literals and then to python native.
        self.fut._loop.call_soon_threadsafe(self.fut.set_result, result)

    def set_error(self, e: BaseException):
        developer_logger.debug(
            f"Setting error for {self.wf_exec.id.name} on thread {threading.current_thread().name} to {e}"
        )
        self.error = e
        self.fut._loop.call_soon_threadsafe(self.fut.set_exception, e)

    def set_exec(self, wf_exec: FlyteWorkflowExecution):
        self.wf_exec = wf_exec


class Informer(typing.Protocol):
    def watch(self, work: WorkItem): ...


class PollingInformer:
    def __init__(self, remote: FlyteRemote, loop: asyncio.AbstractEventLoop):
        self.remote: FlyteRemote = remote
        self.__loop = loop

    async def watch_one(self, work: WorkItem):
        logger.debug(f"Starting watching execution {work.wf_exec.id} on {threading.current_thread().name}")
        while True:
            # not really async but pretend it is for now, change to to_thread in the future.
            developer_logger.debug(f"Looping on {work.wf_exec.id.name}")
            self.remote.sync_execution(work.wf_exec)
            if work.wf_exec.is_done:
                developer_logger.debug(f"Execution {work.wf_exec.id.name} is done.")
                break
            await asyncio.sleep(2)

        # set results
        # but first need to convert from literals resolver to literals and then to python native.
        if work.wf_exec.closure.phase == WorkflowExecutionPhase.SUCCEEDED:
            from flytekit.core.interface import Interface
            from flytekit.core.type_engine import TypeEngine

            if not work.entity.python_interface:
                for k, _ in work.entity.interface.outputs.items():
                    if not re.match(standard_output_format, k):
                        raise AssertionError(
                            f"Entity without python interface found, and output name {k} does not match standard format o[0-9]+"
                        )

                num_outputs = len(work.entity.interface.outputs)
                python_outputs_interface = {}
                # Iterate in order so that we add to the interface in the correct order
                for i in range(num_outputs):
                    key = f"o{i}"
                    # todo:async add a nicer error here
                    var_type = work.entity.interface.outputs[key].type
                    python_outputs_interface[key] = TypeEngine.guess_python_type(var_type)
                py_iface = Interface(inputs={}, outputs=python_outputs_interface)
            else:
                py_iface = work.entity.python_interface

            results = work.wf_exec.outputs.as_python_native(py_iface)

            work.set_result(results)
        elif work.wf_exec.closure.phase == WorkflowExecutionPhase.FAILED:
            from flytekit.exceptions.eager import EagerException

            exc = EagerException(f"Error executing {work.entity.name} with error: {work.wf_exec.closure.error}")
            work.set_error(exc)

    def watch(self, work: WorkItem):
        coro = self.watch_one(work)
        # both of these seem to work, but I think run_coroutine_threadsafe makes more sense in case *this* thread is
        # ever different than the thread that self.__loop is running on.
        # t = self.__loop.create_task(coro)
        f = asyncio.run_coroutine_threadsafe(coro, self.__loop)
        # Just print a message with the future, no need to return it anywhere, unless we want to pass it back to
        # launch_and_start_watch and save this also in the State object somewhere.
        developer_logger.debug(f"Started watch with future {f}")


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
            logger.info(f"Successfully started execution {wf_exec.id.name}")
            state.set_exec(wf_exec)

            # if successful then start watch on the execution
            self.informer.watch(state)

    def add(
        self, task_loop: asyncio.AbstractEventLoop, entity: RunnableEntity, input_kwargs: dict[str, typing.Any]
    ) -> asyncio.Future:
        """
        Add an entity along with the requested inputs to be submitted to Admin for running and return a future
        """
        # need to also check to see if the entity has already been registered, and if not, register it.
        fut = task_loop.create_future()
        i = WorkItem(entity=entity, input_kwargs=input_kwargs, fut=fut)

        # For purposes of awaiting an execution, we don't need to keep track of anything, but doing so for Deck
        if entity.name not in self.entries:
            self.entries[entity.name] = []
        self.entries[entity.name].append(i)
        idx = len(self.entries[entity.name]) - 1

        # trigger a run of the launching function.
        self.__loop.call_soon_threadsafe(self.launch_and_start_watch, entity.name, idx)

        return fut

    def render_html(self) -> str:
        """Render the callstack as a deck presentation to be shown after eager workflow execution."""

        from flytekit.core.base_task import PythonTask
        from flytekit.core.python_function_task import AsyncPythonFunctionTask, EagerAsyncPythonFunctionTask
        from flytekit.core.workflow import WorkflowBase

        output = "<h2>Nodes</h2><hr>"

        def _entity_type(entity) -> str:
            if isinstance(entity, EagerAsyncPythonFunctionTask):
                return "Eager Workflow"
            elif isinstance(entity, AsyncPythonFunctionTask):
                return "Async Task"
            elif isinstance(entity, PythonTask):
                return "Task"
            elif isinstance(entity, WorkflowBase):
                return "Workflow"
            return str(type(entity))

        for entity_name, items_list in self.entries.items():
            for item in items_list:
                kind = _entity_type(item.entity)
                output = f"{output}\n" + NODE_HTML_TEMPLATE.format(
                    entity_type=kind,
                    entity_name=item.entity.name,
                    execution_name=item.wf_exec.id.name,
                    url=self.remote.generate_console_url(item.wf_exec),
                    inputs=item.input_kwargs,
                    outputs=item.result if item.result else item.error,
                )

        return output
