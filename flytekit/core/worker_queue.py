from __future__ import annotations

import asyncio
import atexit
import hashlib
import re
import threading
import typing
from dataclasses import dataclass

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.constants import EAGER_ROOT_ENV_NAME, EAGER_TAG_KEY, EAGER_TAG_ROOT_KEY
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.options import Options
from flytekit.core.reference_entity import ReferenceEntity
from flytekit.core.utils import _dnsify
from flytekit.core.workflow import WorkflowBase
from flytekit.loggers import developer_logger, logger
from flytekit.models.common import Labels
from flytekit.models.core.execution import WorkflowExecutionPhase

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
        assert self.wf_exec is not None
        developer_logger.debug(f"Setting result for {self.wf_exec.id.name} on thread {threading.current_thread().name}")
        self.result = result
        # need to convert from literals resolver to literals and then to python native.
        self.fut._loop.call_soon_threadsafe(self.fut.set_result, result)

    def set_error(self, e: BaseException):
        assert self.wf_exec is not None
        developer_logger.debug(
            f"Setting error for {self.wf_exec.id.name} on thread {threading.current_thread().name} to {e}"
        )
        self.error = e
        self.fut._loop.call_soon_threadsafe(self.fut.set_exception, e)

    def set_exec(self, wf_exec: FlyteWorkflowExecution):
        self.wf_exec = wf_exec

    @property
    def ready(self) -> bool:
        return self.wf_exec is not None and (self.result is not None or self.error is not None)


class Informer(typing.Protocol):
    def watch(self, work: WorkItem): ...


class PollingInformer:
    def __init__(self, remote: FlyteRemote, loop: asyncio.AbstractEventLoop):
        self.remote: FlyteRemote = remote
        self.__loop = loop

    async def watch_one(self, work: WorkItem):
        assert work.wf_exec is not None
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
                python_outputs_interface: typing.Dict[str, typing.Type] = {}
                # Iterate in order so that we add to the interface in the correct order
                for i in range(num_outputs):
                    key = f"o{i}"
                    # todo:async add a nicer error here
                    var_type = work.entity.interface.outputs[key].type
                    python_outputs_interface[key] = TypeEngine.guess_python_type(var_type)
                py_iface = Interface(inputs=typing.cast(dict[str, typing.Type], {}), outputs=python_outputs_interface)
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
    def __init__(self, remote: FlyteRemote, ss: SerializationSettings, tag: str, root_tag: str, exec_prefix: str):
        logger.warning(
            f"Creating Controller for eager execution with {remote.config.platform.endpoint},"
            f" {tag=}, {root_tag=}, {exec_prefix=} and ss: {ss}"
        )
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
        self.exec_prefix = exec_prefix

        # Executions should be tracked in the following way:
        #  a) you should be able to list by label, all executions generated by the current eager task,
        #  b) in the case of nested eager, you should be able to list by label, all executions from the root eager task
        #  c) within a given eager task, the execution names should be deterministic and unique

        # To achieve this, this Controller will:
        #  a) set a label to track the root eager task execution
        #  b) set an environment variable to represent the root eager task execution for downstream tasks to read
        #  b) set a label to track the current eager task exec
        #  c) create deterministic execution names by combining:
        #     i) the current eager execution name (aka the tag)
        #     ii) the entity type being run
        #     iii) the entity name being run
        #     iv) the order in which it's called
        #     v) the input_kwargs
        #     hash the above, and then prepend it with a prefix.
        self.tag = tag
        self.root_tag = root_tag

    def _close(self) -> None:
        if self.__loop:
            self.__loop.stop()

    def _execute(self) -> None:
        loop = self.__loop
        try:
            loop.run_forever()
        finally:
            loop.close()

    def get_labels(self) -> Labels:
        """
        These labels keep track of the current and root (in case of nested) eager execution, that is responsible for
        kicking off this execution.
        """
        l = {EAGER_TAG_KEY: self.tag, EAGER_TAG_ROOT_KEY: self.root_tag}
        return Labels(l)

    def get_env(self) -> typing.Dict[str, str]:
        """
        In order for downstream tasks to correctly set the root label, this needs to pass down that information.
        """
        return {EAGER_ROOT_ENV_NAME: self.root_tag}

    def get_execution_name(self, entity: RunnableEntity, idx: int, input_kwargs: dict[str, typing.Any]) -> str:
        """Make a deterministic name"""
        # todo: Move the transform of python native values to literals up to the controller, and use pb hashing/user
        #   provided hashmethods to comprise the input_kwargs part
        components = f"{self.tag}-{type(entity)}-{entity.name}-{idx}-{input_kwargs}"

        # has the components into something deterministic
        hex = hashlib.md5(components.encode()).hexdigest()
        # just take the first 16 chars.
        exec_name = f"{self.exec_prefix}-{entity.name.split('.')[-1]}-{hex[:16]}"
        exec_name = _dnsify(exec_name)
        return exec_name

    def launch_and_start_watch(self, key: str, idx):
        """This function launches executions. This is called via the loop, so it needs exception handling"""
        # The reason the error handling is complicated is because if there is an error in the getting of the work item
        # object, then there is no way to pass that error to the future that is being awaited on by the user.
        # So errors in getting the work item will just crash the system for now, but other errors will correctly set
        # the exception on the future.
        if key not in self.entries:
            logger.error(f"Key {key} not found in entries")
            # Bad error, terminate everything
            # todo: more graceful error handling
            import sys

            sys.exit(1)

        state = None
        try:
            state = self.entries[key][idx]
        except IndexError:
            logger.error(f"Index {idx} not found in entries for key {key}, entries: {self.entries}")
            # Bad error, terminate everything
            import sys

            sys.exit(1)
        try:
            if state.result is None and state.error is None:
                l = self.get_labels()
                e = self.get_env()
                options = Options(labels=l)
                # need to add try/catch block since the execution might already exist
                # if the execution already exists we should fetch the inputs. If the inputs are the same, then we should
                # start watching it.
                exec_name = self.get_execution_name(state.entity, idx, state.input_kwargs)
                logger.info(f"Generated execution name {exec_name} for {idx}th call of {state.entity.name}")
                wf_exec = self.remote.execute(
                    entity=state.entity,
                    execution_name=exec_name,
                    inputs=state.input_kwargs,
                    version=self.ss.version,
                    image_config=self.ss.image_config,
                    options=options,
                    envs=e,
                )
                logger.info(f"Successfully started execution {wf_exec.id.name}")
                state.set_exec(wf_exec)

                # if successful then start watch on the execution
                self.informer.watch(state)
        except Exception as e:
            logger.error(f"Error launching execution for {state.entity.name} with {state.input_kwargs}")
            state.set_error(e)

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
                if not item.ready:
                    logger.warning(
                        f"Item for {item.entity.name} with inputs {item.input_kwargs}"
                        f" isn't ready, skipping for deck rendering..."
                    )
                    continue
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
