from __future__ import annotations

import asyncio
import atexit
import hashlib
import re
import threading
import typing
from concurrent.futures import Future
from dataclasses import dataclass

from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.constants import EAGER_ROOT_ENV_NAME, EAGER_TAG_KEY, EAGER_TAG_ROOT_KEY
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.options import Options
from flytekit.core.reference_entity import ReferenceEntity
from flytekit.core.utils import _dnsify
from flytekit.core.workflow import WorkflowBase
from flytekit.exceptions.system import FlyteSystemException
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
        developer_logger.debug(
            f"Setting error for {self.wf_exec.id.name if self.wf_exec else 'unstarted execution'}"
            f" on thread {threading.current_thread().name} to {e}"
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
                    if key not in work.entity.interface.outputs:
                        raise AssertionError(
                            f"Output name {key} not found in outputs {[k for k in work.entity.interface.outputs.keys()]}"
                        )
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
        # both run_coroutine_threadsafe and self.__loop.create_task seem to work, but the first makes
        # more sense in case *this* thread is ever different than the thread that self.__loop is running on.
        f = asyncio.run_coroutine_threadsafe(coro, self.__loop)

        developer_logger.debug(f"Started watch with future {f}")

        def cb(fut: Future):
            """
            This cb takes care of any exceptions that might be thrown in the watch_one coroutine
            Note: This is a concurrent Future not an asyncio Future
            """
            e = fut.exception()
            if e:
                logger.error(f"Error in watch for {work.entity.name} with {work.input_kwargs}: {e}")
                work.set_error(e)

        f.add_done_callback(cb)


# A flag to ensure the handler runs only once
handling_signal = 0


class Controller:
    def __init__(self, remote: FlyteRemote, ss: SerializationSettings, tag: str, root_tag: str, exec_prefix: str):
        logger.debug(
            f"Creating Controller for eager execution with {remote.config.platform.endpoint},"
            f" {tag=}, {root_tag=}, {exec_prefix=} and ss: {ss}"
        )
        # Set up things for this controller to operate
        from flytekit.utils.asyn import _selector_policy

        with _selector_policy():
            self.__loop = asyncio.new_event_loop()
        self.__loop.set_exception_handler(self.exc_handler)
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

    @staticmethod
    def exc_handler(loop, context):
        logger.error(f"Caught exception in loop {loop} with context {context}")

    def _execute(self) -> None:
        loop = self.__loop
        try:
            loop.run_forever()
        finally:
            logger.error("Controller event loop stopped.")

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
        #   provided hashmethods to comprise the input_kwargs part. Merely printing input_kwargs is not strictly correct
        #   https://github.com/flyteorg/flyte/issues/6069
        components = f"{self.tag}-{type(entity)}-{entity.name}-{idx}-{input_kwargs}"

        # has the components into something deterministic
        hex = hashlib.md5(components.encode()).hexdigest()
        # just take the first 16 chars.
        hex = hex[:16]
        name = entity.name.split(".")[-1]
        name = name[:8]  # just take the first 8 chars
        exec_name = f"{self.exec_prefix}-{name}-{hex}"
        exec_name = _dnsify(exec_name)
        return exec_name

    def launch_and_start_watch(self, wi: WorkItem, idx: int):
        """This function launches executions. This is called via the loop, so it needs exception handling"""
        try:
            if wi.result is None and wi.error is None:
                l = self.get_labels()
                e = self.get_env()
                options = Options(labels=l)
                exec_name = self.get_execution_name(wi.entity, idx, wi.input_kwargs)
                logger.info(f"Generated execution name {exec_name} for {idx}th call of {wi.entity.name}")
                from flytekit.remote.remote_callable import RemoteEntity

                if isinstance(wi.entity, RemoteEntity):
                    version = wi.entity.id.version
                else:
                    version = self.ss.version

                # todo: if the execution already exists, remote.execute will return that execution. in the future
                #  we can add input checking to make sure the inputs are indeed a match.
                wf_exec = self.remote.execute(
                    entity=wi.entity,
                    execution_name=exec_name,
                    inputs=wi.input_kwargs,
                    version=version,
                    image_config=self.ss.image_config,
                    options=options,
                    envs=e,
                )
                logger.info(f"Successfully started execution {wf_exec.id.name}")
                wi.set_exec(wf_exec)

                # if successful then start watch on the execution
                self.informer.watch(wi)
            else:
                raise AssertionError(
                    "This launch function should not be invoked for work items already" " with result or error"
                )
        except Exception as e:
            # all exceptions get registered onto the future.
            logger.error(f"Error launching execution for {wi.entity.name} with {wi.input_kwargs}")
            wi.set_error(e)

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
        self.__loop.call_soon_threadsafe(self.launch_and_start_watch, i, idx)
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
                    execution_name=item.wf_exec.id.name,  # type: ignore[union-attr]
                    url=self.remote.generate_console_url(item.wf_exec),
                    inputs=item.input_kwargs,
                    outputs=item.result if item.result else item.error,
                )

        return output

    def get_signal_handler(self):
        """
        TODO: At some point, this loop would be ideally managed by the loop manager, and the signal handler should
          gracefully initiate shutdown of all loops, calling .cancel() on all tasks, allowing each loop to clean up,
          starting with the deepest loop/thread first and working up.
          https://github.com/flyteorg/flyte/issues/6068
        """

        def signal_handler(signum, frame):
            logger.warning(f"Received signal {signum}, initiating signal handler")
            global handling_signal
            if handling_signal:
                if handling_signal > 2:
                    exit(1)
                logger.warning("Signal already being handled. Please wait...")
                handling_signal += 1
                return

            handling_signal += 1
            self.__loop.stop()  # stop the loop
            for _, work_list in self.entries.items():
                for work in work_list:
                    if work.wf_exec and not work.wf_exec.is_done:
                        try:
                            self.remote.terminate(work.wf_exec, "eager execution cancelled")
                            logger.warning(f"Cancelled {work.wf_exec.id.name}")
                        except FlyteSystemException as e:
                            logger.info(f"Error cancelling {work.wf_exec.id.name}, may already be cancelled: {e}")
            exit(1)

        return signal_handler

    @classmethod
    def for_sandbox(cls, exec_prefix: typing.Optional[str] = None) -> Controller:
        from flytekit.core.context_manager import FlyteContextManager
        from flytekit.remote import FlyteRemote

        ctx = FlyteContextManager.current_context()
        remote = FlyteRemote.for_sandbox(default_project="flytesnacks", default_domain="development")
        rand = ctx.file_access.get_random_string()
        ss = ctx.serialization_settings
        if not ss:
            ss = SerializationSettings(
                image_config=ImageConfig.auto_default_image(),
                version=f"v{rand[:8]}",
            )
        root_tag = tag = f"eager-local-{rand}"
        exec_prefix = exec_prefix or f"e-{rand[:16]}"

        c = Controller(remote=remote, ss=ss, tag=tag, root_tag=root_tag, exec_prefix=exec_prefix)
        return c
