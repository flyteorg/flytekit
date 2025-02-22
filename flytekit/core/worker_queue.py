from __future__ import annotations

import asyncio
import atexit
import hashlib
import re
import threading
import time
import typing
import uuid
from dataclasses import dataclass
from enum import Enum

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
from flytekit.utils.rate_limiter import RateLimiter

if typing.TYPE_CHECKING:
    from flytekit.remote.remote_callable import RemoteEntity

    RunnableEntity = typing.Union[WorkflowBase, LaunchPlan, PythonTask, ReferenceEntity, RemoteEntity]
    from flytekit.core.interface import Interface
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


class ItemStatus(Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCESS = "Success"
    FAILED = "Failed"


@dataclass
class Update:
    # The item to update
    work_item: WorkItem
    idx: int

    # fields in the item to update
    status: typing.Optional[ItemStatus] = None
    wf_exec: typing.Optional[FlyteWorkflowExecution] = None
    error: typing.Optional[BaseException] = None


@dataclass(unsafe_hash=True)
class WorkItem:
    """
    This is a class to keep track of what the user requested. Since it captures the arguments that the user wants
    to run the entity with, an arbitrary map, can't make this frozen.
    """

    entity: RunnableEntity
    input_kwargs: dict[str, typing.Any]
    result: typing.Any = None
    error: typing.Optional[BaseException] = None
    status: ItemStatus = ItemStatus.PENDING
    wf_exec: typing.Optional[FlyteWorkflowExecution] = None
    python_interface: typing.Optional[Interface] = None
    # This id is the key between this object and the Update object. The reason it's useful is because this
    # dataclass is not hashable.
    uuid: typing.Optional[uuid.UUID] = None

    def __post_init__(self):
        self.python_interface = self._get_python_interface()
        self.uuid = uuid.uuid4()

    @property
    def is_in_terminal_state(self) -> bool:
        return self.status == ItemStatus.SUCCESS or self.status == ItemStatus.FAILED

    def _get_python_interface(self) -> Interface:
        from flytekit.core.interface import Interface
        from flytekit.core.type_engine import TypeEngine

        if self.entity.python_interface:
            return self.entity.python_interface

        for k, _ in self.entity.interface.outputs.items():
            if not re.match(standard_output_format, k):
                raise AssertionError(
                    f"Entity without python interface found, and output name {k} does not match standard format o[0-9]+"
                )

        num_outputs = len(self.entity.interface.outputs)
        python_outputs_interface: typing.Dict[str, typing.Type] = {}
        # Iterate in order so that we add to the interface in the correct order
        for i in range(num_outputs):
            key = f"o{i}"
            if key not in self.entity.interface.outputs:
                raise AssertionError(
                    f"Output name {key} not found in outputs {[k for k in self.entity.interface.outputs.keys()]}"
                )
            var_type = self.entity.interface.outputs[key].type
            python_outputs_interface[key] = TypeEngine.guess_python_type(var_type)
        py_iface = Interface(inputs=typing.cast(dict[str, typing.Type], {}), outputs=python_outputs_interface)

        return py_iface


# A flag to ensure the handler runs only once
handling_signal = 0


class Controller:
    """
    This controller object is responsible for kicking off and monitoring executions against a Flyte Admin endpoint
    using a FlyteRemote object. It is used only for running eager tasks. It exposes one async method, `add`, which
    should be called by the eager task to run a sub-flyte-entity (task, workflow, or a nested eager task).

    The controller maintains a dictionary of entries, where each entry is a list of WorkItems. They are maintained
    in a list because the number of times and order that each task (or subwf, lp) is called affects the execution name
    which is consistently hashed.

    After calling `add`, a background thread is started to reconcile the state of this dictionary of WorkItem entries.
    Executions that should be kicked off will be kicked off, and ones that are running will be checked. This runs
    in a loop similar to a controller loop in a k8s operator.
    """

    def __init__(self, remote: FlyteRemote, ss: SerializationSettings, tag: str, root_tag: str, exec_prefix: str):
        logger.debug(
            f"Creating Controller for eager execution with {remote.config.platform.endpoint},"
            f" {tag=}, {root_tag=}, {exec_prefix=} and ss: {ss}"
        )

        self.entries: typing.Dict[str, typing.List[WorkItem]] = {}
        self.remote = remote
        self.ss = ss
        self.exec_prefix = exec_prefix
        self.entries_lock = threading.Lock()
        from flytekit.core.context_manager import FlyteContextManager

        # Import this to ensure context is loaded... python is reloading this module because its in a different thread
        FlyteContextManager.current_context()

        self.stopping_condition = threading.Event()
        # Things for actually kicking off and monitoring
        self.__runner_thread: threading.Thread = threading.Thread(
            target=self._execute, daemon=True, name="controller-thread"
        )
        self.__runner_thread.start()
        atexit.register(self._close, stopping_condition=self.stopping_condition, runner=self.__runner_thread)
        self.rate_limiter = RateLimiter(rpm=60)

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

    @staticmethod
    def _close(stopping_condition: threading.Event, runner: threading.Thread) -> None:
        stopping_condition.set()
        logger.debug("Set background thread to stop, awaiting...")
        runner.join()

    def reconcile_one(self, update: Update):
        """
        This is responsible for processing one work item. Will launch, update, set error on the update object
        Any errors are captured in the update object.
        """
        try:
            item = update.work_item
            if item.wf_exec is None:
                logger.info(f"reconcile should launch for {id(item)} entity name: {item.entity.name}")
                wf_exec = self.launch_execution(update.work_item, update.idx)
                update.wf_exec = wf_exec
                # Set this to running even if the launched execution was a re-run and already succeeded.
                # This forces the bottom half of the conditional to run, properly fetching all outputs.
                update.status = ItemStatus.RUNNING
            else:
                # Technically a mutating operation, but let's pretend it's not
                update.wf_exec = self.remote.sync_execution(item.wf_exec)

                # Fill in update status
                if update.wf_exec.closure.phase == WorkflowExecutionPhase.SUCCEEDED:
                    update.status = ItemStatus.SUCCESS
                elif update.wf_exec.closure.phase == WorkflowExecutionPhase.FAILED:
                    update.status = ItemStatus.FAILED
                elif update.wf_exec.closure.phase == WorkflowExecutionPhase.ABORTED:
                    update.status = ItemStatus.FAILED
                elif update.wf_exec.closure.phase == WorkflowExecutionPhase.TIMED_OUT:
                    update.status = ItemStatus.FAILED
                else:
                    update.status = ItemStatus.RUNNING

        except Exception as e:
            logger.error(
                f"Error launching execution for {update.work_item.entity.name} with {update.work_item.input_kwargs}: {e}"
            )
            update.error = e
            update.status = ItemStatus.FAILED

    def _get_update_items(self) -> typing.Dict[uuid.UUID, Update]:
        with self.entries_lock:
            update_items: typing.Dict[uuid.UUID, Update] = {}
            for entity_name, items in self.entries.items():
                for idx, item in enumerate(items):
                    # Only process items that need it
                    if item.status == ItemStatus.SUCCESS or item.status == ItemStatus.FAILED:
                        continue
                    update = Update(work_item=item, idx=idx)
                    update_items[typing.cast(uuid.UUID, item.uuid)] = update
            return update_items

    def _apply_updates(self, update_items: typing.Dict[uuid.UUID, Update]) -> None:
        with self.entries_lock:
            for entity_name, items in self.entries.items():
                for item in items:
                    if item.uuid in update_items:
                        update = update_items[typing.cast(uuid.UUID, item.uuid)]
                        item.wf_exec = update.wf_exec
                        if update.status is None:
                            raise AssertionError(f"update's status missing for {item.entity.name}")
                        item.status = update.status
                        if update.status == ItemStatus.SUCCESS:
                            if update.wf_exec is None:
                                raise AssertionError(f"update's wf_exec missing for {item.entity.name}")
                            item.result = update.wf_exec.outputs.as_python_native(item.python_interface)
                        elif update.status == ItemStatus.FAILED:
                            # If update object already has an error, then use that, otherwise look for one in the
                            # execution closure.
                            if update.error:
                                item.error = update.error
                            else:
                                from flytekit.exceptions.eager import EagerException

                                if update.wf_exec is None:
                                    raise AssertionError(
                                        f"update's wf_exec missing in error case for {item.entity.name}"
                                    )

                                exc = EagerException(
                                    f"Error executing {update.work_item.entity.name} with error:"
                                    f" {update.wf_exec.closure.error}"
                                )
                                item.error = exc

                        # otherwise it's still pending or running

    def _poll(self) -> None:
        # This needs to be a while loop that runs forever,
        while True:
            if self.stopping_condition.is_set():
                developer_logger.debug("Controller thread stopping detected, quitting poll loop")
                break
            # Gather all items that need processing
            update_items = self._get_update_items()

            # Actually call for updates outside of the lock.
            # Currently this happens one at a time, but only because the API only supports one at a time.
            for up in update_items.values():
                self.reconcile_one(up)

            # Take the lock again and apply all the updates
            self._apply_updates(update_items)

            # This is a blocking call so we don't hit the API too much.
            time.sleep(2)

    def _execute(self) -> None:
        try:
            self._poll()
        except Exception as e:
            logger.error(f"Error in eager execution processor: {e}")
            exit(1)

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

    def launch_execution(self, wi: WorkItem, idx: int) -> FlyteWorkflowExecution:
        """This function launches executions."""
        logger.info(f"Launching execution for {wi.entity.name} {idx=} with {wi.input_kwargs}")
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
                assert self.ss.version
                version = self.ss.version

            self.rate_limiter.sync_acquire()
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
            return wf_exec
        else:
            raise AssertionError(
                "This launch function should not be invoked for work items already" " with result or error"
            )

    async def add(self, entity: RunnableEntity, input_kwargs: dict[str, typing.Any]) -> typing.Any:
        """
        Add an entity along with the requested inputs to be submitted to Admin for running and return a future
        """
        # need to also check to see if the entity has already been registered, and if not, register it.
        i = WorkItem(entity=entity, input_kwargs=input_kwargs)

        with self.entries_lock:
            if entity.name not in self.entries:
                self.entries[entity.name] = []
            self.entries[entity.name].append(i)

        # wait for it to finish one way or another
        while True:
            developer_logger.debug(f"Watching id {id(i)}")
            if i.status == ItemStatus.SUCCESS:
                return i.result
            elif i.status == ItemStatus.FAILED:
                if i.error is None:
                    raise AssertionError(f"Error should not be None if status is failed for {entity.name}")
                raise i.error
            else:
                await asyncio.sleep(2)  # Small delay to avoid busy-waiting

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
                if not item.is_in_terminal_state:
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
