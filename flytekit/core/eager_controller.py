import asyncio
import hashlib
import typing
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from typing import Optional, Any, Dict

from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.base_task import PythonTask
from flytekit.core.controller import Controller, ResourceProtocol, Informer
from flytekit.core.reference_entity import ReferenceEntity
from flytekit.core.workflow import WorkflowBase

RunnableEntity = typing.Union[WorkflowBase, LaunchPlan, PythonTask, ReferenceEntity]


@dataclass
class LocalNode(ResourceProtocol):
    root_exec_id: str
    entity: RunnableEntity
    last_updated: datetime = None
    input_kwargs: Optional[Dict[str, Any]] = None
    result: Any = None
    node_group: Optional[str] = None
    error: Optional[BaseException] = None
    _task: Optional[asyncio.Task] = None  # Private field to store the background task
    name: str = None

    def __post_init__(self):
        self.last_updated = datetime.now()
        if self.input_kwargs is None:
            self.input_kwargs = {}
        self.name = self.key  # Cache the key

    async def _run_entity(self):
        """Internal async method to run the entity and store results/errors"""
        try:
            # Assuming RunnableEntity.__call__ is synchronous
            # If it's async, remove the await asyncio.to_thread()
            result = await self.entity._task_function(**self.input_kwargs)
            self.result = result
            self.error = None
        except Exception as e:
            self.result = None
            self.error = e

    async def sync(self) -> 'LocalNode':
        """
        Non-blocking sync that always returns self.
        Checks task status and updates results if complete.
        """
        return self

    async def launch(self) -> bool:
        """
        Launch the RunnableEntity in the background
        """
        print(f"Launching {self.key}")
        if self._task is None:
            self._task = asyncio.create_task(self._run_entity())
            print(f"Launched {self.key} with task {self._task}")
        return True

    def is_terminal(self) -> bool:
        """
        Returns True if the node has completed (successfully or with error)
        """
        print(f"Checking terminal status for {self.key}")
        if self._task is None:
            print(f"Task not found for {self.key}")
            return False
        print(f"Task status for {self.key}: {self._task.done()}")
        return self._task.done()

    @cached_property
    def key(self) -> str:
        """Make a deterministic name"""
        components = f"{self.root_exec_id}-{self.name}-{self.input_kwargs}" + (
            f"-{self.node_group}" if self.node_group else "")

        # has the components into something deterministic
        hex = hashlib.md5(components.encode()).hexdigest()
        exec_name = f"{hex}"
        return exec_name

    def is_started(self) -> bool:
        """Check if resource has been started."""
        return self._task is not None


class EagerController(Controller[LocalNode]):

    def __init__(self, root_exec_id: str):
        poll_queue = asyncio.Queue()
        poll_informer = Informer[LocalNode](poll_queue, sync_period=2.0, use_watch=False)
        super(EagerController, self).__init__(informer=poll_informer, shared_queue=poll_queue,
                                              max_concurrent_launches=2)
        self.root_exec_id = root_exec_id

    async def _process_resource(self, resource: LocalNode):
        """Process resource updates - can be overridden by subclasses"""
        pass

    async def submit_node(self, entity: RunnableEntity, kwargs) -> Any:
        """
        Submit a node to the controller
        """
        node = LocalNode(root_exec_id=self.root_exec_id, entity=entity, input_kwargs=kwargs)
        n = await self.submit_resource(node)
        if n.error:
            raise n.error
        return n.result
