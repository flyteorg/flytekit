from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, Type, Union

from flytekit.core.context_manager import BranchEvalMode, ExecutionState, FlyteContext
from flytekit.core.promise import flyte_entity_call_handler, Promise, VoidPromise, extract_obj_name
from flytekit.exceptions import user as user_exceptions
from flytekit.loggers import remote_logger as logger
from flytekit.models.core.workflow import NodeMetadata


class RemoteEntity(ABC):
    def __init__(self, *args, **kwargs):

        # In cases where we make a FlyteTask/Workflow/LaunchPlan from a locally created Python object (i.e. an @task
        # or an @workflow decorated function), we actually have the Python interface, so
        self._python_interface: Optional[Dict[str, Type]] = None

        super().__init__(*args, **kwargs)

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    def construct_node_metadata(self) -> NodeMetadata:
        """
        Used when constructing the node that encapsulates this task as part of a broader workflow definition.
        """
        return NodeMetadata(
            name=extract_obj_name(self.name),
        )

    def __call__(self, *args, **kwargs):
        return flyte_entity_call_handler(self, *args, **kwargs)  # type: ignore

    def local_execute(self, ctx: FlyteContext, **kwargs) -> Optional[Union[Tuple[Promise], Promise, VoidPromise]]:
        return self.execute(**kwargs)

    def local_execution_mode(self):
        return ExecutionState.Mode.LOCAL_TASK_EXECUTION

    def execute(self, **kwargs) -> Any:
        raise AssertionError(f"Remotely fetched entities cannot be run locally. Please mock the {self.name}.execute.")

    @property
    def python_interface(self) -> Optional[Dict[str, Type]]:
        return self._python_interface
