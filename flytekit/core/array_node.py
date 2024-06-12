from typing import Any, List, Optional, Set, Union

from flytekit.core import interface as flyte_interface
from flytekit.core.base_task import Task
from flytekit.core.context_manager import ExecutionState, FlyteContext
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.node import Node
from flytekit.core.promise import flyte_entity_call_handler
from flytekit.core.task import TaskMetadata
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model


class ArrayNode(object):
    def __init__(
        self,
        target: Union[Task, LaunchPlan],
        concurrency: Optional[int] = None,
        min_successes: Optional[int] = None,
        min_success_ratio: Optional[float] = None,
        bound_inputs: Optional[Set[str]] = None,
        metadata: Optional[Union[_workflow_model.NodeMetadata, TaskMetadata]] = None,
        **kwargs,
    ):
        """
        :param target: The subnode to be executed in parallel
        :param concurrency: The number of parallel executions to run
        :param min_successes: The minimum number of successful executions
        :param min_success_ratio: The minimum ratio of successful executions
        :param bound_inputs: The set of inputs that should be bound to the map task
        """
        self.target = target
        self.concurrency = concurrency
        self.min_successes = min_successes
        self.min_success_ratio = min_success_ratio
        self.bound_inputs = set()

        self.metadata = None
        self.id = target.name

        n_outputs = len(self.target.python_interface.outputs)
        if n_outputs > 1:
            raise ValueError("Only tasks with a single output are supported in map tasks.")

        output_as_list_of_optionals = min_success_ratio is not None and min_success_ratio != 1 and n_outputs == 1
        collection_interface = transform_interface_to_list_interface(
            self.target.python_interface, self.bound_inputs, output_as_list_of_optionals
        )
        self._collection_interface = collection_interface

        if isinstance(target, Task):
            if metadata:
                if isinstance(metadata, TaskMetadata):
                    self.metadata = metadata
                else:
                    raise Exception("Invalid metadata for Task. Should be TaskMetadata.")
            elif target.metadata:
                self.metadata = target.metadata
        elif isinstance(target, LaunchPlan):
            if metadata:
                if isinstance(metadata, _workflow_model.NodeMetadata):
                    self.metadata = metadata
                else:
                    raise Exception("Invalid metadata for LaunchPlan. Should be NodeMetadata.")

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata:
        # Part of SupportsNodeCreation interface
        return _workflow_model.NodeMetadata(name=self.target.name)

    @property
    def name(self) -> str:
        # Part of SupportsNodeCreation interface
        return self.target.name

    @property
    def python_interface(self) -> flyte_interface.Interface:
        # Part of SupportsNodeCreation interface
        return self._collection_interface

    @property
    def bindings(self) -> List[_literal_models.Binding]:
        return []

    @property
    def upstream_nodes(self) -> List[Node]:
        return []

    @property
    def flyte_entity(self) -> Any:
        return self.target

    def local_execute(self, ctx: FlyteContext, **kwargs):
        self.target.local_execute(ctx, **kwargs)

    def local_execution_mode(self):
        return ExecutionState.Mode.LOCAL_TASK_EXECUTION


def map_task(
    target: Union[Task, LaunchPlan],
    concurrency: Optional[int] = None,
    min_successes: Optional[int] = None,
    min_success_ratio: Optional[float] = None,
    **kwargs,
):
    a = ArrayNode(target, concurrency, min_successes, min_success_ratio)

    return flyte_entity_call_handler(a, **kwargs)
