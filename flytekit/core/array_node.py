from typing import Any, List, Optional, Union

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
        metadata: Optional[Union[_workflow_model.NodeMetadata, TaskMetadata]] = None,
    ):
        """
        :param target: The target Flyte entity to map over
        :param concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch
            size. If the size of the input exceeds the concurrency value, then multiple batches will be run serially until
            all inputs are processed. If set to 0, this means unbounded concurrency. If left unspecified, this means the
            array node will inherit parallelism from the workflow
        :param min_success_ratio: If specified, this determines the minimum fraction of total jobs which can complete
            successfully before terminating this task and marking it successful.
        :param min_successes: If specified, an absolute number of the minimum number of successful completions of subtasks.
            As soon as the criteria is met, the array job will be marked as successful and outputs will be computed.
        """
        self.target = target
        self._concurrency = concurrency
        self._min_successes = min_successes
        self._min_success_ratio = min_success_ratio
        self.id = target.name

        n_outputs = len(self.target.python_interface.outputs)
        if n_outputs > 1:
            raise ValueError("Only tasks with a single output are supported in map tasks.")

        output_as_list_of_optionals = min_success_ratio is not None and min_success_ratio != 1 and n_outputs == 1
        collection_interface = transform_interface_to_list_interface(
            self.target.python_interface, set(), output_as_list_of_optionals
        )
        self._collection_interface = collection_interface

        self.metadata = None
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
        # TODO - include passed in metadata
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
        pass
        # TODO - pvditt
        # self.target.local_execute(ctx, **kwargs)

    @staticmethod
    def local_execution_mode(self):
        return ExecutionState.Mode.LOCAL_TASK_EXECUTION

    @property
    def min_success_ratio(self) -> Optional[float]:
        return self._min_success_ratio

    @property
    def min_successes(self) -> Optional[int]:
        return self._min_successes

    @property
    def concurrency(self) -> Optional[int]:
        return self._concurrency


def mapped_entity(
    target: Union[LaunchPlan],
    concurrency: Optional[int] = None,
    min_success_ratio: Optional[float] = None,
    min_successes: Optional[int] = None,
    **kwargs,
):
    """
    Map tasks that maps over tasks and other Flyte entities
    Args:
    :param target: The target Flyte entity to map over
    :param concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch
        size. If the size of the input exceeds the concurrency value, then multiple batches will be run serially until
        all inputs are processed. If set to 0, this means unbounded concurrency. If left unspecified, this means the
        array node will inherit parallelism from the workflow
    :param min_success_ratio: If specified, this determines the minimum fraction of total jobs which can complete
        successfully before terminating this task and marking it successful.
    :param min_successes: If specified, an absolute number of the minimum number of successful completions of subtasks.
        As soon as the criteria is met, the array job will be marked as successful and outputs will be computed.
    """
    if not isinstance(target, LaunchPlan):
        raise ValueError("Only LaunchPlans are supported for now.")

    node = ArrayNode(target, concurrency, min_successes, min_success_ratio)

    def callable_entity(**inner_kwargs):
        combined_kwargs = {**kwargs, **inner_kwargs}
        return flyte_entity_call_handler(node, **combined_kwargs)

    return callable_entity
