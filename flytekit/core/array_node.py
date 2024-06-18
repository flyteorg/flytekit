import functools
import os
from typing import Any, List, Optional, Set, Union

from flytekit.configuration import SerializationSettings
from flytekit.core import interface as flyte_interface
from flytekit.core import tracker
from flytekit.core.base_task import Task, TaskResolverMixin
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.launch_plan import LaunchPlan
from flytekit.core.node import Node
from flytekit.core.promise import flyte_entity_call_handler
from flytekit.core.python_function_task import PythonFunctionTask, PythonInstanceTask
from flytekit.core.task import TaskMetadata
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models import literals as _literal_models
from flytekit.models.core import workflow as _workflow_model
from flytekit.tools.module_loader import load_object_from_module


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
        self._concurrency = concurrency
        self._min_successes = min_successes
        self._min_success_ratio = min_success_ratio
        # self.bound_inputs: Set[Any] = set()

        self.metadata = None
        self.id = target.name

        n_outputs = len(self.target.python_interface.outputs)
        if n_outputs > 1:
            raise ValueError("Only tasks with a single output are supported in map tasks.")

        output_as_list_of_optionals = min_success_ratio is not None and min_success_ratio != 1 and n_outputs == 1
        collection_interface = transform_interface_to_list_interface(
            self.target.python_interface, set(), output_as_list_of_optionals
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


class ArrayPythonFunctionTaskWrapper(PythonInstanceTask):
    def __init__(
        self, task: Union[PythonFunctionTask, functools.partial], bound_inputs: Optional[Set[str]] = None, **kwargs
    ):
        self._partial = None

        if isinstance(task, functools.partial):
            self._partial = task
            self.python_task = task.func
        else:
            self.python_task = task

        self._bound_inputs: Set[str] = bound_inputs or set(bound_inputs) if bound_inputs else set()
        if self._partial:
            self._bound_inputs.update(self._partial.keywords.keys())

        # TODO - optional outputs
        # collection_interface = transform_interface_to_list_interface(
        #     self.python_task.python_interface, self._bound_inputs, False
        # )
        #
        # self._collection_interface = collection_interface

        super().__init__(
            # TODO - pvditt name
            name="array_wrapper",
            interface=self.python_task.python_interface,
            task_config=self.python_task.task_config,
            task_type=self.python_task.task_type,
            task_resolver=self.python_task.task_resolver,
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            return self._execute_map_task(ctx, **kwargs)

        return self._raw_execute(**kwargs)

    def _execute_map_task(self, _: FlyteContext, **kwargs) -> Any:
        task_index = self._compute_array_job_index()
        map_task_inputs = {}
        for k in self.interface.inputs.keys():
            v = kwargs[k]
            if isinstance(v, list) and k not in self.bound_inputs:
                map_task_inputs[k] = v[task_index]
            else:
                map_task_inputs[k] = v
        return exception_scopes.user_entry_point(self.python_task.execute)(**map_task_inputs)

    def get_command(self, settings: SerializationSettings) -> List[str]:
        mt = ArrayPythonFunctionTaskWrapperResolver()
        container_args = [
            "pyflyte-map-execute",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--checkpoint-path",
            "{{.checkpointOutputPrefix}}",
            "--prev-checkpoint",
            "{{.prevCheckpointPrefix}}",
            "--resolver",
            mt.name(),
            "--",
            *mt.loader_args(settings, self),
        ]

        # if self._cmd_prefix:
        #     return self._cmd_prefix + container_args
        return container_args

    def _raw_execute(self, **kwargs) -> Any:
        return None

    @staticmethod
    def _compute_array_job_index() -> int:
        """
        Computes the absolute index of the current array job. This is determined by summing the compute-environment-specific
        environment variable and the offset (if one's set). The offset will be set and used when the user request that the
        job runs in a number of slots less than the size of the input.
        """
        return int(os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET", "0")) + int(
            os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME", "0"), "0")
        )

    def __call__(self, *args, **kwargs):
        """
        This call method modifies the kwargs and adds kwargs from partial.
        This is mostly done in the local_execute and compilation only.
        At runtime, the map_task is created with all the inputs filled in. to support this, we have modified
        the map_task interface in the constructor.
        """
        if self._partial:
            """If partial exists, then mix-in all partial values"""
            kwargs = {**self._partial.keywords, **kwargs}
        return super().__call__(*args, **kwargs)

    @property
    def python_interface(self):
        return self.python_task.python_interface

    @property
    def bound_inputs(self) -> Set[str]:
        return self._bound_inputs


class ArrayPythonFunctionTaskWrapperResolver(tracker.TrackedInstance, TaskResolverMixin):
    """
    TODO - pvditt fill in
    """

    def name(self) -> str:
        return "flytekit.core.array_node_map_task.ArrayPythonFunctionTaskWrapperResolver"

    def load_task(self, loader_args: List[str], max_concurrency: int = 0) -> Task:
        """
        Loader args should be of the form
        vars "var1,var2,.." resolver "resolver" [resolver_args]
        """
        _, bound_vars, _, resolver, *resolver_args = loader_args
        resolver_obj = load_object_from_module(resolver)
        # Use the resolver to load the actual task object
        _task_def = resolver_obj.load_task(loader_args=resolver_args)
        bound_inputs = set(bound_vars.split(","))
        return ArrayPythonFunctionTaskWrapper(_task_def, _task_def.task_type, _task_def.name, bound_inputs=bound_inputs)

    def loader_args(self, settings: SerializationSettings, t: ArrayPythonFunctionTaskWrapper) -> List[str]:  # type:ignore
        return [
            "vars",
            f'{",".join(sorted(t.bound_inputs))}',
            "resolver",
            t.python_task.task_resolver.location,
            *t.python_task.task_resolver.loader_args(settings, t.python_task),
        ]

    def get_all_tasks(self) -> List[Task]:
        raise NotImplementedError(
            "ArrayPythonFunctionTaskWrapper resolver cannot return every instance of the map task"
        )


def map_task(
    target: Union[PythonFunctionTask, LaunchPlan, functools.partial],
    concurrency: Optional[int] = None,
    min_successes: Optional[int] = None,
    min_success_ratio: Optional[float] = None,
    **kwargs,
):
    if isinstance(target, PythonFunctionTask) or isinstance(target, functools.partial):
        target = ArrayPythonFunctionTaskWrapper(target, **kwargs)
        node = ArrayNode(target, concurrency, min_successes, min_success_ratio, bound_inputs=target.bound_inputs)
    else:
        node = ArrayNode(target, concurrency, min_successes, min_success_ratio)

    def callable_entity(**inner_kwargs):
        combined_kwargs = {**kwargs, **inner_kwargs}
        return flyte_entity_call_handler(node, **combined_kwargs)

    return callable_entity
