# TODO: has to support the SupportsNodeCreation protocol
import hashlib
import logging
import os  # TODO: use flytekit logger
from typing import List, Optional, Set
from contextlib import contextmanager

from typing_extensions import Any

from flytekit.configuration import SerializationSettings
from flytekit.core import tracker
from flytekit.core.base_task import PythonTask, TaskResolverMixin
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.promise import flyte_entity_call_handler
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.core.utils import timeit
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models.core.workflow import NodeMetadata
from flytekit.models.task import Task
from flytekit.tools.module_loader import load_object_from_module
from flytekit.models.task import Container, K8sPod, Sql
from flytekit.exceptions import scopes as exception_scopes


class ArrayNodeMapTask(PythonTask):
    def __init__(
        self,
        # TODO: add support for other Flyte entities
        python_function_task: PythonFunctionTask,
        concurrency: Optional[int] = None,
        min_successes: Optional[int] = None,
        min_success_ratio: Optional[float] = None,
        # TODO: add support for partials
        bound_inputs: Optional[Set[str]] = None,
        **kwargs,
    ):
        """
        :param python_function_task: The task to be executed in parallel
        :param concurrency: The number of parallel executions to run
        :param min_successes: The minimum number of successful executions
        :param min_success_ratio: The minimum ratio of successful executions
        :param bound_inputs: The set of inputs that should be bound to the map task
        :param kwargs: Additional keyword arguments to pass to the base class
        """
        self._python_function_task = python_function_task
        self._concurrency = concurrency
        self._min_successes = min_successes
        self._min_success_ratio = min_success_ratio
        self._bound_inputs = bound_inputs or set()

        collection_interface = transform_interface_to_list_interface(
            self.python_function_task.python_interface, self._bound_inputs
        )
        _, mod, f, _ = tracker.extract_task_module(self.python_function_task.task_function)
        h = hashlib.md5(collection_interface.__str__().encode("utf-8")).hexdigest()
        self._name = f"{mod}.map_{f}_{h}-arraynode"

        self._collection_interface = collection_interface

        super().__init__(
            name=self.name,
            interface=collection_interface,
            task_type=SdkTaskType.PYTHON_TASK,
            task_config=None,
            task_type_version=1,
            **kwargs,
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def python_interface(self):
        # TODO: wut?
        # return self._python_function_task.python_interface
        return self._collection_interface

    def construct_node_metadata(self) -> NodeMetadata:
        return NodeMetadata(
            name=self.name,
        )

    @property
    def min_success_ratio(self) -> Optional[float]:
        return self._min_success_ratio

    @property
    def min_successes(self) -> Optional[int]:
        return self._min_successes

    @property
    def concurrency(self) -> Optional[int]:
        return self._concurrency

    @property
    def python_function_task(self) -> PythonFunctionTask:
        return self._python_function_task

    @property
    def bound_inputs(self) -> Set[str]:
        return self._bound_inputs

    @contextmanager
    def prepare_target(self):
        """
        TODO: why do we do this?
        Alters the underlying run_task command to modify it for map task execution and then resets it after.
        """
        self.python_function_task.set_command_fn(self.get_command)
        try:
            yield
        finally:
            self.python_function_task.reset_command_fn()

    def get_container(self, settings: SerializationSettings) -> Container:
        with self.prepare_target():
            return self.python_function_task.get_container(settings)

    def get_k8s_pod(self, settings: SerializationSettings) -> K8sPod:
        with self.prepare_target():
            return self.python_function_task.get_k8s_pod(settings)

    def get_sql(self, settings: SerializationSettings) -> Sql:
        with self.prepare_target():
            return self.python_function_task.get_sql(settings)

    def get_command(self, settings: SerializationSettings) -> List[str]:
        """
        TODO ADD bound variables to the resolver. Maybe we need a different resolver?
        """
        mt = ArrayNodeMapTaskResolver()
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
            "--experimental",
            "--resolver",
            mt.name(),
            "--",
            *mt.loader_args(settings, self),
        ]

        # TODO: add support for ContainerTask
        # if self._cmd_prefix:
        #     return self._cmd_prefix + container_args
        return container_args

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
        return exception_scopes.user_entry_point(self.python_function_task.execute)(**map_task_inputs)

    def _compute_array_job_index() -> int:
        """
        Computes the absolute index of the current array job. This is determined by summing the compute-environment-specific
        environment variable and the offset (if one's set). The offset will be set and used when the user request that the
        job runs in a number of slots less than the size of the input.
        """
        return int(os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET", "0")) + int(
            os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME", "0"), "0")
        )

    def _raw_execute(self, **kwargs) -> Any:
        """
        This is called during locally run executions. Unlike array task execution on the Flyte platform, _raw_execute
        produces the full output collection.
        """
        outputs_expected = True
        if not self.interface.outputs:
            outputs_expected = False
        outputs = []

        any_input_key = (
            list(self._python_function_task.interface.inputs.keys())[0]
            if self._python_function_task.interface.inputs.items() is not None
            else None
        )

        for i in range(len(kwargs[any_input_key])):
            single_instance_inputs = {}
            for k in self.interface.inputs.keys():
                v = kwargs[k]
                if isinstance(v, list) and k not in self._bound_inputs:
                    single_instance_inputs[k] = kwargs[k][i]
                else:
                    single_instance_inputs[k] = kwargs[k]
            o = exception_scopes.user_entry_point(self._python_function_task.execute)(**single_instance_inputs)
            if outputs_expected:
                outputs.append(o)

        return outputs


def map_task(
    # TODO: add support for partials
    task_function: PythonFunctionTask,
    concurrency: int = 0,
    # TODO why no min_successes?
    min_success_ratio: float = 1.0,
    **kwargs,
):
    return ArrayNodeMapTask(task_function, concurrency=concurrency, min_success_ratio=min_success_ratio, **kwargs)


class ArrayNodeMapTaskResolver(tracker.TrackedInstance, TaskResolverMixin):
    """
    TODO
    """

    def name(self) -> str:
        return "ArrayNodeMapTaskResolver"

    @timeit("Load map task")
    def load_task(self, loader_args: List[str], max_concurrency: int = 0) -> ArrayNodeMapTask:
        """
        Loader args should be of the form
        vars "var1,var2,.." resolver "resolver" [resolver_args]
        """
        _, bound_vars, _, resolver, *resolver_args = loader_args
        logging.info(f"MapTask found task resolver {resolver} and arguments {resolver_args}")
        resolver_obj = load_object_from_module(resolver)
        # Use the resolver to load the actual task object
        _task_def = resolver_obj.load_task(loader_args=resolver_args)
        bound_inputs = set(bound_vars.split(","))
        return ArrayNodeMapTask(
            python_function_task=_task_def, max_concurrency=max_concurrency, bound_inputs=bound_inputs
        )

    def loader_args(self, settings: SerializationSettings, t: ArrayNodeMapTask) -> List[str]:  # type:ignore
        return [
            "vars",
            f'{",".join(t.bound_inputs)}',
            "resolver",
            t.python_function_task.task_resolver.location,
            *t.python_function_task.task_resolver.loader_args(settings, t.python_function_task),
        ]

    def get_all_tasks(self) -> List[Task]:
        raise NotImplementedError("MapTask resolver cannot return every instance of the map task")
