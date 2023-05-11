# TODO: has to support the SupportsNodeCreation protocol
import functools
import hashlib
from typing import Optional, Set, Tuple, Union
from typing_extensions import Any
from flytekit.core import tracker
from flytekit.core.base_task import PythonTask
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.promise import Promise, VoidPromise, flyte_entity_call_handler
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.models.core.workflow import NodeMetadata
from flytekit.exceptions import scopes as exception_scopes


class ExperimentalMapTask(PythonTask):
    def __init__(
        self,
        # TODO: add support for other Flyte entities
        python_function_task: Union[PythonFunctionTask, functools.partial],
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

        collection_interface = transform_interface_to_list_interface(self.python_function_task.python_interface, self._bound_inputs)
        _, mod, f, _ = tracker.extract_task_module(self.python_function_task.task_function)
        h = hashlib.md5(collection_interface.__str__().encode("utf-8")).hexdigest()
        self._name = f"{mod}.map_{f}_{h}"

        self._collection_interface = collection_interface

        super().__init__(
            name=self.name,
            interface=collection_interface,
            task_type=SdkTaskType.CONTAINER_ARRAY_TASK,
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
    def python_function_task(self) -> Union[PythonFunctionTask, functools.partial]:
        return self._python_function_task

    # def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise, None]:
    #     pass

    def __call__(self, *args, **kwargs):
        return flyte_entity_call_handler(self, *args, **kwargs)

    def get_command(self, settings: SerializationSettings) -> List[str]:
        """
        TODO ADD bound variables to the resolver. Maybe we need a different resolver?
        """
        mt = MapTaskResolver()
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

        if self._cmd_prefix:
            return self._cmd_prefix + container_args
        return container_args

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            return self._execute_map_task(ctx, **kwargs)

        return self._raw_execute(**kwargs)

    def _execute_map_task(self, _: FlyteContext, **kwargs) -> Any:
        raise NotImplementedError("not yet")

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
    task_function: Union[PythonFunctionTask, functools.partial],
    concurrency: int = 0,
    # TODO why no min_successes?
    min_success_ratio: float = 1.0,
    **kwargs,
):
    return ExperimentalMapTask(task_function, concurrency=concurrency, min_success_ratio=min_success_ratio, **kwargs)

class ArrayNodeMapTaskResolver(TrackedInstance, TaskResolverMixin):
    """
    TODO
    """

    def name(self) -> str:
        return "ArrayNodeMapTaskResolver"

    @timeit("Load map task")
    def load_task(self, loader_args: List[str], max_concurrency: int = 0) -> ExperimentalMapTask:
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
        return ExperimentalMapTask(python_function_task=_task_def, max_concurrency=max_concurrency, bound_inputs=bound_inputs)

    def loader_args(self, settings: SerializationSettings, t: MapPythonTask) -> List[str]:  # type:ignore
        return [
            "vars",
            f'{",".join(t.bound_inputs)}',
            "resolver",
            t.run_task.task_resolver.location,
            *t.run_task.task_resolver.loader_args(settings, t.run_task),
        ]

    # TODO: needed?
    # def get_all_tasks(self) -> List[Task]:
    #     raise NotImplementedError("MapTask resolver cannot return every instance of the map task")
