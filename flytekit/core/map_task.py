import os
from typing import Any, Dict, List, Optional, Type

from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.context_manager import ExecutionState, FlyteContext, SerializationSettings
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.python_function_task import PythonFunctionTask, get_registerable_container_image
from flytekit.models.interface import Variable
from flytekit.models.task import Container


class MapPythonTask(PythonTask):
    """
    TODO: support lambda functions
    """

    def __init__(
        self,
        tk: PythonFunctionTask,
        metadata: Optional[TaskMetadata] = None,
        concurrency=None,
        min_success_ratio=None,
        **kwargs,
    ):
        collection_interface = transform_interface_to_list_interface(tk.python_interface)
        name = f"{tk._task_function.__module__}.mapper_{tk._task_function.__name__}"
        self._run_task = tk
        self._max_concurrency = concurrency
        self._min_success_ratio = min_success_ratio
        self._array_task_interface = tk.python_interface
        super().__init__(
            name=name,
            interface=collection_interface,
            metadata=metadata,
            task_type="container_array",
            task_config=None,
            task_type_version=1,
            **kwargs,
        )

    def get_command(self, settings: SerializationSettings) -> List[str]:
        return [
            "pyflyte-map-execute",
            "--task-module",
            self._run_task._task_function.__module__,
            "--task-name",
            f"{self._run_task._task_function.__name__}",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
        ]

    def get_container(self, settings: SerializationSettings) -> Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=get_registerable_container_image(None, settings.image_config),
            command=[],
            args=self.get_command(settings=settings),
            data_loading_config=None,
            environment=env,
        )

    @property
    def run_task(self) -> PythonTask:
        return self._run_task

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContext.current_context()
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            return self._execute_map_task(ctx, **kwargs)

        outputs = self._raw_execute(**kwargs)
        return outputs

    @staticmethod
    def _compute_array_job_index() -> int:
        """
        Computes the absolute index of the current array job. This is determined by summing the compute-environment-specific
        environment variable and the offset (if one's set). The offset will be set and used when the user request that the
        job runs in a number of slots less than the size of the input.
        :rtype: int
        """
        offset = 0
        if os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET"):
            offset = int(os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET"))
        return offset + int(os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME")))

    @property
    def _outputs_interface(self) -> Dict[Any, Variable]:
        """
        We override this method from PythonTask because the dispatch_execute method uses this
        interface to construct outputs. Each instance of an container_array task will however produce outputs
        according to the underlying run_task interface and the array plugin handler will actually create a collection
        from these individual outputs as the final output value.
        """

        ctx = FlyteContext.current_context()
        if ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            return self.interface.outputs
        return self._run_task.interface.outputs

    def get_type_for_output_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        """
        We override this method from flytekit.core.base_task Task because the dispatch_execute method uses this
        interface to construct outputs. Each instance of an container_array task will however produce outputs
        according to the underlying run_task interface and the array plugin handler will actually create a collection
        from these individual outputs as the final output value.
        """
        ctx = FlyteContext.current_context()
        if ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            return self._python_interface.outputs[k]
        return self._run_task._python_interface.outputs[k]

    def _execute_map_task(self, ctx: FlyteContext, **kwargs) -> Any:
        """
        This is called during ExecutionState.Mode.TASK_EXECUTION executions, that is executions orchestrated by the
        Flyte platform. Individual instances of the map task, aka array task jobs are passed the full set of inputs but
        only produce a single output based on the map task (array task) instance. The array plugin handler will actually
        create a collection from these individual outputs as the final map task output value.
        """
        task_index = self._compute_array_job_index()
        map_task_inputs = {}
        for k in self.interface.inputs.keys():
            map_task_inputs[k] = kwargs[k][task_index]
        return self._run_task.execute(**map_task_inputs)

    def _raw_execute(self, **kwargs) -> Any:
        outputs_expected = True
        if not self.interface.outputs:
            outputs_expected = False
        outputs = []
        for _ in self._outputs_interface.keys():
            outputs.append([])

        any_input_key = (
            list(self._run_task.interface.inputs.keys())[0]
            if self._run_task.interface.inputs.items() is not None
            else None
        )

        for i in range(len(kwargs[any_input_key])):
            single_instance_inputs = {}
            for k in self.interface.inputs.keys():
                single_instance_inputs[k] = kwargs[k][i]
            o = self._run_task.execute(**single_instance_inputs)
            if outputs_expected:
                for x in range(len(outputs)):
                    outputs[x].append(o[x])

        if len(outputs) == 1:
            return outputs[0]

        return tuple(outputs)


def maptask(tk: PythonFunctionTask, concurrency=None, min_success_ratio=None, metadata=None):
    if not isinstance(tk, PythonFunctionTask):
        raise ValueError(f"Only Flyte python task types are supported in maptask currently, received {type(tk)}")
    # We could register in a global singleton here?
    return MapPythonTask(tk, concurrency=concurrency, min_success_ratio=min_success_ratio, metadata=metadata)
