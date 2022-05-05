"""
Flytekit map tasks specify how to run a single task across a list of inputs. Map tasks themselves are constructed with
a reference task as well as run-time parameters that limit execution concurrency and failure tolerations.
"""

import os
import typing
from contextlib import contextmanager
from itertools import count
from typing import Any, Dict, List, Optional, Type

from flytekit.configuration import SerializationSettings
from flytekit.core import tracker
from flytekit.core.base_task import PythonTask
from flytekit.core.constants import SdkTaskType
from flytekit.core.context_manager import ExecutionState, FlyteContext, FlyteContextManager
from flytekit.core.interface import transform_interface_to_list_interface
from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.exceptions import scopes as exception_scopes
from flytekit.models.array_job import ArrayJob
from flytekit.models.interface import Variable
from flytekit.models.task import Container, K8sPod, Sql


class MapPythonTask(PythonTask):
    """
    A MapPythonTask defines a :py:class:`flytekit.PythonTask` which specifies how to run
    an inner :py:class:`flytekit.PythonFunctionTask` across a range of inputs in parallel.
    TODO: support lambda functions
    """

    # To support multiple map tasks declared around identical python function tasks, we keep a global count of
    # MapPythonTask instances to uniquely differentiate map task names for each declared instance.
    _ids = count(0)

    def __init__(
        self,
        python_function_task: PythonFunctionTask,
        concurrency: int = None,
        min_success_ratio: float = None,
        **kwargs,
    ):
        """
        :param python_function_task: This argument is implicitly passed and represents the repeatable function
        :param concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given
        batch size
        :param min_success_ratio: If specified, this determines the minimum fraction of total jobs which can complete
            successfully before terminating this task and marking it successful.
        """
        if len(python_function_task.python_interface.inputs.keys()) > 1:
            raise ValueError("Map tasks only accept python function tasks with 0 or 1 inputs")

        if len(python_function_task.python_interface.outputs.keys()) > 1:
            raise ValueError("Map tasks only accept python function tasks with 0 or 1 outputs")

        collection_interface = transform_interface_to_list_interface(python_function_task.python_interface)
        instance = next(self._ids)
        _, mod, f, _ = tracker.extract_task_module(python_function_task.task_function)
        name = f"{mod}.mapper_{f}_{instance}"

        self._cmd_prefix = None
        self._run_task = python_function_task
        self._max_concurrency = concurrency
        self._min_success_ratio = min_success_ratio
        self._array_task_interface = python_function_task.python_interface
        if "metadata" not in kwargs and python_function_task.metadata:
            kwargs["metadata"] = python_function_task.metadata
        if "security_ctx" not in kwargs and python_function_task.security_context:
            kwargs["security_ctx"] = python_function_task.security_context
        super().__init__(
            name=name,
            interface=collection_interface,
            task_type=SdkTaskType.CONTAINER_ARRAY_TASK,
            task_config=None,
            task_type_version=1,
            **kwargs,
        )

    def get_command(self, settings: SerializationSettings) -> List[str]:
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
            self._run_task.task_resolver.location,
            "--",
            *self._run_task.task_resolver.loader_args(settings, self._run_task),
        ]

        if self._cmd_prefix:
            return self._cmd_prefix + container_args
        return container_args

    def set_command_prefix(self, cmd: typing.List[str]):
        self._cmd_prefix = cmd

    @contextmanager
    def prepare_target(self):
        """
        Alters the underlying run_task command to modify it for map task execution and then resets it after.
        """
        self._run_task.set_command_fn(self.get_command)
        try:
            yield
        finally:
            self._run_task.reset_command_fn()

    def get_container(self, settings: SerializationSettings) -> Container:
        with self.prepare_target():
            return self._run_task.get_container(settings)

    def get_k8s_pod(self, settings: SerializationSettings) -> K8sPod:
        with self.prepare_target():
            return self._run_task.get_k8s_pod(settings)

    def get_sql(self, settings: SerializationSettings) -> Sql:
        with self.prepare_target():
            return self._run_task.get_sql(settings)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return ArrayJob(parallelism=self._max_concurrency, min_success_ratio=self._min_success_ratio).to_dict()

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        return self._run_task.get_config(settings)

    @property
    def run_task(self) -> PythonFunctionTask:
        return self._run_task

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            return self._execute_map_task(ctx, **kwargs)

        return self._raw_execute(**kwargs)

    @staticmethod
    def _compute_array_job_index() -> int:
        """
        Computes the absolute index of the current array job. This is determined by summing the compute-environment-specific
        environment variable and the offset (if one's set). The offset will be set and used when the user request that the
        job runs in a number of slots less than the size of the input.
        """
        return int(os.environ.get("BATCH_JOB_ARRAY_INDEX_OFFSET", 0)) + int(
            os.environ.get(os.environ.get("BATCH_JOB_ARRAY_INDEX_VAR_NAME"))
        )

    @property
    def _outputs_interface(self) -> Dict[Any, Variable]:
        """
        We override this method from PythonTask because the dispatch_execute method uses this
        interface to construct outputs. Each instance of an container_array task will however produce outputs
        according to the underlying run_task interface and the array plugin handler will actually create a collection
        from these individual outputs as the final output value.
        """

        ctx = FlyteContextManager.current_context()
        if ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            # In workflow execution mode we actually need to use the parent (mapper) task output interface.
            return self.interface.outputs
        return self._run_task.interface.outputs

    def get_type_for_output_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        """
        We override this method from flytekit.core.base_task Task because the dispatch_execute method uses this
        interface to construct outputs. Each instance of an container_array task will however produce outputs
        according to the underlying run_task interface and the array plugin handler will actually create a collection
        from these individual outputs as the final output value.
        """
        ctx = FlyteContextManager.current_context()
        if ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            # In workflow execution mode we actually need to use the parent (mapper) task output interface.
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
        return exception_scopes.user_entry_point(self._run_task.execute)(**map_task_inputs)

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
            list(self._run_task.interface.inputs.keys())[0]
            if self._run_task.interface.inputs.items() is not None
            else None
        )

        for i in range(len(kwargs[any_input_key])):
            single_instance_inputs = {}
            for k in self.interface.inputs.keys():
                single_instance_inputs[k] = kwargs[k][i]
            o = exception_scopes.user_entry_point(self._run_task.execute)(**single_instance_inputs)
            if outputs_expected:
                outputs.append(o)

        return outputs


def map_task(task_function: PythonFunctionTask, concurrency: int = 0, min_success_ratio: float = 1.0, **kwargs):
    """
    Use a map task for parallelizable tasks that run across a list of an input type. A map task can be composed of
    any individual :py:class:`flytekit.PythonFunctionTask`.

    Invoke a map task with arguments using the :py:class:`list` version of the expected input.

    Usage:

    .. literalinclude:: ../../../tests/flytekit/unit/core/test_map_task.py
       :start-after: # test_map_task_start
       :end-before: # test_map_task_end
       :language: python
       :dedent: 4

    At run time, the underlying map task will be run for every value in the input collection. Attributes
    such as :py:class:`flytekit.TaskMetadata` and ``with_overrides`` are applied to individual instances
    of the mapped task.

    **Map Task Plugins**

    There are two plugins to run maptasks that ship as part of flyteplugins:

    1. K8s Array
    2. `AWS batch <https://docs.flyte.org/en/latest/deployment/plugin_setup/aws/batch.html>`_

    Enabling a plugin is controlled in the plugin configuration at `values-sandbox.yaml <https://github.com/flyteorg/flyte/blob/10cee9f139824512b6c5be1667d321bdbc8835fa/charts/flyte/values-sandbox.yaml#L152-L162>`_.

    **K8s Array**

    By default, the map task uses the ``K8s Array`` plugin. It executes array tasks by launching a pod for every instance in the array. It’s simple to use, has a straightforward implementation, and works out of the box.

    **AWS batch**

    Learn more about ``AWS batch`` setup configuration `here <https://docs.flyte.org/en/latest/deployment/plugin_setup/aws/batch.html#deployment-plugin-setup-aws-array>`_.

    A custom plugin can also be implemented to handle the task type.

    :param task_function: This argument is implicitly passed and represents the repeatable function
    :param concurrency: If specified, this limits the number of mapped tasks than can run in parallel to the given batch
        size. If the size of the input exceeds the concurrency value, then multiple batches will be run serially until
        all inputs are processed. If left unspecified, this means unbounded concurrency.
    :param min_success_ratio: If specified, this determines the minimum fraction of total jobs which can complete
        successfully before terminating this task and marking it successful.

    """
    if not isinstance(task_function, PythonFunctionTask):
        raise ValueError(
            f"Only Flyte python task types are supported in map tasks currently, received {type(task_function)}"
        )
    return MapPythonTask(task_function, concurrency=concurrency, min_success_ratio=min_success_ratio, **kwargs)
