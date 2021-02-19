import inspect
from typing import Any, Optional, Callable, Union

from flytekit.core.python_function_task import PythonFunctionTask
from flytekit.models.types import OutputReference

from flytekit.core.condition import ConditionalSection
from flytekit.core.promise import binding_from_python_std, Promise
from flytekit.models.array_job import ArrayJob

from flytekit.core.workflow import WorkflowMetadata, WorkflowFailurePolicy, WorkflowMetadataDefaults, Workflow
from flytekit.models.dynamic_job import DynamicJobSpec

from flytekit.models.literals import LiteralMap, BindingData, Binding, BindingDataCollection

from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.context_manager import ExecutionState, FlyteContext
from flytekit.core.interface import transform_interface_to_list_interface, transform_signature_to_interface
from flytekit.loggers import logger


class MapPythonTask(PythonTask):
    """
    TODO We might need a special entrypoint to start execution of this task type as there is possibly no instance of this
    type and it needs to be dynamically generated at runtime. We can easily generate it by passing it the actual task
    that is to be generated.

    To do this we might have to give up on supporting lambda functions initially
    """

    def __init__(self, tk: PythonFunctionTask, metadata: Optional[TaskMetadata] = None, concurrency=None, **kwargs):
        collection_interface = transform_interface_to_list_interface(tk.python_interface)
        name = f"{tk._task_function.__module__}.mapper_{tk._task_function.__name__}"
        self._run_task = tk
        self._max_concurrency = concurrency
        self._array_task_interface = tk.python_interface
        super().__init__(
            name=name,
            interface=collection_interface,
            metadata=metadata,
            task_type="map_task",
            task_config=None,
            **kwargs,
        )

    @property
    def run_task(self) -> PythonTask:
        return self._run_task

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContext.current_context()
        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            with ctx.new_execution_context(ExecutionState.Mode.TASK_EXECUTION):
                logger.info("Executing Dynamic workflow, using raw inputs")
                return self._raw_execute(**kwargs)

        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            return self.compile_into_workflow(ctx, self.run_task, **kwargs)

    def compile_into_workflow(self, ctx: FlyteContext, **kwargs) -> Union[DynamicJobSpec, LiteralMap]:

        # TODO(katrogan): Add support for collection type inputs.
        if self._are_inputs_collection_type():
            logger.info("Not handling collection type inputs, defaulting to raw execution")
            return self._raw_execute(**kwargs)

        with ctx.new_compilation_context(prefix="dynamic") as comp_ctx:
            # TODO: Resolve circular import
            from flytekit.common.translator import get_serializable

            # Let's create a workflow, with one node that runs a container array task
            collection_interface = self.python_interface
            self._python_interface = self._array_task_interface
            compile_once_inputs = {k: v[0] for (k, v) in kwargs.items()}
            self.run_task.compile(comp_ctx, **compile_once_inputs)
            # self.compile(comp_ctx, **compile_once_inputs)
            self._python_interface = collection_interface
            workflow_nodes = comp_ctx.compilation_state.nodes
            if len(workflow_nodes) != 1:
                logger.error(f"Expected one node when compiling map task, got f{len(workflow_nodes)}")

            sdk_workflow_node = workflow_nodes[0]
            # Iterate through the workflow outputs
            output_names = list(self.interface.outputs.keys())

            any_key = (
                list(self._run_task.interface.inputs.keys())[0]
                if self._run_task.interface.inputs.items() is not None
                else None
            )
            outputs_expected = True
            if not self.interface.outputs:
                outputs_expected = False

            node_id = sdk_workflow_node.id
            bindings = []
            if outputs_expected:
                for output_name in output_names:
                    binding_data_collection = []
                    for i in range(len(kwargs[any_key])):
                        promise = OutputReference(node_id, f"[{i}].{output_name}")
                        binding_data = BindingData(promise=promise)
                        binding_data_collection.append(binding_data)
                    binding = Binding(
                        var=output_name, binding=BindingData(collection=BindingDataCollection(binding_data_collection))
                    )
                    bindings.append(binding)

        sdk_node = get_serializable(ctx.serialization_settings, sdk_workflow_node)

        array_job = ArrayJob(
            parallelism=self._max_concurrency if self._max_concurrency else 0, size=1, min_successes=1,
        )
        sdk_task_node = sdk_node.task_node
        sdk_task_node.sdk_task.assign_custom_and_return(array_job.to_dict()).assign_type_and_return("container_array")

        dj_spec = DynamicJobSpec(
            min_successes=1, tasks=[sdk_task_node.sdk_task], nodes=[sdk_node], outputs=bindings, subworkflows=[],
        )

        logger.info("dj_spec {}".format(dj_spec))
        return dj_spec

    def _are_inputs_collection_type(self) -> bool:
        all_types_are_collection = True
        for k, v in self._run_task.interface.inputs.items():
            if v.type.collection_type is None:
                all_types_are_collection = False
                break
        return all_types_are_collection

    def _raw_execute(self, **kwargs) -> Any:
        all_types_are_collection = self._are_inputs_collection_type()
        any_key = (
            list(self._run_task.interface.inputs.keys())[0]
            if self._run_task.interface.inputs.items() is not None
            else None
        )

        # If all types are collection we can just handle the call as a pass through
        if all_types_are_collection:
            return self._run_task.execute(**kwargs)

        # If all types are not collection then we need to perform batching
        batch = {}

        outputs_expected = True
        if not self.interface.outputs:
            outputs_expected = False
        outputs = []
        for k in self.interface.outputs.keys():
            outputs.append([])
        for i in range(len(kwargs[any_key])):
            for k in self.interface.inputs.keys():
                batch[k] = kwargs[k][i]
            o = self._run_task.execute(**batch)
            if outputs_expected:
                for x in range(len(outputs)):
                    outputs[x].append(o[x])
        if len(outputs) == 1:
            return outputs[0]

        return tuple(outputs)


def maptask(tk: PythonTask, concurrency="auto", metadata=None):
    if not isinstance(tk, PythonTask):
        raise ValueError(f"Only Flyte Task types are supported in maptask currently, received {type(tk)}")
    # We could register in a global singleton here?
    return MapPythonTask(tk, metadata=metadata)
