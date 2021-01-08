import functools
from typing import Any, Callable, Optional, Union

from flytekit import TaskMetadata
from flytekit.annotated import task
from flytekit.annotated.context_manager import ExecutionState, FlyteContext
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.task import TaskPlugins
from flytekit.annotated.workflow import Workflow, WorkflowFailurePolicy, WorkflowMetadata, WorkflowMetadataDefaults
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models


class _Dynamic(object):
    pass


class DynamicWorkflowTask(PythonFunctionTask[_Dynamic]):
    def __init__(
        self,
        task_config: _Dynamic,
        dynamic_workflow_function: Callable,
        metadata: Optional[TaskMetadata] = None,
        **kwargs,
    ):
        self._wf = None

        super().__init__(
            task_config=task_config,
            task_function=dynamic_workflow_function,
            metadata=metadata,
            task_type="dynamic-task",
            **kwargs,
        )

    def compile_into_workflow(
        self, ctx: FlyteContext, **kwargs
    ) -> Union[_dynamic_job.DynamicJobSpec, _literal_models.LiteralMap]:
        with ctx.new_compilation_context(prefix="dynamic"):
            workflow_metadata = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)
            defaults = WorkflowMetadataDefaults(interruptible=False)

            self._wf = Workflow(self._task_function, metadata=workflow_metadata, default_metadata=defaults)
            self._wf.compile(**kwargs)

            wf = self._wf
            sdk_workflow = wf.get_registerable_entity()

            # If no nodes were produced, let's just return the strict outputs
            if len(sdk_workflow.nodes) == 0:
                return _literal_models.LiteralMap(
                    literals={binding.var: binding.binding.to_literal_model() for binding in sdk_workflow._outputs}
                )

            # Gather underlying tasks/workflows that get referenced. Launch plans are handled by propeller.
            tasks = set()
            sub_workflows = set()
            for n in sdk_workflow.nodes:
                DynamicWorkflowTask.aggregate(tasks, sub_workflows, n)

            dj_spec = _dynamic_job.DynamicJobSpec(
                min_successes=len(sdk_workflow.nodes),
                tasks=list(tasks),
                nodes=sdk_workflow.nodes,
                outputs=sdk_workflow._outputs,
                subworkflows=list(sub_workflows),
            )

            return dj_spec

    @staticmethod
    def aggregate(tasks, workflows, node) -> None:
        if node.task_node is not None:
            tasks.add(node.task_node.sdk_task)
        if node.workflow_node is not None:
            if node.workflow_node.sdk_workflow is not None:
                workflows.add(node.workflow_node.sdk_workflow)
                for sub_node in node.workflow_node.sdk_workflow.nodes:
                    DynamicWorkflowTask.aggregate(tasks, workflows, sub_node)
        if node.branch_node is not None:
            if node.branch_node.if_else.case.then_node is not None:
                DynamicWorkflowTask.aggregate(tasks, workflows, node.branch_node.if_else.case.then_node)
            if node.branch_node.if_else.other:
                for oth in node.branch_node.if_else.other:
                    if oth.then_node:
                        DynamicWorkflowTask.aggregate(tasks, workflows, oth.then_node)
            if node.branch_node.if_else.else_node is not None:
                DynamicWorkflowTask.aggregate(tasks, workflows, node.branch_node.if_else.else_node)

    def execute(self, **kwargs) -> Any:
        """
        By the time this function is invoked, the _local_execute function should have unwrapped the Promises and Flyte
        literal wrappers so that the kwargs we are working with here are now Python native literal values. This
        function is also expected to return Python native literal values.

        Since the user code within a dynamic task constitute a workflow, we have to first compile the workflow, and
        then execute that workflow.

        When running for real in production, the task would stop after the compilation step, and then create a file
        representing that newly generated workflow, instead of executing it.
        """
        ctx = FlyteContext.current_context()

        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION:
            with ctx.new_execution_context(ExecutionState.Mode.TASK_EXECUTION):
                logger.info("Executing Dynamic workflow, using raw inputs")
                return self._task_function(**kwargs)

        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            return self.compile_into_workflow(ctx, **kwargs)


# Register dynamic workflow task
TaskPlugins.register_pythontask_plugin(_Dynamic, DynamicWorkflowTask)
dynamic = functools.partial(task.task, task_config=_Dynamic())
