import inspect
from abc import ABC
from collections import OrderedDict
from enum import Enum
from typing import Any, Callable, List, Optional, TypeVar, Union

from flytekit.core.base_task import TaskResolverMixin
from flytekit.core.context_manager import ExecutionState, FlyteContext
from flytekit.core.interface import transform_signature_to_interface
from flytekit.core.python_auto_container import PythonAutoContainerTask, default_task_resolver
from flytekit.core.tracker import isnested, istestfunction
from flytekit.core.workflow import (
    PythonFunctionWorkflow,
    WorkflowFailurePolicy,
    WorkflowMetadata,
    WorkflowMetadataDefaults,
)
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models

T = TypeVar("T")


class PythonInstanceTask(PythonAutoContainerTask[T], ABC):
    """
    This class should be used as the base class for all Tasks that do not have a user defined function body, but have
    a platform defined execute method. (Execute needs to be overriden). This base class ensures that the module loader
    will invoke the right class automatically, by capturing the module name and variable in the module name.

    .. code-block: python

        x = MyInstanceTask(name="x", .....)

        # this can be invoked as
        x(a=5) # depending on the interface of the defined task

    """

    def __init__(
        self,
        name: str,
        task_config: T,
        task_type: str = "python-task",
        task_resolver: Optional[TaskResolverMixin] = None,
        **kwargs,
    ):
        super().__init__(name=name, task_config=task_config, task_type=task_type, task_resolver=task_resolver, **kwargs)


class PythonFunctionTask(PythonAutoContainerTask[T]):
    """
    A Python Function task should be used as the base for all extensions that have a python function. It will
    automatically detect interface of the python function and also, create the write execution command to execute the
    function

    It is advised this task is used using the @task decorator as follows

    .. code-block: python

        @task
        def my_func(a: int) -> str:
           ...

    In the above code, the name of the function, the module, and the interface (inputs = int and outputs = str) will be
    auto detected.
    """

    class ExecutionBehavior(Enum):
        DEFAULT = 1
        DYNAMIC = 2

    def __init__(
        self,
        task_config: T,
        task_function: Callable,
        task_type="python-task",
        ignore_input_vars: Optional[List[str]] = None,
        execution_mode: Optional[ExecutionBehavior] = ExecutionBehavior.DEFAULT,
        task_resolver: Optional[TaskResolverMixin] = None,
        **kwargs,
    ):
        """
        :param task_config: Configuration object for Task. Should be a unique type for that specific Task
        :param task_function: Python function that has type annotations and works for the task
        :param ignore_input_vars: When supplied, these input variables will be removed from the interface. This
                                  can be used to inject some client side variables only. Prefer using ExecutionParams
        :param task_type: String task type to be associated with this Task
        """
        if task_function is None:
            raise ValueError("TaskFunction is a required parameter for PythonFunctionTask")
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        mutated_interface = self._native_interface.remove_inputs(ignore_input_vars)
        super().__init__(
            task_type=task_type,
            name=f"{task_function.__module__}.{task_function.__name__}",
            interface=mutated_interface,
            task_config=task_config,
            task_resolver=task_resolver,
            **kwargs,
        )

        if self._task_resolver is default_task_resolver:
            # The default task resolver can't handle nested functions
            # TODO: Consider moving this to a can_handle function or something inside the resolver itself.
            if not istestfunction(func=task_function) and isnested(func=task_function):
                raise ValueError(
                    "TaskFunction cannot be a nested/inner or local function. "
                    "It should be accessible at a module level for Flyte to execute it. Test modules with "
                    "names begining with `test_` are allowed to have nested tasks. If you want to create your own tasks"
                    "use the TaskResolverMixin"
                )
        self._task_function = task_function
        self._execution_mode = execution_mode

    @property
    def execution_mode(self) -> ExecutionBehavior:
        return self._execution_mode

    @property
    def task_function(self):
        return self._task_function

    def execute(self, **kwargs) -> Any:
        """
        This method will be invoked to execute the task. If you do decide to override this method you must also
        handle dynamic tasks or you will no longer be able to use the task as a dynamic task generator.
        """
        if self.execution_mode == self.ExecutionBehavior.DEFAULT:
            return self._task_function(**kwargs)
        elif self.execution_mode == self.ExecutionBehavior.DYNAMIC:
            return self.dynamic_execute(self._task_function, **kwargs)

    def compile_into_workflow(
        self, ctx: FlyteContext, is_fast_execution: bool, task_function: Callable, **kwargs
    ) -> Union[_dynamic_job.DynamicJobSpec, _literal_models.LiteralMap]:
        with ctx.new_compilation_context(prefix="dynamic"):
            # TODO: Resolve circular import
            from flytekit.common.translator import get_serializable

            workflow_metadata = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)
            defaults = WorkflowMetadataDefaults(interruptible=False)

            self._wf = PythonFunctionWorkflow(task_function, metadata=workflow_metadata, default_metadata=defaults)
            self._wf.compile(**kwargs)

            wf = self._wf
            sdk_workflow = get_serializable(OrderedDict(), ctx.serialization_settings, wf, is_fast_execution)

            # If no nodes were produced, let's just return the strict outputs
            if len(sdk_workflow.nodes) == 0:
                return _literal_models.LiteralMap(
                    literals={binding.var: binding.binding.to_literal_model() for binding in sdk_workflow._outputs}
                )

            # Gather underlying tasks/workflows that get referenced. Launch plans are handled by propeller.
            tasks = set()
            sub_workflows = set()
            for n in sdk_workflow.nodes:
                self.aggregate(tasks, sub_workflows, n)

            if is_fast_execution:
                if (
                    not ctx.execution_state
                    or not ctx.execution_state.additional_context
                    or not ctx.execution_state.additional_context.get("dynamic_addl_distro")
                ):
                    raise AssertionError(
                        "Compilation for a dynamic workflow called in fast execution mode but no additional code "
                        "distribution could be retrieved"
                    )
                logger.warn(f"ctx.execution_state.additional_context {ctx.execution_state.additional_context}")
                sanitized_tasks = set()
                for task in tasks:
                    sanitized_args = []
                    for arg in task.container.args:
                        if arg == "{{ .remote_package_path }}":
                            sanitized_args.append(ctx.execution_state.additional_context.get("dynamic_addl_distro"))
                        elif arg == "{{ .dest_dir }}":
                            sanitized_args.append(ctx.execution_state.additional_context.get("dynamic_dest_dir", "."))
                        else:
                            sanitized_args.append(arg)
                    del task.container.args[:]
                    task.container.args.extend(sanitized_args)
                    sanitized_tasks.add(task)

                tasks = sanitized_tasks

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
                    PythonFunctionTask.aggregate(tasks, workflows, sub_node)
        if node.branch_node is not None:
            if node.branch_node.if_else.case.then_node is not None:
                PythonFunctionTask.aggregate(tasks, workflows, node.branch_node.if_else.case.then_node)
            if node.branch_node.if_else.other:
                for oth in node.branch_node.if_else.other:
                    if oth.then_node:
                        PythonFunctionTask.aggregate(tasks, workflows, oth.then_node)
            if node.branch_node.if_else.else_node is not None:
                PythonFunctionTask.aggregate(tasks, workflows, node.branch_node.if_else.else_node)

    def dynamic_execute(self, task_function: Callable, **kwargs) -> Any:
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
                return task_function(**kwargs)

        if ctx.execution_state and ctx.execution_state.mode == ExecutionState.Mode.TASK_EXECUTION:
            is_fast_execution = bool(
                ctx.execution_state
                and ctx.execution_state.additional_context
                and ctx.execution_state.additional_context.get("dynamic_addl_distro")
            )
            return self.compile_into_workflow(ctx, is_fast_execution, task_function, **kwargs)
