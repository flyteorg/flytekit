import inspect
import re
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import ExecutionState, FlyteContext, ImageConfig, SerializationSettings
from flytekit.core.interface import transform_signature_to_interface
from flytekit.core.resources import Resources, ResourceSpec
from flytekit.core.workflow import Workflow, WorkflowFailurePolicy, WorkflowMetadata, WorkflowMetadataDefaults
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_model

_IMAGE_REPLACE_REGEX = re.compile(r"({{\s*.image[s]?.(\w+).(\w+)\s*}})", re.IGNORECASE)


def get_registerable_container_image(img: Optional[str], cfg: ImageConfig) -> str:
    """
    :param img: Configured image
    :param cfg: Registration configuration
    :return:
    """
    if img is not None and img != "":
        matches = _IMAGE_REPLACE_REGEX.findall(img)
        if matches is None or len(matches) == 0:
            return img
        for m in matches:
            if len(m) < 2:
                raise AssertionError(
                    "Image specification should be of the form <fqn>:<tag> OR <fqn>:{{.image.default.version}} OR "
                    f"{{.image.xyz.fqn}}:{{.image.xyz.version}} - Received {m}"
                )
            replace_group, name, attr = m
            if name is None or name == "" or attr is None or attr == "":
                raise AssertionError(f"Image format is incorrect {m}")
            img_cfg = cfg.find_image(name)
            if img_cfg is None:
                raise AssertionError(f"Image Config with name {name} not found in the configuration")
            if attr == "version":
                if img_cfg.tag is not None:
                    img = img.replace(replace_group, img_cfg.tag)
                else:
                    img = img.replace(replace_group, cfg.default_image.tag)
            elif attr == "fqn":
                img = img.replace(replace_group, img_cfg.fqn)
            else:
                raise AssertionError(f"Only fqn and version are supported replacements, {attr} is not supported")
        return img
    return f"{cfg.default_image.fqn}:{cfg.default_image.tag}"


T = TypeVar("T")


class PythonAutoContainerTask(PythonTask[T], ABC):
    """
    A Python AutoContainer task should be used as the base for all extensions that want the users code to be in the container
    and the container information to be automatically captured.
    This base will auto configure the image and image version to be used for all its derivatives.

    If you are looking to extend, you might prefer to use ``PythonFunctionTask`` or ``PythonInstanceTask``
    """

    def __init__(
        self,
        name: str,
        task_config: T,
        task_type="python-task",
        container_image: Optional[str] = None,
        requests: Optional[Resources] = None,
        limits: Optional[Resources] = None,
        environment: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        :param task_config: Configuration object for Task. Should be a unique type for that specific Task
        :param task_function: Python function that has type annotations and works for the task
        :param metadata: Task execution metadata e.g. retries, timeout etc
        :param ignore_input_vars:
        :param task_type: String task type to be associated with this Task
        :param container_image: String FQN for the image.
        :param Resources requests: custom resource request settings.
        :param Resources limits: custom resource limit settings.
        """
        super().__init__(
            task_type=task_type, name=name, task_config=task_config, **kwargs,
        )
        self._container_image = container_image
        # TODO(katrogan): Implement resource overrides
        self._resources = ResourceSpec(
            requests=requests if requests else Resources(), limits=limits if limits else Resources()
        )
        self._environment = environment

    @property
    def container_image(self) -> Optional[str]:
        return self._container_image

    @property
    def resources(self) -> ResourceSpec:
        return self._resources

    @abstractmethod
    def get_command(self, settings: SerializationSettings) -> List[str]:
        pass

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        env = {**settings.env, **self.environment} if self.environment else settings.env
        return _get_container_definition(
            image=get_registerable_container_image(self.container_image, settings.image_config),
            command=[],
            args=self.get_command(settings=settings),
            data_loading_config=None,
            environment=env,
            storage_request=self.resources.requests.storage,
            cpu_request=self.resources.requests.cpu,
            gpu_request=self.resources.requests.gpu,
            memory_request=self.resources.requests.mem,
            storage_limit=self.resources.limits.storage,
            cpu_limit=self.resources.limits.cpu,
            gpu_limit=self.resources.limits.gpu,
            memory_limit=self.resources.limits.mem,
        )


def isnested(func) -> bool:
    """
    Returns true if a function is local to another function and is not accessible through a module

    This would essentially be any function with a `.<local>.` (defined within a function) e.g.

    .. code:: python

        def foo():
            def foo_inner():
                pass
            pass

    In the above example `foo_inner` is the local function or a nested function.
    """
    return func.__code__.co_flags & inspect.CO_NESTED != 0


def istestfunction(func) -> bool:
    """
    Returns true if the function is defined in a test module. A test module has to have `test_` as the prefix.
    False in all other cases
    """
    mod = inspect.getmodule(func)
    if mod:
        mod_name = mod.__name__
        if "." in mod_name:
            mod_name = mod_name.split(".")[-1]
        return mod_name.startswith("test_")
    return False


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
        if not istestfunction(func=task_function) and isnested(func=task_function):
            raise ValueError(
                "TaskFunction cannot be a nested/inner or local function. "
                "It should be accessible at a module level for Flyte to execute it. Test modules with "
                "names begining with `test_` are allowed to have nested tasks"
            )
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        mutated_interface = self._native_interface.remove_inputs(ignore_input_vars)
        super().__init__(
            task_type=task_type,
            name=f"{task_function.__module__}.{task_function.__name__}",
            interface=mutated_interface,
            task_config=task_config,
            **kwargs,
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

    def get_command(self, settings: SerializationSettings) -> List[str]:
        return [
            "pyflyte-execute",
            "--task-module",
            self._task_function.__module__,
            "--task-name",
            self._task_function.__name__,
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
        ]

    def compile_into_workflow(
        self, ctx: FlyteContext, task_function: Callable, **kwargs
    ) -> Union[_dynamic_job.DynamicJobSpec, _literal_models.LiteralMap]:
        with ctx.new_compilation_context(prefix="dynamic"):
            # TODO: Resolve circular import
            from flytekit.common.translator import get_serializable

            workflow_metadata = WorkflowMetadata(on_failure=WorkflowFailurePolicy.FAIL_IMMEDIATELY)
            defaults = WorkflowMetadataDefaults(interruptible=False)

            self._wf = Workflow(task_function, metadata=workflow_metadata, default_metadata=defaults)
            self._wf.compile(**kwargs)

            wf = self._wf
            sdk_workflow = get_serializable(ctx.serialization_settings, wf)

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
            return self.compile_into_workflow(ctx, task_function, **kwargs)


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

    def __init__(self, name: str, task_config: T, task_type: str = "python-task", **kwargs):
        super().__init__(name=name, task_config=task_config, task_type=task_type, **kwargs)

    def get_command(self, settings: SerializationSettings) -> List[str]:
        """
        NOTE: This command is different, it tries to retrieve the actual LHS of where this object was assigned, so that
        the module loader can easily retreive this for execution - at runtime.
        """
        var = settings.get_instance_var(self)
        return [
            "pyflyte-execute",
            "--task-module",
            var.module,
            "--task-name",
            var.name,
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
        ]
