import collections
import datetime as _datetime
import functools
import inspect
import re
from abc import abstractmethod
from enum import Enum
from typing import Any, Callable, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union

from flytekit.annotated.context_manager import BranchEvalMode, ExecutionState, FlyteContext, FlyteEntities
from flytekit.annotated.interface import (
    Interface,
    transform_interface_to_list_interface,
    transform_interface_to_typed_interface,
    transform_signature_to_interface,
)
from flytekit.annotated.node import create_and_link_node
from flytekit.annotated.promise import Promise, create_task_output, translate_inputs_to_literals
from flytekit.annotated.type_engine import TypeEngine
from flytekit.annotated.workflow import Workflow
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks.raw_container import _get_container_definition
from flytekit.common.tasks.task import SdkTask
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_model
from flytekit.models.core import identifier as _identifier_model


def kwtypes(**kwargs) -> Dict[str, Type]:
    """
    Converts the keyword arguments to typed dictionary
    """
    d = collections.OrderedDict()
    for k, v in kwargs.items():
        d[k] = v
    return d


# This is the least abstract task. It will have access to the loaded Python function
# itself if run locally, so it will always be a Python task.
# This is analogous to the current SdkRunnableTask. Need to analyze the benefits of duplicating the class versus
# adding to it. Also thinking that the relationship to SdkTask should be a has-one relationship rather than an is-one.
# I'm not attached to this class at all, it's just here as a stand-in. Everything in this PR is subject to change.
#
# I think the class layers are IDL -> Model class -> SdkBlah class. While the model and generated-IDL classes
# obviously encapsulate the IDL, the SdkTask/Workflow/Launchplan/Node classes should encapsulate the control plane.
# That is, all the control plane interactions we wish to build should belong there. (I think this is how it's done
# already.)
class Task(object):
    def __init__(
        self,
        task_type: str,
        name: str,
        interface: _interface_models.TypedInterface,
        metadata: _task_model.TaskMetadata,
        *args,
        **kwargs,
    ):
        self._task_type = task_type
        self._name = name
        self._interface = interface
        self._metadata = metadata

        # This will get populated only at registration time, when we retrieve the rest of the environment variables like
        # project/domain/version/image and anything else we might need from the environment in the future.
        self._registerable_entity: Optional[SdkTask] = None

        FlyteEntities.entities.append(self)

    @property
    def interface(self) -> _interface_models.TypedInterface:
        return self._interface

    @property
    def metadata(self) -> _task_model.TaskMetadata:
        return self._metadata

    @property
    def name(self) -> str:
        return self._name

    @property
    def task_type(self) -> str:
        return self._task_type

    def get_type_for_input_var(self, k: str, v: Any) -> type:
        """
        Returns the python native type for the given input variable
        # TODO we could use literal type to determine this
        """
        return type(v)

    def get_type_for_output_var(self, k: str, v: Any) -> type:
        """
        Returns the python native type for the given output variable
        # TODO we could use literal type to determine this
        """
        return type(v)

    def get_input_types(self) -> Dict[str, type]:
        """
        Returns python native types for inputs. In case this is not a python native task (base class) and hence
        returns a None. we could deduce the type from literal types, but that is not a required excercise
        # TODO we could use literal type to determine this
        """
        return None

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, None]:
        """
        This code is used only in the case when we want to dispatch_execute with outputs from a previous node
        For regular execution, dispatch_execute is invoked directly.
        """
        # Unwrap the kwargs values. After this, we essentially have a LiteralMap
        # The reason why we need to do this is because the inputs during local execute can be of 2 types
        #  - Promises or native constants
        #  Promises as essentially inputs from previous task executions
        #  native constants are just bound to this specific task (default values for a task input)
        #  Also alongwith promises and constants, there could be dictionary or list of promises or constants
        kwargs = translate_inputs_to_literals(
            ctx, input_kwargs=kwargs, interface=self.interface, native_input_types=self.get_input_types()
        )
        input_literal_map = _literal_models.LiteralMap(literals=kwargs)

        outputs_literal_map = self.dispatch_execute(ctx, input_literal_map)
        outputs_literals = outputs_literal_map.literals

        # TODO maybe this is the part that should be done for local execution, we pass the outputs to some special
        #    location, otherwise we dont really need to right? The higher level execute could just handle literalMap
        # After running, we again have to wrap the outputs, if any, back into Promise objects
        output_names = list(self.interface.outputs.keys())
        if len(output_names) != len(outputs_literals):
            # Length check, clean up exception
            raise AssertionError(f"Length difference {len(output_names)} {len(outputs_literals)}")

        vals = [Promise(var, outputs_literals[var]) for var in output_names]
        return create_task_output(vals)

    def __call__(self, *args, **kwargs):
        # When a Task is () aka __called__, there are three things we may do:
        #  a. Task Execution Mode - just run the Python function as Python normally would. Flyte steps completely
        #     out of the way.
        #  b. Compilation Mode - this happens when the function is called as part of a workflow (potentially
        #     dynamic task?). Instead of running the user function, produce promise objects and create a node.
        #  c. Workflow Execution Mode - when a workflow is being run locally. Even though workflows are functions
        #     and everything should be able to be passed through naturally, we'll want to wrap output values of the
        #     function into objects, so that potential .with_cpu or other ancillary functions can be attached to do
        #     nothing. Subsequent tasks will have to know how to unwrap these. If by chance a non-Flyte task uses a
        #     task output as an input, things probably will fail pretty obviously.
        if len(args) > 0:
            raise _user_exceptions.FlyteAssertion(
                f"In Flyte workflows, on keyword args are supported to pass inputs to workflows and tasks."
                f"Aborting execution as detected {len(args)} positional args {args}"
            )

        ctx = FlyteContext.current_context()
        if ctx.compilation_state is not None and ctx.compilation_state.mode == 1:
            return self.compile(ctx, *args, **kwargs)
        elif (
            ctx.execution_state is not None and ctx.execution_state.mode == ExecutionState.Mode.LOCAL_WORKFLOW_EXECUTION
        ):
            if ctx.execution_state.branch_eval_mode == BranchEvalMode.BRANCH_SKIPPED:
                return
            return self._local_execute(ctx, **kwargs)
        else:
            logger.warning("task run without context - executing raw function")
            return self.execute(**kwargs)

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        raise Exception("not implemented")

    def get_task_structure(self) -> SdkTask:
        settings = FlyteContext.current_context().registration_settings
        tk = SdkTask(
            type=self.task_type,
            metadata=self.metadata,
            interface=self.interface,
            custom=self.get_custom(),
            container=self.get_container(),
        )
        # Reset just to make sure it's what we give it
        tk.id._project = settings.project
        tk.id._domain = settings.domain
        tk.id._name = self.name
        tk.id._version = settings.version
        return tk

    def get_container(self) -> _task_model.Container:
        return None

    def get_custom(self) -> Dict[str, Any]:
        return None

    @abstractmethod
    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap,
    ) -> _literal_models.LiteralMap:
        """
        This method translates Flyte's Type system based input values and invokes the actual call to the executor
        This method is also invoked during runtime.
        """
        pass

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass


class PythonTask(Task):
    def __init__(
        self, task_type: str, name: str, interface: Interface, metadata: _task_model.TaskMetadata, *args, **kwargs
    ):
        super().__init__(task_type, name, transform_interface_to_typed_interface(interface), metadata)
        self._python_interface = interface

    # TODO lets call this interface and the other as flyte_interface?
    @property
    def python_interface(self):
        return self._python_interface

    def get_type_for_input_var(self, k: str, v: Any) -> type:
        return self._python_interface.inputs[k]

    def get_type_for_output_var(self, k: str, v: Any) -> type:
        return self._python_interface.outputs[k]

    def get_input_types(self) -> Dict[str, type]:
        return self._python_interface.inputs

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        return create_and_link_node(
            ctx,
            entity=self,
            interface=self.python_interface,
            timeout=self.metadata.timeout,
            retry_strategy=self.metadata.retries,
            **kwargs,
        )

    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This method translates Flyte's Type system based input values and invokes the actual call to the executor
        This method is also invoked during runtime.
        """

        # TODO We could support default values here too - but not part of the plan right now
        # Translate the input literals to Python native
        native_inputs = TypeEngine.literal_map_to_kwargs(ctx, input_literal_map, self.python_interface.inputs)

        # TODO: Logger should auto inject the current context information to indicate if the task is running within
        #   a workflow or a subworkflow etc
        logger.info(f"Invoking {self.name} with inputs: {native_inputs}")
        try:
            native_outputs = self.execute(**native_inputs)
        except Exception as e:
            logger.exception(f"Exception when executing {e}")
            raise e
        logger.info(f"Task executed successfully in user level, outputs: {native_outputs}")

        # Short circuit the translation to literal map because what's returned may be a dj spec (or an
        # already-constructed LiteralMap if the dynamic task was a no-op), not python native values
        if isinstance(native_outputs, _literal_models.LiteralMap) or isinstance(
            native_outputs, _dynamic_job.DynamicJobSpec
        ):
            return native_outputs

        expected_output_names = list(self.interface.outputs.keys())
        if len(expected_output_names) == 1:
            native_outputs_as_map = {expected_output_names[0]: native_outputs}
        else:
            # Question: How do you know you're going to enumerate them in the correct order? Even if autonamed, will
            # output2 come before output100 if there's a hundred outputs? We don't! We'll have to circle back to
            # the Python task instance and inspect annotations again. Or we change the Python model representation
            # of the interface to be an ordered dict and we fill it in correctly to begin with.
            native_outputs_as_map = {expected_output_names[i]: native_outputs[i] for i, _ in enumerate(native_outputs)}

        # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
        # built into the IDL that all the values of a literal map are of the same type.
        outputs_literal_map = _literal_models.LiteralMap(
            literals={
                k: TypeEngine.to_literal(ctx, v, self.get_type_for_output_var(k, v), self.interface.outputs[k].type)
                for k, v in native_outputs_as_map.items()
            }
        )
        return outputs_literal_map

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        pass

    def get_registerable_entity(self) -> SdkTask:
        if self._registerable_entity is not None:
            return self._registerable_entity
        self._registerable_entity = self.get_task_structure()
        return self._registerable_entity


class ContainerTask(PythonTask):
    class MetadataFormat(Enum):
        JSON = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_JSON
        YAML = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_YAML
        PROTO = _task_model.DataLoadingConfig.LITERALMAP_FORMAT_PROTO

    class IOStrategy(Enum):
        DOWNLOAD_EAGER = _task_model.IOStrategy.DOWNLOAD_MODE_EAGER
        DOWNLOAD_STREAM = _task_model.IOStrategy.DOWNLOAD_MODE_STREAM
        DO_NOT_DOWNLOAD = _task_model.IOStrategy.DOWNLOAD_MODE_NO_DOWNLOAD
        UPLOAD_EAGER = _task_model.IOStrategy.UPLOAD_MODE_EAGER
        UPLOAD_ON_EXIT = _task_model.IOStrategy.UPLOAD_MODE_ON_EXIT
        DO_NOT_UPLOAD = _task_model.IOStrategy.UPLOAD_MODE_NO_UPLOAD

    def __init__(
        self,
        name: str,
        image: str,
        metadata: _task_model.TaskMetadata,
        inputs: Dict[str, Type],
        command: List[str],
        arguments: List[str] = None,
        outputs: Dict[str, Type] = None,
        input_data_dir: str = None,
        output_data_dir: str = None,
        metadata_format: MetadataFormat = MetadataFormat.JSON,
        io_strategy: IOStrategy = None,
        *args,
        **kwargs,
    ):
        super().__init__(
            task_type="raw-container",
            name=name,
            interface=Interface(inputs, outputs),
            metadata=metadata,
            *args,
            **kwargs,
        )
        self._image = image
        self._cmd = command
        self._args = arguments
        self._input_data_dir = input_data_dir
        self._output_data_dir = output_data_dir
        self._md_format = metadata_format
        self._io_strategy = io_strategy

    def execute(self, **kwargs) -> Any:
        print(kwargs)
        print(
            f"\ndocker run --rm -v /tmp/inputs:{self._input_data_dir} -v /tmp/outputs:{self._output_data_dir} -e x=y "
            f"{self._image} {self._cmd} {self._args}"
        )
        return None

    def get_container(self) -> _task_model.Container:
        settings = FlyteContext.current_context().registration_settings
        env = settings.env
        return _get_container_definition(
            image=settings.image,
            command=self._cmd,
            args=self._args,
            data_loading_config=_task_model.DataLoadingConfig(
                input_path=self._input_data_dir,
                output_path=self._output_data_dir,
                format=self._md_format.value,
                enabled=True,
                io_strategy=self._io_strategy.value if self._io_strategy else None,
            ),
            environment=env,
        )


T = TypeVar("T")


class PythonFunctionTask(PythonTask, Generic[T]):
    """
    A Python Function task should be used as the base for all extensions that have a python function.
    This base has an interesting property, where it will auto configure the image and image version to be
    used for all the derivatives.
    """

    def __init__(
        self,
        task_config: T,
        task_function: Callable,
        metadata: _task_model.TaskMetadata,
        ignore_input_vars: List[str] = None,
        task_type="python-task",
        *args,
        **kwargs,
    ):
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        mutated_interface = self._native_interface.remove_inputs(ignore_input_vars)
        super().__init__(
            task_type=task_type,
            name=f"{task_function.__module__}.{task_function.__name__}",
            interface=mutated_interface,
            metadata=metadata,
            *args,
            **kwargs,
        )
        self._task_function = task_function
        self._task_config = task_config

    def execute(self, **kwargs) -> Any:
        return self._task_function(**kwargs)

    @property
    def native_interface(self) -> Interface:
        return self._native_interface

    @property
    def task_config(self) -> T:
        return self._task_config

    def get_container(self) -> _task_model.Container:
        settings = FlyteContext.current_context().registration_settings
        args = [
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
        env = settings.env
        return _get_container_definition(
            image=settings.image, command=[], args=args, data_loading_config=None, environment=env
        )


class MapPythonTask(PythonTask):
    """
    TODO We might need a special entrypoint to start execution of this task type as there is possibly no instance of this
    type and it needs to be dynamically generated at runtime. We can easily generate it by passing it the actual task
    that is to be generated.

    To do this we might have to give up on supporting lambda functions initially
    """

    def __init__(self, tk: PythonTask, metadata: _task_model.TaskMetadata, *args, **kwargs):
        collection_interface = transform_interface_to_list_interface(tk.python_interface)
        name = "mapper_" + tk.name
        self._run_task = tk
        super().__init__(
            name=name, interface=collection_interface, metadata=metadata, task_type="map_task", *args, **kwargs
        )

    @property
    def run_task(self) -> PythonTask:
        return self._run_task

    def execute(self, **kwargs) -> Any:
        all_types_are_collection = True
        any_key = None
        for k, v in self._run_task.interface.inputs.items():
            any_key = k
            if v.type.collection_type is None:
                all_types_are_collection = False
                break

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


class SQLTask(PythonTask):
    """
    Base task types for all SQL tasks
    """

    # TODO this should be replaced with Schema Type
    _OUTPUTS = kwtypes(results=str)
    _INPUT_REGEX = re.compile(r"({{\s*.inputs.(\w+)\s*}})", re.IGNORECASE)

    def __init__(
        self,
        name: str,
        query_template: str,
        inputs: Dict[str, Type],
        metadata: _task_model.TaskMetadata,
        task_type="sql_task",
        *args,
        **kwargs,
    ):
        super().__init__(
            task_type=task_type,
            name=name,
            interface=Interface(inputs=inputs, outputs=self._OUTPUTS),
            metadata=metadata,
            *args,
            **kwargs,
        )
        self._query_template = query_template

    @property
    def query_template(self) -> str:
        return self._query_template

    def execute(self, **kwargs) -> Any:
        modified_query = self._query_template
        matched = set()
        for match in self._INPUT_REGEX.finditer(self._query_template):
            expr = match.groups()[0]
            var = match.groups()[1]
            if var not in kwargs:
                raise ValueError(f"Variable {var} in Query (part of {expr}) not found in inputs {kwargs.keys()}")
            matched.add(var)
            val = kwargs[var]
            # str conversion should be deliberate, with right conversion for each type
            modified_query = modified_query.replace(expr, str(val))

        if len(matched) < len(kwargs.keys()):
            diff = set(kwargs.keys()).difference(matched)
            raise ValueError(f"Extra Inputs have not matches in query template - missing {diff}")
        return None


class _Dynamic(object):
    pass


class DynamicWorkflowTask(PythonFunctionTask[_Dynamic]):
    def __init__(
        self,
        task_config: _Dynamic,
        dynamic_workflow_function: Callable,
        metadata: _task_model.TaskMetadata,
        *args,
        **kwargs,
    ):
        super().__init__(
            task_config=task_config,
            task_function=dynamic_workflow_function,
            metadata=metadata,
            task_type="dynamic-task",
            *args,
            **kwargs,
        )

    def compile_into_workflow(
        self, ctx: FlyteContext, **kwargs
    ) -> Union[_dynamic_job.DynamicJobSpec, _literal_models.LiteralMap]:
        with ctx.new_compilation_context(prefix="dynamic"):
            self._wf = Workflow(self._task_function)
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


class Reference(object):
    def __init__(
        self, project: str, domain: str, name: str, version: str, *args, **kwargs,
    ):
        self._id = _identifier_model.Identifier(_identifier_model.ResourceType.TASK, project, domain, name, version)

    @property
    def id(self) -> _identifier_model.Identifier:
        return self._id


class ReferenceTask(PythonTask):
    """
    fdsa
    """

    def __init__(
        self,
        task_config: Reference,
        task_function: Callable,
        ignored_metadata: _task_model.TaskMetadata,
        *args,
        task_type="reference-task",
        **kwargs,
    ):
        self._reference = task_config
        self._native_interface = transform_signature_to_interface(inspect.signature(task_function))
        super().__init__(
            task_type=task_type,
            name=task_config._id._name,
            interface=self._native_interface,
            metadata=metadata(),
            *args,
            **kwargs,
        )
        self._task_function = task_function

    def execute(self, **kwargs) -> Any:
        raise Exception("Remote reference tasks cannot be run locally. You must mock this out.")

    @property
    def reference(self) -> Reference:
        return self._reference

    @property
    def native_interface(self) -> Interface:
        return self._native_interface

    def get_task_structure(self) -> SdkTask:
        tk = SdkTask(
            type=self.task_type,
            metadata=self.metadata,
            interface=self.interface,
            custom=self.get_custom(),
            container=None,
        )
        # Reset id to ensure it matches user input
        tk._id = self._reference._id
        tk._has_registered = True
        return tk

    @property
    def id(self) -> _identifier_model.Identifier:
        return self._reference._id


class TaskPlugins(object):
    _PYTHONFUNCTION_TASK_PLUGINS: Dict[type, Type[PythonFunctionTask]] = {}

    @classmethod
    def register_pythontask_plugin(cls, plugin_config_type: type, plugin: Type[PythonFunctionTask]):
        if plugin_config_type in cls._PYTHONFUNCTION_TASK_PLUGINS:
            found = cls._PYTHONFUNCTION_TASK_PLUGINS[plugin_config_type]
            if found == plugin:
                return
            raise TypeError(
                f"Requesting to register plugin {plugin} - collides with existing plugin {found}"
                f" for type {plugin_config_type}"
            )

        cls._PYTHONFUNCTION_TASK_PLUGINS[plugin_config_type] = plugin

    @classmethod
    def find_pythontask_plugin(cls, plugin_config_type: type) -> Type[PythonFunctionTask]:
        if plugin_config_type in cls._PYTHONFUNCTION_TASK_PLUGINS:
            return cls._PYTHONFUNCTION_TASK_PLUGINS[plugin_config_type]
        # Defaults to returning Base PythonFunctionTask
        return PythonFunctionTask


class Resources(object):
    def __init__(self, cpu=None, mem=None, gpu=None, storage=None):
        self._cpu = cpu
        self._mem = mem
        self._gpu = gpu
        self._storage = storage


def metadata(
    cache: bool = False,
    cache_version: str = "",
    retries: int = 0,
    interruptible: bool = False,
    deprecated: str = "",
    timeout: Union[_datetime.timedelta, int] = None,
) -> _task_model.TaskMetadata:
    return _task_model.TaskMetadata(
        discoverable=cache,
        runtime=_task_model.RuntimeMetadata(_task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.2.3", "python"),
        timeout=timeout,
        retries=_literal_models.RetryStrategy(retries),
        interruptible=interruptible,
        discovery_version=cache_version,
        deprecated_error_message=deprecated,
    )


def task(
    _task_function: Callable = None,
    task_config: Any = None,
    cache: bool = False,
    cache_version: str = "",
    retries: int = 0,
    interruptible: bool = False,
    deprecated: str = "",
    timeout: Union[_datetime.timedelta, int] = 0,
    environment: Dict[str, str] = None,  # TODO: Ketan - what do we do with this?  Not sure how to use kwargs
    *args,
    **kwargs,
) -> Callable:
    def wrapper(fn) -> PythonFunctionTask:
        if isinstance(timeout, int):
            _timeout = _datetime.timedelta(seconds=timeout)
        elif timeout and isinstance(timeout, _datetime.timedelta):
            _timeout = timeout
        else:
            raise ValueError("timeout should be duration represented as either a datetime.timedelta or int seconds")

        _metadata = metadata(cache, cache_version, retries, interruptible, deprecated, _timeout)

        task_instance = TaskPlugins.find_pythontask_plugin(type(task_config))(
            task_config, fn, _metadata, *args, **kwargs
        )

        return task_instance

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper


dynamic = functools.partial(task, task_config=_Dynamic())


def _load_default_plugins():
    TaskPlugins.register_pythontask_plugin(_Dynamic, DynamicWorkflowTask)
    TaskPlugins.register_pythontask_plugin(Reference, ReferenceTask)


_load_default_plugins()
