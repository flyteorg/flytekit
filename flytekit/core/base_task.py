"""
==============================
:mod:`flytekit.core.base_task`
==============================

.. currentmodule:: flytekit.core.base_task

.. autosummary::
   :toctree: generated/

   kwtypes
   PythonTask
   Task
   TaskMetadata
   TaskResolverMixin
   IgnoreOutputs

"""

import collections
import datetime
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar, Union

from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.core.context_manager import FlyteContext, FlyteContextManager, FlyteEntities, SerializationSettings
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.local_cache import LocalTaskCache
from flytekit.core.promise import (
    Promise,
    VoidPromise,
    create_and_link_node,
    create_task_output,
    extract_obj_name,
    flyte_entity_call_handler,
    translate_inputs_to_literals,
)
from flytekit.core.tracker import TrackedInstance
from flytekit.core.type_engine import TypeEngine
from flytekit.loggers import logger
from flytekit.models import dynamic_job as _dynamic_job
from flytekit.models import interface as _interface_models
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_model
from flytekit.models.core import workflow as _workflow_model
from flytekit.models.interface import Variable
from flytekit.models.security import SecurityContext


def kwtypes(**kwargs) -> Dict[str, Type]:
    """
    This is a small helper function to convert the keyword arguments to an OrderedDict of types.

    .. code-block:: python

        kwtypes(a=int, b=str)
    """
    d = collections.OrderedDict()
    for k, v in kwargs.items():
        d[k] = v
    return d


@dataclass
class TaskMetadata(object):
    """
    Metadata for a Task. Things like retries and whether or not caching is turned on, and cache version are specified
    here.

    See the :std:ref:`IDL <idl:protos/docs/core/core:taskmetadata>` for the protobuf definition.

    Args:
        cache (bool): Indicates if caching should be enabled. See :std:ref:`Caching <cookbook:caching>`
        cache_serialize (bool): Indicates if identical (ie. same inputs) instances of this task should be executed in serial when caching is enabled. See :std:ref:`Caching <cookbook:caching>`
        cache_version (str): Version to be used for the cached value
        interruptible (Optional[bool]): Indicates that this task can be interrupted and/or scheduled on nodes with
            lower QoS guarantees that can include pre-emption. This can reduce the monetary cost executions incur at the
            cost of performance penalties due to potential interruptions
        deprecated (str): Can be used to provide a warning message for deprecated task. Absence or empty str indicates
            that the task is active and not deprecated
        retries (int): for retries=n; n > 0, on failures of this task, the task will be retried at-least n number of times.
        timeout (Optional[Union[datetime.timedelta, int]]): the max amount of time for which one execution of this task
            should be executed for. The execution will be terminated if the runtime exceeds the given timeout
            (approximately)
    """

    cache: bool = False
    cache_serialize: bool = False
    cache_version: str = ""
    interruptible: Optional[bool] = None
    deprecated: str = ""
    retries: int = 0
    timeout: Optional[Union[datetime.timedelta, int]] = None

    def __post_init__(self):
        if self.timeout:
            if isinstance(self.timeout, int):
                self.timeout = datetime.timedelta(seconds=self.timeout)
            elif not isinstance(self.timeout, datetime.timedelta):
                raise ValueError("timeout should be duration represented as either a datetime.timedelta or int seconds")
        if self.cache and not self.cache_version:
            raise ValueError("Caching is enabled ``cache=True`` but ``cache_version`` is not set.")
        if self.cache_serialize and not self.cache:
            raise ValueError("Cache serialize is enabled ``cache_serialize=True`` but ``cache`` is not enabled.")

    @property
    def retry_strategy(self) -> _literal_models.RetryStrategy:
        return _literal_models.RetryStrategy(self.retries)

    def to_taskmetadata_model(self) -> _task_model.TaskMetadata:
        """
        Converts to _task_model.TaskMetadata
        """
        from flytekit import __version__

        return _task_model.TaskMetadata(
            discoverable=self.cache,
            runtime=_task_model.RuntimeMetadata(
                _task_model.RuntimeMetadata.RuntimeType.FLYTE_SDK, __version__, "python"
            ),
            timeout=self.timeout,
            retries=self.retry_strategy,
            interruptible=self.interruptible,
            discovery_version=self.cache_version,
            deprecated_error_message=self.deprecated,
            cache_serializable=self.cache_serialize,
        )


class IgnoreOutputs(Exception):
    """
    This exception should be used to indicate that the outputs generated by this can be safely ignored.
    This is useful in case of distributed training or peer-to-peer parallel algorithms.

    For example look at Sagemaker training, e.g.
    :py:class:`plugins.awssagemaker.flytekitplugins.awssagemaker.training.SagemakerBuiltinAlgorithmsTask`.
    """

    pass


class Task(object):
    """
    The base of all Tasks in flytekit. This task is closest to the FlyteIDL TaskTemplate and captures information in
    FlyteIDL specification and does not have python native interfaces associated. Refer to the derived classes for
    examples of how to extend this class.
    """

    def __init__(
        self,
        task_type: str,
        name: str,
        interface: Optional[_interface_models.TypedInterface] = None,
        metadata: Optional[TaskMetadata] = None,
        task_type_version=0,
        security_ctx: Optional[SecurityContext] = None,
        **kwargs,
    ):
        self._task_type = task_type
        self._name = name
        self._interface = interface
        self._metadata = metadata if metadata else TaskMetadata()
        self._task_type_version = task_type_version
        self._security_ctx = security_ctx

        FlyteEntities.entities.append(self)

    @property
    def interface(self) -> Optional[_interface_models.TypedInterface]:
        return self._interface

    @property
    def metadata(self) -> TaskMetadata:
        return self._metadata

    @property
    def name(self) -> str:
        return self._name

    @property
    def task_type(self) -> str:
        return self._task_type

    @property
    def python_interface(self) -> Optional[Interface]:
        return None

    @property
    def task_type_version(self) -> int:
        return self._task_type_version

    @property
    def security_context(self) -> SecurityContext:
        return self._security_ctx

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

    def get_input_types(self) -> Optional[Dict[str, type]]:
        """
        Returns python native types for inputs. In case this is not a python native task (base class) and hence
        returns a None. we could deduce the type from literal types, but that is not a required exercise
        # TODO we could use literal type to determine this
        """
        return None

    def local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, VoidPromise]:
        """
        This function is used only in the local execution path and is responsible for calling dispatch execute.
        Use this function when calling a task with native values (or Promises containing Flyte literals derived from
        Python native values).
        """
        # Unwrap the kwargs values. After this, we essentially have a LiteralMap
        # The reason why we need to do this is because the inputs during local execute can be of 2 types
        #  - Promises or native constants
        #  Promises as essentially inputs from previous task executions
        #  native constants are just bound to this specific task (default values for a task input)
        #  Also along with promises and constants, there could be dictionary or list of promises or constants

        kwargs = translate_inputs_to_literals(
            ctx,
            incoming_values=kwargs,
            flyte_interface_types=self.interface.inputs,  # type: ignore
            native_types=self.get_input_types(),
        )
        input_literal_map = _literal_models.LiteralMap(literals=kwargs)

        # if metadata.cache is set, check memoized version
        if self.metadata.cache:
            # TODO: how to get a nice `native_inputs` here?
            logger.info(
                f"Checking cache for task named {self.name}, cache version {self.metadata.cache_version} "
                f"and inputs: {input_literal_map}"
            )
            outputs_literal_map = LocalTaskCache.get(self.name, self.metadata.cache_version, input_literal_map)
            # The cache returns None iff the key does not exist in the cache
            if outputs_literal_map is None:
                logger.info("Cache miss, task will be executed now")
                outputs_literal_map = self.dispatch_execute(ctx, input_literal_map)
                # TODO: need `native_inputs`
                LocalTaskCache.set(self.name, self.metadata.cache_version, input_literal_map, outputs_literal_map)
                logger.info(
                    f"Cache set for task named {self.name}, cache version {self.metadata.cache_version} "
                    f"and inputs: {input_literal_map}"
                )
            else:
                logger.info("Cache hit")
        else:
            outputs_literal_map = self.dispatch_execute(ctx, input_literal_map)
        outputs_literals = outputs_literal_map.literals

        # TODO maybe this is the part that should be done for local execution, we pass the outputs to some special
        #    location, otherwise we dont really need to right? The higher level execute could just handle literalMap
        # After running, we again have to wrap the outputs, if any, back into Promise objects
        output_names = list(self.interface.outputs.keys())  # type: ignore
        if len(output_names) != len(outputs_literals):
            # Length check, clean up exception
            raise AssertionError(f"Length difference {len(output_names)} {len(outputs_literals)}")

        # Tasks that don't return anything still return a VoidPromise
        if len(output_names) == 0:
            return VoidPromise(self.name)

        vals = [Promise(var, outputs_literals[var]) for var in output_names]
        return create_task_output(vals, self.python_interface)

    def __call__(self, *args, **kwargs):
        return flyte_entity_call_handler(self, *args, **kwargs)

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        raise Exception("not implemented")

    def get_container(self, settings: SerializationSettings) -> _task_model.Container:
        """
        Returns the container definition (if any) that is used to run the task on hosted Flyte.
        """
        return None

    def get_k8s_pod(self, settings: SerializationSettings) -> _task_model.K8sPod:
        """
        Returns the kubernetes pod definition (if any) that is used to run the task on hosted Flyte.
        """
        return None

    def get_sql(self, settings: SerializationSettings) -> Optional[_task_model.Sql]:
        """
        Returns the Sql definition (if any) that is used to run the task on hosted Flyte.
        """
        return None

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        """
        Return additional plugin-specific custom data (if any) as a serializable dictionary.
        """
        return None

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        """
        Returns the task config as a serializable dictionary. This task config consists of metadata about the custom
        defined for this task.
        """
        return None

    @abstractmethod
    def dispatch_execute(
        self,
        ctx: FlyteContext,
        input_literal_map: _literal_models.LiteralMap,
    ) -> _literal_models.LiteralMap:
        """
        This method translates Flyte's Type system based input values and invokes the actual call to the executor
        This method is also invoked during runtime.
        """
        pass

    @abstractmethod
    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        This is the method that will be invoked directly before executing the task method and before all the inputs
        are converted. One particular case where this is useful is if the context is to be modified for the user process
        to get some user space parameters. This also ensures that things like SparkSession are already correctly
        setup before the type transformers are called

        This should return either the same context of the mutated context
        """
        pass

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """
        This method will be invoked to execute the task.
        """
        pass


T = TypeVar("T")


class PythonTask(TrackedInstance, Task, Generic[T]):
    """
    Base Class for all Tasks with a Python native ``Interface``. This should be directly used for task types, that do
    not have a python function to be executed. Otherwise refer to :py:class:`flytekit.PythonFunctionTask`.
    """

    def __init__(
        self,
        task_type: str,
        name: str,
        task_config: T,
        interface: Optional[Interface] = None,
        environment: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """
        Args:
            task_type (str): defines a unique task-type for every new extension. If a backend plugin is required then
                this has to be done in-concert with the backend plugin identifier
            name (str): A unique name for the task instantiation. This is unique for every instance of task.
            task_config (T): Configuration for the task. This is used to configure the specific plugin that handles this
                task
            interface (Optional[Interface]): A python native typed interface ``(inputs) -> outputs`` that declares the
                signature of the task
            environment (Optional[Dict[str, str]]): Any environment variables that should be supplied during the
                execution of the task. Supplied as a dictionary of key/value pairs
        """
        super().__init__(
            task_type=task_type,
            name=name,
            interface=transform_interface_to_typed_interface(interface),
            **kwargs,
        )
        self._python_interface = interface if interface else Interface()
        self._environment = environment if environment else {}
        self._task_config = task_config

    # TODO lets call this interface and the other as flyte_interface?
    @property
    def python_interface(self) -> Interface:
        """
        Returns this task's python interface.
        """
        return self._python_interface

    @property
    def task_config(self) -> T:
        """
        Returns the user-specified task config which is used for plugin-specific handling of the task.
        """
        return self._task_config

    def get_type_for_input_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        """
        Returns the python type for an input variable by name.
        """
        return self._python_interface.inputs[k]

    def get_type_for_output_var(self, k: str, v: Any) -> Optional[Type[Any]]:
        """
        Returns the python type for the specified output variable by name.
        """
        return self._python_interface.outputs[k]

    def get_input_types(self) -> Optional[Dict[str, type]]:
        """
        Returns the names and python types as a dictionary for the inputs of this task.
        """
        return self._python_interface.inputs

    def construct_node_metadata(self) -> _workflow_model.NodeMetadata:
        """
        Used when constructing the node that encapsulates this task as part of a broader workflow definition.
        """
        return _workflow_model.NodeMetadata(
            name=extract_obj_name(self.name),
            timeout=self.metadata.timeout,
            retries=self.metadata.retry_strategy,
            interruptible=self.metadata.interruptible,
        )

    def compile(self, ctx: FlyteContext, *args, **kwargs):
        """
        Generates a node that encapsulates this task in a workflow definition.
        """
        return create_and_link_node(ctx, entity=self, **kwargs)

    @property
    def _outputs_interface(self) -> Dict[Any, Variable]:
        return self.interface.outputs  # type: ignore

    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap
    ) -> Union[_literal_models.LiteralMap, _dynamic_job.DynamicJobSpec]:
        """
        This method translates Flyte's Type system based input values and invokes the actual call to the executor
        This method is also invoked during runtime.

        * ``VoidPromise`` is returned in the case when the task itself declares no outputs.
        * ``Literal Map`` is returned when the task returns either one more outputs in the declaration. Individual outputs
          may be none
        * ``DynamicJobSpec`` is returned when a dynamic workflow is executed
        """

        # Invoked before the task is executed
        new_user_params = self.pre_execute(ctx.user_space_params)

        # Create another execution context with the new user params, but let's keep the same working dir
        with FlyteContextManager.with_context(
            ctx.with_execution_state(ctx.execution_state.with_params(user_space_params=new_user_params))  # type: ignore
        ) as exec_ctx:
            # TODO We could support default values here too - but not part of the plan right now
            # Translate the input literals to Python native
            native_inputs = TypeEngine.literal_map_to_kwargs(exec_ctx, input_literal_map, self.python_interface.inputs)

            # TODO: Logger should auto inject the current context information to indicate if the task is running within
            #   a workflow or a subworkflow etc
            logger.info(f"Invoking {self.name} with inputs: {native_inputs}")
            try:
                native_outputs = self.execute(**native_inputs)
            except Exception as e:
                logger.exception(f"Exception when executing {e}")
                raise e

            logger.info(f"Task executed successfully in user level, outputs: {native_outputs}")
            # Lets run the post_execute method. This may result in a IgnoreOutputs Exception, which is
            # bubbled up to be handled at the callee layer.
            native_outputs = self.post_execute(new_user_params, native_outputs)

            # Short circuit the translation to literal map because what's returned may be a dj spec (or an
            # already-constructed LiteralMap if the dynamic task was a no-op), not python native values
            if isinstance(native_outputs, _literal_models.LiteralMap) or isinstance(
                native_outputs, _dynamic_job.DynamicJobSpec
            ):
                return native_outputs

            expected_output_names = list(self._outputs_interface.keys())
            if len(expected_output_names) == 1:
                # Here we have to handle the fact that the task could've been declared with a typing.NamedTuple of
                # length one. That convention is used for naming outputs - and single-length-NamedTuples are
                # particularly troublesome but elegant handling of them is not a high priority
                # Again, we're using the output_tuple_name as a proxy.
                if self.python_interface.output_tuple_name and isinstance(native_outputs, tuple):
                    native_outputs_as_map = {expected_output_names[0]: native_outputs[0]}
                else:
                    native_outputs_as_map = {expected_output_names[0]: native_outputs}
            elif len(expected_output_names) == 0:
                native_outputs_as_map = {}
            else:
                native_outputs_as_map = {
                    expected_output_names[i]: native_outputs[i] for i, _ in enumerate(native_outputs)
                }

            # We manually construct a LiteralMap here because task inputs and outputs actually violate the assumption
            # built into the IDL that all the values of a literal map are of the same type.
            literals = {}
            for k, v in native_outputs_as_map.items():
                literal_type = self._outputs_interface[k].type
                py_type = self.get_type_for_output_var(k, v)

                if isinstance(v, tuple):
                    raise TypeError(f"Output({k}) in task{self.name} received a tuple {v}, instead of {py_type}")
                try:
                    literals[k] = TypeEngine.to_literal(exec_ctx, v, py_type, literal_type)
                except Exception as e:
                    logger.error(f"Failed to convert return value for var {k} with error {type(e)}: {e}")
                    raise TypeError(
                        f"Failed to convert return value for var {k} for function {self.name} with error {type(e)}: {e}"
                    ) from e

            outputs_literal_map = _literal_models.LiteralMap(literals=literals)
            # After the execute has been successfully completed
            return outputs_literal_map

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        This is the method that will be invoked directly before executing the task method and before all the inputs
        are converted. One particular case where this is useful is if the context is to be modified for the user process
        to get some user space parameters. This also ensures that things like SparkSession are already correctly
        setup before the type transformers are called

        This should return either the same context of the mutated context
        """
        return user_params

    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """
        This method will be invoked to execute the task.
        """
        pass

    def post_execute(self, user_params: ExecutionParameters, rval: Any) -> Any:
        """
        Post execute is called after the execution has completed, with the user_params and can be used to clean-up,
        or alter the outputs to match the intended tasks outputs. If not overridden, then this function is a No-op

        Args:
            rval is returned value from call to execute
            user_params: are the modified user params as created during the pre_execute step
        """
        return rval

    @property
    def environment(self) -> Dict[str, str]:
        """
        Any environment variables that supplied during the execution of the task.
        """
        return self._environment


class TaskResolverMixin(object):
    """
    Flytekit tasks interact with the Flyte platform very, very broadly in two steps. They need to be uploaded to Admin,
    and then they are run by the user upon request (either as a single task execution or as part of a workflow). In any
    case, at execution time, for most tasks (that is those that generate a container target) the container image
    containing the task needs to be spun up again at which point the container needs to know which task it's supposed
    to run and how to rehydrate the task object.

    For example, the serialization of a simple task ::

        # in repo_root/workflows/example.py
        @task
        def t1(...) -> ...: ...

    might result in a container with arguments like ::

        pyflyte-execute --inputs s3://path/inputs.pb --output-prefix s3://outputs/location \
        --raw-output-data-prefix /tmp/data \
        --resolver flytekit.core.python_auto_container.default_task_resolver \
        -- \
        task-module repo_root.workflows.example task-name t1

    At serialization time, the container created for the task will start out automatically with the ``pyflyte-execute``
    bit, along with the requisite input/output args and the offloaded data prefix. Appended to that will be two things,

    #. the ``location`` of the task's task resolver, followed by two dashes, followed by
    #. the arguments provided by calling the ``loader_args`` function below.

    The ``default_task_resolver`` declared below knows that

    * When ``loader_args`` is called on a task, to look up the module the task is in, and the name of the task (the
      key of the task in the module, either the function name, or the variable it was assigned to).
    * When ``load_task`` is called, it interprets the first part of the command as the module to call
      ``importlib.import_module`` on, and then looks for a key ``t1``.

    This is just the default behavior. Users should feel free to implement their own resolvers.
    """

    @property
    @abstractmethod
    def location(self) -> str:
        pass

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def load_task(self, loader_args: List[str]) -> Task:
        """
        Given the set of identifier keys, should return one Python Task or raise an error if not found
        """
        pass

    @abstractmethod
    def loader_args(self, settings: SerializationSettings, t: Task) -> List[str]:
        """
        Return a list of strings that can help identify the parameter Task
        """
        pass

    @abstractmethod
    def get_all_tasks(self) -> List[Task]:
        """
        Future proof method. Just making it easy to access all tasks (Not required today as we auto register them)
        """
        pass

    def task_name(self, t: Task) -> Optional[str]:
        """
        Overridable function that can optionally return a custom name for a given task
        """
        return None
