from __future__ import absolute_import

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec

import six as _six

from flytekit import __version__
from flytekit.common import constants as _constants
from flytekit.common import interface as _interface
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.core.identifier import WorkflowExecutionIdentifier
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import output as _task_output
from flytekit.common.tasks import task as _base_task
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import internal as _internal_config
from flytekit.configuration import resources as _resource_config
from flytekit.configuration import sdk as _sdk_config
from flytekit.engines import loader as _engine_loader
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models


class ExecutionParameters(object):

    """
    This is the parameter object that will be provided as the first parameter for every execution of any @*_task
    decorated function.
    """

    def __init__(self, execution_date, tmp_dir, stats, execution_id, logging):
        self._stats = stats
        self._execution_date = execution_date
        self._working_directory = tmp_dir
        self._execution_id = execution_id
        self._logging = logging

    @property
    def stats(self):
        """
        A handle to a special statsd object that provides usefully tagged stats.

        TODO: Usage examples and better comments

        :rtype: flytekit.interfaces.stats.taggable.TaggableStats
        """
        return self._stats

    @property
    def logging(self):
        """
        A handle to a useful logging object.

        TODO: Usage examples

        :rtype: logging
        """
        return self._logging

    @property
    def working_directory(self):
        """
        A handle to a special working directory for easily producing temporary files.

        TODO: Usage examples

        :rtype: flytekit.common.utils.AutoDeletingTempDir
        """
        return self._working_directory

    @property
    def execution_date(self):
        """
        This is a datetime representing the time at which a workflow was started.  This is consistent across all tasks
        executed in a workflow or sub-workflow.

        .. note::

            Do NOT use this execution_date to drive any production logic.  It might be useful as a tag for data to help
            in debugging.

        :rtype: datetime.datetime
        """
        return self._execution_date

    @property
    def execution_id(self):
        """
        This is the identifier of the workflow execution within the underlying engine.  It will be consistent across all
        task executions in a workflow or sub-workflow execution.

        .. note::

            Do NOT use this execution_id to drive any production logic.  This execution ID should only be used as a tag
            on output data to link back to the workflow run that created it.

        :rtype: Text
        """
        return self._execution_id


class SdkRunnableContainer(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _task_models.Container)):
    def __init__(
        self, command, args, resources, env, config,
    ):
        super(SdkRunnableContainer, self).__init__("", command, args, resources, env or {}, config)

    @property
    def args(self):
        """
        :rtype: list[Text]
        """
        return _sdk_config.SDK_PYTHON_VENV.get() + self._args

    @property
    def image(self):
        """
        :rtype: Text
        """
        return _internal_config.IMAGE.get()

    @property
    def env(self):
        """
        :rtype: dict[Text,Text]
        """
        env = super(SdkRunnableContainer, self).env.copy()
        env.update(
            {
                _internal_config.CONFIGURATION_PATH.env_var: _internal_config.CONFIGURATION_PATH.get(),
                _internal_config.IMAGE.env_var: _internal_config.IMAGE.get(),
                # TODO: Phase out the below.  Propeller will set these and these are not SDK specific
                _internal_config.PROJECT.env_var: _internal_config.PROJECT.get(),
                _internal_config.DOMAIN.env_var: _internal_config.DOMAIN.get(),
                _internal_config.NAME.env_var: _internal_config.NAME.get(),
                _internal_config.VERSION.env_var: _internal_config.VERSION.get(),
            }
        )
        return env


class SdkRunnableTask(_six.with_metaclass(_sdk_bases.ExtendedSdkType, _base_task.SdkTask)):
    """
    This class includes the additional logic for building a task that executes in Python code.  It has even more
    validation checks to ensure proper behavior than it's superclasses.

    Since an SdkRunnableTask is assumed to run by hooking into Python code, we will provide additional shortcuts and
    methods on this object.
    """

    def __init__(
        self,
        task_function,
        task_type,
        discovery_version,
        retries,
        interruptible,
        deprecated,
        storage_request,
        cpu_request,
        gpu_request,
        memory_request,
        storage_limit,
        cpu_limit,
        gpu_limit,
        memory_limit,
        discoverable,
        timeout,
        environment,
        custom,
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param bool interruptible: Specify whether task is interruptible
        :param Text deprecated:
        :param Text storage_request:
        :param Text cpu_request:
        :param Text gpu_request:
        :param Text memory_request:
        :param Text storage_limit:
        :param Text cpu_limit:
        :param Text gpu_limit:
        :param Text memory_limit:
        :param bool discoverable:
        :param datetime.timedelta timeout:
        :param dict[Text, Text] environment:
        :param dict[Text, T] custom:
        """
        self._task_function = task_function

        super(SdkRunnableTask, self).__init__(
            task_type,
            _task_models.TaskMetadata(
                discoverable,
                _task_models.RuntimeMetadata(
                    _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK, __version__, "python",
                ),
                timeout,
                _literal_models.RetryStrategy(retries),
                interruptible,
                discovery_version,
                deprecated,
            ),
            _interface.TypedInterface({}, {}),
            custom,
            container=self._get_container_definition(
                storage_request=storage_request,
                cpu_request=cpu_request,
                gpu_request=gpu_request,
                memory_request=memory_request,
                storage_limit=storage_limit,
                cpu_limit=cpu_limit,
                gpu_limit=gpu_limit,
                memory_limit=memory_limit,
                environment=environment,
            ),
        )
        self.id._name = "{}.{}".format(self.task_module, self.task_function_name)

    _banned_inputs = {}
    _banned_outputs = {}

    @_exception_scopes.system_entry_point
    def add_inputs(self, inputs):
        """
        Adds the inputs to this task.  This can be called multiple times, but it will fail if an input with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] inputs: names and variables
        """
        self._validate_inputs(inputs)
        self.interface.inputs.update(inputs)

    @classmethod
    def promote_from_model(cls, base_model):
        # TODO: If the task exists in this container, we should be able to retrieve it.
        raise _user_exceptions.FlyteAssertion("Cannot promote a base object to a runnable task.")

    @property
    def task_function(self):
        return self._task_function

    @property
    def task_function_name(self):
        """
        :rtype: Text
        """
        return self.task_function.__name__

    @property
    def task_module(self):
        """
        :rtype: Text
        """
        return self._task_function.__module__

    def validate(self):
        super(SdkRunnableTask, self).validate()
        missing_args = self._missing_mapped_inputs_outputs()
        if len(missing_args) > 0:
            raise _user_exceptions.FlyteAssertion(
                "The task {} is invalid because not all inputs and outputs in the "
                "task function definition were specified in @outputs and @inputs. "
                "We are missing definitions for {}.".format(self, missing_args)
            )

    @_exception_scopes.system_entry_point
    def unit_test(self, **input_map):
        """
        :param dict[Text, T] input_map: Python Std input from users.  We will cast these to the appropriate Flyte
            literals.
        :returns: Depends on the behavior of the specific task in the unit engine.
        """
        return (
            _engine_loader.get_engine("unit")
            .get_task(self)
            .execute(
                _type_helpers.pack_python_std_map_to_literal_map(
                    input_map,
                    {
                        k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                        for k, v in _six.iteritems(self.interface.inputs)
                    },
                )
            )
        )

    @_exception_scopes.system_entry_point
    def local_execute(self, **input_map):
        """
        :param dict[Text, T] input_map: Python Std input from users.  We will cast these to the appropriate Flyte
            literals.
        :rtype: dict[Text, T]
        :returns: The output produced by this task in Python standard format.
        """
        return (
            _engine_loader.get_engine("local")
            .get_task(self)
            .execute(
                _type_helpers.pack_python_std_map_to_literal_map(
                    input_map,
                    {
                        k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                        for k, v in _six.iteritems(self.interface.inputs)
                    },
                )
            )
        )

    def _execute_user_code(self, context, inputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param dict[Text, T] inputs: This variable is a bit of a misnomer, since it's both inputs and outputs. The
            dictionary passed here will be passed to the user-defined function, and will have values that are a
            variety of types.  The T's here are Python std values for inputs.  If there isn't a native Python type for
            something (like Schema or Blob), they are the Flyte classes.  For outputs they are OutputReferences.
            (Note that these are not the same OutputReferences as in BindingData's)
        :rtype: Any: the returned object from user code.
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """

        return _exception_scopes.user_entry_point(self.task_function)(
            ExecutionParameters(
                execution_date=context.execution_date,
                # TODO: it might be better to consider passing the full struct
                execution_id=_six.text_type(WorkflowExecutionIdentifier.promote_from_model(context.execution_id)),
                stats=context.stats,
                logging=context.logging,
                tmp_dir=context.working_directory,
            ),
            **inputs
        )

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text, flytekit.models.common.FlyteIdlEntity]
        :returns: This function must return a dictionary mapping 'filenames' to Flyte Interface Entities.  These
            entities will be used by the engine to pass data from node to node, populate metadata, etc. etc..  Each
            engine will have different behavior.  For instance, the Flyte engine will upload the entities to a remote
            working directory (with the names provided), which will in turn allow Flyte Propeller to push along the
            workflow.  Where as local engine will merely feed the outputs directly into the next node.
        """
        inputs_dict = _type_helpers.unpack_literal_map_to_sdk_python_std(
            inputs,
            {k: _type_helpers.get_sdk_type_from_literal_type(v.type) for k, v in _six.iteritems(self.interface.inputs)},
        )
        outputs_dict = {
            name: _task_output.OutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }
        inputs_dict.update(outputs_dict)

        self._execute_user_code(context, inputs_dict)

        return {
            _constants.OUTPUT_FILE_NAME: _literal_models.LiteralMap(
                literals={k: v.sdk_value for k, v in _six.iteritems(outputs_dict)}
            )
        }

    def _get_container_definition(
        self,
        storage_request=None,
        cpu_request=None,
        gpu_request=None,
        memory_request=None,
        storage_limit=None,
        cpu_limit=None,
        gpu_limit=None,
        memory_limit=None,
        environment=None,
        cls=None,
    ):
        """
        :param Text storage_request:
        :param Text cpu_request:
        :param Text gpu_request:
        :param Text memory_request:
        :param Text storage_limit:
        :param Text cpu_limit:
        :param Text gpu_limit:
        :param Text memory_limit:
        :param dict[Text,Text] environment:
        :param cls Optional[type]: Type of container to instantiate. Generally should subclass SdkRunnableContainer.
        :rtype: flytekit.models.task.Container
        """
        storage_limit = storage_limit or _resource_config.DEFAULT_STORAGE_LIMIT.get()
        storage_request = storage_request or _resource_config.DEFAULT_STORAGE_REQUEST.get()
        cpu_limit = cpu_limit or _resource_config.DEFAULT_CPU_LIMIT.get()
        cpu_request = cpu_request or _resource_config.DEFAULT_CPU_REQUEST.get()
        gpu_limit = gpu_limit or _resource_config.DEFAULT_GPU_LIMIT.get()
        gpu_request = gpu_request or _resource_config.DEFAULT_GPU_REQUEST.get()
        memory_limit = memory_limit or _resource_config.DEFAULT_MEMORY_LIMIT.get()
        memory_request = memory_request or _resource_config.DEFAULT_MEMORY_REQUEST.get()

        requests = []
        if storage_request:
            requests.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.STORAGE, storage_request)
            )
        if cpu_request:
            requests.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.CPU, cpu_request))
        if gpu_request:
            requests.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.GPU, gpu_request))
        if memory_request:
            requests.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.MEMORY, memory_request)
            )

        limits = []
        if storage_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.STORAGE, storage_limit)
            )
        if cpu_limit:
            limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.CPU, cpu_limit))
        if gpu_limit:
            limits.append(_task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.GPU, gpu_limit))
        if memory_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(_task_models.Resources.ResourceName.MEMORY, memory_limit)
            )

        return (cls or SdkRunnableContainer)(
            command=[],
            args=[
                "pyflyte-execute",
                "--task-module",
                self.task_module,
                "--task-name",
                self.task_function_name,
                "--inputs",
                "{{.input}}",
                "--output-prefix",
                "{{.outputPrefix}}",
                "--raw-output-data-prefix",
                "{{.rawOutputDataPrefix}}",
            ],
            resources=_task_models.Resources(limits=limits, requests=requests),
            env=environment,
            config={},
        )

    def _validate_inputs(self, inputs):
        """
        This method should be overridden in sub-classes that intend to do additional checks on inputs.  If validation
        fails, this function should raise an informative exception.
        :param dict[Text, flytekit.models.interface.Variable] inputs:  Input variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        super(SdkRunnableTask, self)._validate_inputs(inputs)
        for k, v in _six.iteritems(inputs):
            if not self._is_argname_in_function_definition(k):
                raise _user_exceptions.FlyteValidationException(
                    "The input named '{}' was not specified in the task function.  Therefore, this input cannot be "
                    "provided to the task.".format(k)
                )
            if _type_helpers.get_sdk_type_from_literal_type(v.type) in type(self)._banned_inputs:
                raise _user_exceptions.FlyteValidationException(
                    "The input '{}' is not an accepted input type.".format(v)
                )

    def _validate_outputs(self, outputs):
        """
        This method should be overridden in sub-classes that intend to do additional checks on outputs.  If validation
        fails, this function should raise an informative exception.
        :param dict[Text, flytekit.models.interface.Variable] outputs:  Output variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        super(SdkRunnableTask, self)._validate_outputs(outputs)
        for k, v in _six.iteritems(outputs):
            if not self._is_argname_in_function_definition(k):
                raise _user_exceptions.FlyteValidationException(
                    "The output named '{}' was not specified in the task function.  Therefore, this output cannot be "
                    "provided to the task.".format(k)
                )
            if _type_helpers.get_sdk_type_from_literal_type(v.type) in type(self)._banned_outputs:
                raise _user_exceptions.FlyteValidationException(
                    "The output '{}' is not an accepted output type.".format(v)
                )

    def _get_kwarg_inputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        return set(_getargspec(self.task_function).args[1:])

    def _is_argname_in_function_definition(self, key):
        return key in self._get_kwarg_inputs()

    def _missing_mapped_inputs_outputs(self):
        # Trim off first parameter as it is reserved for workflow_parameters
        args = self._get_kwarg_inputs()
        inputs_and_outputs = set(self.interface.outputs.keys()) | set(self.interface.inputs.keys())
        return args ^ inputs_and_outputs
