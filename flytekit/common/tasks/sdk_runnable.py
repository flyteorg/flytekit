from __future__ import absolute_import

import abc as _abc
import importlib as _importlib
import six as _six

from flytekit import __version__
from flytekit.common import interface as _interface, constants as _constants, sdk_bases as _sdk_bases
from flytekit.common.core import identifier as _identifier
from flytekit.common.exceptions import user as _user_exceptions, system as _system_exceptions, \
    scopes as _exception_scopes
from flytekit.common.tasks import task as _base_task, output as _task_output
from flytekit.common.tasks.mixins.executable_traits import function as _function_mixin, notebook as _notebook_mixin
from flytekit.common.types import helpers as _type_helpers
from flytekit.configuration import sdk as _sdk_config, internal as _internal_config, resources as _resource_config
from flytekit.engines import loader as _engine_loader
from flytekit.models import literals as _literal_models, task as _task_models


class _ExecutionParameters(object):

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
        self,
        command=None,
        args=None,
        resources=None,
        env=None,
        config=None,
        image=None,
        runnable_task=None,
    ):
        super(SdkRunnableContainer, self).__init__(
            image or type(self)._get_default_image(),
            command or type(self)._get_default_command(),
            args or type(self)._get_default_args(),
            resources or type(self)._get_default_resources(),
            env or type(self)._get_default_env(),
            config or type(self)._get_default_config()
        )
        self._runnable_task = runnable_task

    @classmethod
    def _get_default_image(cls):
        """
        :rtype: Text
        """
        return ""

    @classmethod
    def _get_default_command(cls):
        """
        :rtype: list[Text]
        """
        return []

    @classmethod
    def _get_default_args(cls):
        """
        :rtype: list[Text]
        """
        return []

    @classmethod
    def _get_default_resources(cls):
        """
        :rtype: _task_models.Resources
        """
        return _task_models.Resources(limits=[], requests=[])

    @classmethod
    def _get_default_env(cls):
        """
        :rtype: dict[Text,Text]
        """
        return dict()

    @classmethod
    def _get_default_config(cls):
        """
        :rtype: dict[Text,Text]
        """
        return dict()

    @property
    def args(self):
        """
        :rtype: list[Text]
        """
        if not self._args:
            return _sdk_config.SDK_PYTHON_VENV.get() + [
                "pyflyte-execute",
                "--task-module",
                self._runnable_task.task_module,
                "--task-name",
                self._runnable_task.task_name,
                "--inputs",
                "{{.input}}",
                "--output-prefix",
                "{{.outputPrefix}}"
            ]
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
            task_type='',
            discovery_version='',
            retries=0,
            deprecated=False,
            storage_request=None,
            cpu_request=None,
            gpu_request=None,
            memory_request=None,
            storage_limit=None,
            cpu_limit=None,
            gpu_limit=None,
            memory_limit=None,
            discoverable=None,
            timeout=None,
            environment=None,
            custom=None,
            **kwargs
    ):
        """
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
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
        :param Text _instantiated_in:  Module name in which this object was instantiated. This will be automatically
            provided.
        :param Text _instantiated_in_file:  File in which this object was instantiated. This will be
            automatically provided.
        """
        self._discovered_attribute = None
        super(SdkRunnableTask, self).__init__(
            type=task_type,
            metadata=_task_models.TaskMetadata(
                discoverable,
                _task_models.RuntimeMetadata(
                    _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                    __version__,
                    'python'
                ),
                timeout,
                _literal_models.RetryStrategy(retries),
                discovery_version,
                deprecated
            ),
            interface=_interface.TypedInterface({}, {}),
            custom=custom,
            container=self._get_container_definition(
                storage_request=storage_request,
                cpu_request=cpu_request,
                gpu_request=gpu_request,
                memory_request=memory_request,
                storage_limit=storage_limit,
                cpu_limit=cpu_limit,
                gpu_limit=gpu_limit,
                memory_limit=memory_limit,
                environment=environment
            ),
            **kwargs
        )

    _banned_inputs = {}
    _banned_outputs = {}

    @classmethod
    def promote_from_model(cls, base_model):
        # TODO: If the task exists in this container, we should be able to retrieve it.
        raise _user_exceptions.FlyteAssertion("Cannot promote a base object to a runnable task.")

    @property
    def task_name(self):
        """
        :rtype: Text
        """
        # TODO: This is a current limitation of the SDK engine. It requires we be able to load a task definition from
        # TODO: a module-level attribute. Therefore, we need to know this information about the object. This also leads
        # TODO: this late-binding code which is confusing.
        if not self._discovered_attribute:
            try:
                m = _importlib.import_module(self.task_module)
            except:
                raise _system_exceptions.FlyteSystemAssertion(
                    "Cannot load module: {} for your task. This exception being raised indicates a bug which should "
                    "have been identified upstream."
                )
            for k in dir(m):
                if getattr(m, k) is self:
                    self._discovered_attribute = k
                    break
            if not self._discovered_attribute:
                raise _user_exceptions.FlyteAssertion(
                    "Your task defined in {} must be stored as a class-level attribute.".format(self.task_module)
                )
        return self._discovered_attribute

    @property
    def task_module(self):
        """
        :rtype: Text
        """
        return self._instantiated_in

    def validate(self):
        super(SdkRunnableTask, self).validate()
        missing_args = self._missing_mapped_inputs_outputs()
        if len(missing_args) > 0:
            raise _user_exceptions.FlyteAssertion(
                "The task {} is invalid because not all inputs and outputs in the "
                "task function definition were specified in @outputs and @inputs. "
                "We are missing definitions for {}.".format(
                    self,
                    missing_args
                )
            )

    @_exception_scopes.system_entry_point
    def unit_test(self, **input_map):
        """
        :param dict[Text, T] input_map: Python Std input from users.  We will cast these to the appropriate Flyte
            literals.
        :returns: Depends on the behavior of the specific task in the unit engine.
        """
        return _engine_loader.get_engine('unit').get_task(self).execute(
            _type_helpers.pack_python_std_map_to_literal_map(input_map, {
                k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                for k, v in _six.iteritems(self.interface.inputs)
            })
        )

    @_exception_scopes.system_entry_point
    def local_execute(self, **input_map):
        """
        :param dict[Text, T] input_map: Python Std input from users.  We will cast these to the appropriate Flyte
            literals.
        :rtype: dict[Text, T]
        :returns: The output produced by this task in Python standard format.
        """
        return _engine_loader.get_engine('local').get_task(self).execute(
            _type_helpers.pack_python_std_map_to_literal_map(input_map, {
                k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                for k, v in _six.iteritems(self.interface.inputs)
            })
        )

    # TODO: Docstrings
    # TODO: Ensure opportunity to add to output dictionary for all methods
    def _get_vargs(self, context):
        """
        :param context:
        :rtype: list[T]
        """
        return [
            _ExecutionParameters(
                execution_date=context.execution_date,
                # TODO: it might be better to consider passing the full struct
                execution_id=_six.text_type(
                    _identifier.WorkflowExecutionIdentifier.promote_from_model(context.execution_id)
                ),
                stats=context.stats,
                logging=context.logging,
                tmp_dir=context.working_directory
            )
        ]

    def _unpack_inputs(self, context, inputs):
        """
        :param context:
        :rtype: TODO
        """
        return _type_helpers.unpack_literal_map_to_sdk_python_std(
            inputs,
            {
                k: _type_helpers.get_sdk_type_from_literal_type(v.type)
                for k, v in _six.iteritems(self.interface.inputs)
            }
        )

    def _unpack_output_references(self, context):
        """
        :param context:
        :rtype: dict[Text,_task_output.OutputReference]
        """
        return {
            name: _task_output.OutputReference(_type_helpers.get_sdk_type_from_literal_type(variable.type))
            for name, variable in _six.iteritems(self.interface.outputs)
        }

    def _handle_user_returns(self, context, user_returned):
        """
        :param context:
        :param user_returned:
        """
        pass

    def _pack_output_references(self, context, outputs):
        """
        :param context:
        :param outputs:
        :rtype:
        """
        context.output_protos[_constants.OUTPUT_FILE_NAME] = _literal_models.LiteralMap(
            literals={k: v.sdk_value for k, v in _six.iteritems(outputs)}
        )
    # TODO: End docstrings

    @_abc.abstractmethod
    def _execute_user_code(self, *_, **__):
        """
        This method should be overridden via mixin deriving from
        ref:`flytekit.common.tasks.mixins.executable_traits.common.ExecutableTaskMixin`
        """
        pass

    @_exception_scopes.system_entry_point
    def execute(self, context, inputs):
        """
        It is not recommended to override this function unless implementing a mixin like in
        flytekit.common.tasks.mixins.executable_traits. This function might be modified by mixins to ensure behavior
        given the execution context. However, the general flow should adhere to the order laid out in this method.

        To modify behavior for a task extension that is being authored, override the methods called from this function.

        The goal of this function is to fill out the output_protos and raw_output_files dicts in the 'context' object.
        These files and protos will be written to the metadata path specified by the scheduler and can be read by
        service-side components.

        :param flytekit.engines.common.EngineContext context:
        :param flytekit.models.literals.LiteralMap inputs:
        :rtype: dict[Text,flytekit.models.common.FlyteIdlEntity]
        """
        inputs_dict = self._unpack_inputs(context, inputs)
        vargs = self._get_vargs(context)
        outputs_dict = self._unpack_output_references(context)
        user_returned = self._execute_user_code(context, vargs, inputs_dict, outputs_dict)
        self._handle_user_returns(context, user_returned)
        self._pack_output_references(context, outputs_dict)

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
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.STORAGE,
                    storage_request
                )
            )
        if cpu_request:
            requests.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.CPU,
                    cpu_request
                )
            )
        if gpu_request:
            requests.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.GPU,
                    gpu_request
                )
            )
        if memory_request:
            requests.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.MEMORY,
                    memory_request
                )
            )

        limits = []
        if storage_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.STORAGE,
                    storage_limit
                )
            )
        if cpu_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.CPU,
                    cpu_limit
                )
            )
        if gpu_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.GPU,
                    gpu_limit
                )
            )
        if memory_limit:
            limits.append(
                _task_models.Resources.ResourceEntry(
                    _task_models.Resources.ResourceName.MEMORY,
                    memory_limit
                )
            )

        # TODO: No circular reference
        return (cls or SdkRunnableContainer)(
            runnable_task=self,
            resources=_task_models.Resources(limits=limits, requests=requests),
            env=environment,
        )


class RunnablePythonFunctionTask(_function_mixin.WrappedFunctionTask, SdkRunnableTask):
    pass


class RunnableNotebookTask(_notebook_mixin.NotebookTask, SdkRunnableTask):
    pass
