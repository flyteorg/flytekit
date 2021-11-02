import datetime as _datetime
import importlib as _importlib
import inspect as _inspect
import json as _json
import os as _os
import sys as _sys

import six as _six
from google.protobuf import json_format as _json_format
from google.protobuf import text_format as _text_format

from flytekit import __version__
from flytekit.bin import entrypoint as _entrypoint
from flytekit.common import constants as _constants
from flytekit.common import interface as _interface2
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import output as _task_output
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.common.tasks import spark_task as _spark_task
from flytekit.common.tasks import task as _base_tasks
from flytekit.common.types import helpers as _type_helpers
from flytekit.contrib.notebook.supported_types import notebook_types_map as _notebook_types_map
from flytekit.engines import loader as _engine_loader
from flytekit.models import interface as _interface
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models
from flytekit.plugins import papermill as _pm
from flytekit.sdk.spark_types import SparkType as _spark_type
from flytekit.sdk.types import Types as _Types

OUTPUT_NOTEBOOK = "output_notebook"


def python_notebook(
    notebook_path="",
    inputs={},
    outputs={},
    cache_version="",
    retries=0,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    cache=False,
    timeout=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a Python Notebook Task definition.

    :rtype: SdkNotebookTask
    """
    return SdkNotebookTask(
        notebook_path=notebook_path,
        inputs=inputs,
        outputs=outputs,
        task_type=_constants.SdkTaskType.PYTHON_TASK,
        discovery_version=cache_version,
        retries=retries,
        deprecated=deprecated,
        storage_request=storage_request,
        cpu_request=cpu_request,
        gpu_request=gpu_request,
        memory_request=memory_request,
        storage_limit=storage_limit,
        cpu_limit=cpu_limit,
        gpu_limit=gpu_limit,
        memory_limit=memory_limit,
        discoverable=cache,
        timeout=timeout or _datetime.timedelta(seconds=0),
        environment=environment,
        custom={},
    )


class SdkNotebookTask(_base_tasks.SdkTask):

    """
    This class includes the additional logic for building a task that executes Notebooks.

    """

    def __init__(
        self,
        notebook_path,
        inputs,
        outputs,
        task_type,
        discovery_version,
        retries,
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

        if _os.path.isabs(notebook_path) is False:
            # Find absolute path for the notebook.
            task_module = _importlib.import_module(_find_instance_module())
            module_path = _os.path.dirname(task_module.__file__)
            notebook_path = _os.path.normpath(_os.path.join(module_path, notebook_path))

        self._notebook_path = notebook_path

        super(SdkNotebookTask, self).__init__(
            task_type,
            _task_models.TaskMetadata(
                discoverable,
                _task_models.RuntimeMetadata(
                    _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                    __version__,
                    "notebook",
                ),
                timeout,
                _literal_models.RetryStrategy(retries),
                False,
                discovery_version,
                deprecated,
            ),
            _interface2.TypedInterface({}, {}),
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
        # Add Inputs
        if inputs is not None:
            inputs(self)

        # Add outputs
        if outputs is not None:
            outputs(self)

        # Add a Notebook output as a Blob.
        self.interface.outputs.update(
            output_notebook=_interface.Variable(_Types.Blob.to_flyte_literal_type(), OUTPUT_NOTEBOOK)
        )

    def _validate_inputs(self, inputs):
        """
        :param dict[Text, flytekit.models.interface.Variable] inputs:  Input variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        for k, v in _six.iteritems(inputs):
            sdk_type = _type_helpers.get_sdk_type_from_literal_type(v.type)
            if sdk_type not in _notebook_types_map.values():
                raise _user_exceptions.FlyteValidationException(
                    "Input Type '{}' not supported.  Only Primitives are supported for notebook.".format(sdk_type)
                )
        super(SdkNotebookTask, self)._validate_inputs(inputs)

    def _validate_outputs(self, outputs):
        """
        :param dict[Text, flytekit.models.interface.Variable] inputs:  Input variables to validate
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """

        # Add output_notebook as an implicit output to the task.

        for k, v in _six.iteritems(outputs):

            if k == OUTPUT_NOTEBOOK:
                raise ValueError(
                    "{} is a reserved output keyword. Please use a different output name.".format(OUTPUT_NOTEBOOK)
                )

            sdk_type = _type_helpers.get_sdk_type_from_literal_type(v.type)
            if sdk_type not in _notebook_types_map.values():
                raise _user_exceptions.FlyteValidationException(
                    "Output Type '{}' not supported.  Only Primitives are supported for notebook.".format(sdk_type)
                )
        super(SdkNotebookTask, self)._validate_outputs(outputs)

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

    @_exception_scopes.system_entry_point
    def add_outputs(self, outputs):
        """
        Adds the outputs to this task.  This can be called multiple times, but it will fail if an output with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] outputs: names and variables
        """
        self._validate_outputs(outputs)
        self.interface.outputs.update(outputs)

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

        input_notebook_path = self._notebook_path
        # Execute Notebook via Papermill.
        output_notebook_path = input_notebook_path.split(".ipynb")[0] + "-out.ipynb"
        _pm.execute_notebook(input_notebook_path, output_notebook_path, parameters=inputs_dict)

        # Parse Outputs from Notebook.
        outputs = None
        with open(output_notebook_path) as json_file:
            data = _json.load(json_file)
            for p in data["cells"]:
                meta = p["metadata"]
                if "outputs" in meta["tags"]:
                    outputs = " ".join(p["outputs"][0]["data"]["text/plain"])

        if outputs is not None:
            dict = _literal_models._literals_pb2.LiteralMap()
            _text_format.Parse(outputs, dict)

        # Add output_notebook as an output to the task.
        output_notebook = _task_output.OutputReference(
            _type_helpers.get_sdk_type_from_literal_type(_Types.Blob.to_flyte_literal_type())
        )
        output_notebook.set(output_notebook_path)

        output_literal_map = _literal_models.LiteralMap.from_flyte_idl(dict)
        output_literal_map.literals[OUTPUT_NOTEBOOK] = output_notebook.sdk_value

        return {_constants.OUTPUT_FILE_NAME: output_literal_map}

    @property
    def container(self):
        """
        If not None, the target of execution should be a container.
        :rtype: Container
        """

        # Find task_name
        task_module = _importlib.import_module(self.instantiated_in)
        for k in dir(task_module):
            if getattr(task_module, k) is self:
                task_name = k
                break

        self._container._args = [
            "pyflyte-execute",
            "--task-module",
            self.instantiated_in,
            "--task-name",
            task_name,
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
        ]
        return self._container

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
        **kwargs
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
        :param dict[Text, Text] environment:
        :rtype: flytekit.models.task.Container
        """

        storage_limit = storage_limit or storage_request
        cpu_limit = cpu_limit or cpu_request
        gpu_limit = gpu_limit or gpu_request
        memory_limit = memory_limit or memory_request

        resources = _sdk_runnable.SdkRunnableContainer.get_resources(
            storage_request, cpu_request, gpu_request, memory_request, storage_limit, cpu_limit, gpu_limit, memory_limit
        )

        return _sdk_runnable.SdkRunnableContainer(
            command=[],
            args=[],
            resources=resources,
            env=environment,
            config={},
        )


def spark_notebook(
    notebook_path,
    inputs={},
    outputs={},
    spark_conf=None,
    cache_version="",
    retries=0,
    deprecated="",
    cache=False,
    timeout=None,
    environment=None,
):
    """
    Decorator to create a Notebook spark task. This task will connect to a Spark cluster, configure the environment,
    and then execute the code within the notebook_path as the Spark driver program.
    """
    return SdkNotebookSparkTask(
        notebook_path=notebook_path,
        inputs=inputs,
        outputs=outputs,
        spark_conf=spark_conf,
        discovery_version=cache_version,
        retries=retries,
        deprecated=deprecated,
        discoverable=cache,
        timeout=timeout or _datetime.timedelta(seconds=0),
        environment=environment or {},
    )


def _find_instance_module():
    frame = _inspect.currentframe()
    while frame:
        if frame.f_code.co_name == "<module>":
            return frame.f_globals["__name__"]
        frame = frame.f_back
    return None


class SdkNotebookSparkTask(SdkNotebookTask):

    """
    This class includes the additional logic for building a task that executes Spark Notebooks.

    """

    def __init__(
        self,
        notebook_path,
        inputs,
        outputs,
        spark_conf,
        discovery_version,
        retries,
        deprecated,
        discoverable,
        timeout,
        environment=None,
    ):

        spark_exec_path = _os.path.abspath(_entrypoint.__file__)
        if spark_exec_path.endswith(".pyc"):
            spark_exec_path = spark_exec_path[:-1]

        if spark_conf is None:
            # Parse spark_conf from notebook if not set at task_level.
            with open(notebook_path) as json_file:
                data = _json.load(json_file)
                for p in data["cells"]:
                    meta = p["metadata"]
                    if "tags" in meta:
                        if "conf" in meta["tags"]:
                            sc_str = " ".join(p["source"])
                            ldict = {}
                            exec(sc_str, globals(), ldict)
                            spark_conf = ldict["spark_conf"]

            spark_job = _task_models.SparkJob(
                spark_conf=spark_conf,
                main_class="",
                spark_type=_spark_type.PYTHON,
                hadoop_conf={},
                application_file="local://" + spark_exec_path,
                executor_path=_sys.executable,
            ).to_flyte_idl()

        super(SdkNotebookSparkTask, self).__init__(
            notebook_path,
            inputs,
            outputs,
            _constants.SdkTaskType.SPARK_TASK,
            discovery_version,
            retries,
            deprecated,
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            discoverable,
            timeout,
            environment,
            _json_format.MessageToDict(spark_job),
        )

    def _get_container_definition(self, environment=None, **kwargs):
        """
        :rtype: flytekit.models.task.Container
        """

        return _spark_task.SdkRunnableSparkContainer(
            command=[],
            args=[],
            resources=_task_models.Resources(limits=[], requests=[]),
            env=environment or {},
            config={},
        )
