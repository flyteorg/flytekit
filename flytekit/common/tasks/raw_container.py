from __future__ import absolute_import

import datetime as _datetime
from typing import Dict, List

from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit import __version__
from flytekit.common import constants as _constants
from flytekit.common import interface as _interface
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import task as _base_task
from flytekit.common.types.base_sdk_types import FlyteSdkType
from flytekit.configuration import resources as _resource_config
from flytekit.models import literals as _literals, task as _task_models, copilot as _copilot
from flytekit.models.interface import Variable


def types_to_variable(t: Dict[str, FlyteSdkType]) -> Dict[str, Variable]:
    var = {}
    if t:
        for k, v in t.items():
            var[k] = Variable(v.to_flyte_literal_type(), "")
    return var


def _get_container_definition(
        image: str,
        command: List[str],
        args: List[str],
        storage_request: str = None,
        cpu_request: str = None,
        gpu_request: str = None,
        memory_request: str = None,
        storage_limit: str = None,
        cpu_limit: str = None,
        gpu_limit: str = None,
        memory_limit: str = None,
        environment: Dict[str, str] = None,
) -> _task_models.Container:
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

    if environment is None:
        environment = {}

    return _task_models.Container(
        image=image,
        command=command,
        args=args,
        resources=_task_models.Resources(limits=limits, requests=requests),
        env=environment,
        config={}
    )


class SdkRawContainerTask(_base_task.SdkTask):
    """
    This class includes the logic for building a task that executes as a Presto task.
    """
    METADATA_FORMAT_JSON = _copilot.CoPilot.METADATA_FORMAT_JSON
    METADATA_FORMAT_YAML = _copilot.CoPilot.METADATA_FORMAT_YAML
    METADATA_FORMAT_PROTO = _copilot.CoPilot.METADATA_FORMAT_PROTO

    def __init__(
            self,
            inputs: Dict[str, FlyteSdkType],
            image: str,
            outputs: Dict[str, FlyteSdkType]=None,
            input_data_dir: str = None,
            output_data_dir: str = None,
            metadata_format: int = METADATA_FORMAT_JSON,
            command: List[str] = None,
            args: List[str] = None,
            storage_request: str = None,
            cpu_request: str = None,
            gpu_request: str = None,
            memory_request: str = None,
            storage_limit: str = None,
            cpu_limit: str = None,
            gpu_limit: str = None,
            memory_limit: str = None,
            environment: Dict[str, str] = None,
            interruptible: bool = False,
            discoverable: bool = False,
            discovery_version: str = None,
            retries: int = 1,
            timeout: _datetime.timedelta = None,
    ):
        """
        :param inputs:
        :param outputs:
        :param image:
        :param command:
        :param args:
        :param storage_request:
        :param cpu_request:
        :param gpu_request:
        :param memory_request:
        :param storage_limit:
        :param cpu_limit:
        :param gpu_limit:
        :param memory_limit:
        :param environment:
        :param interruptible:
        :param discoverable:
        :param discovery_version:
        :param retries:
        :param timeout:
        :param input_data_dir: This is the directory where data will be downloaded to
        :param output_data_dir: This is the directory where data will be uploaded from
        :param metadata_format: Format in which the metadata will be available for the script
        """

        # Set as class fields which are used down below to configure implicit
        # parameters
        self.input_data_dir = input_data_dir
        self.output_data_dir = output_data_dir
        self.metadata_format = metadata_format

        metadata = _task_models.TaskMetadata(
            discoverable,
            # This needs to have the proper version reflected in it
            _task_models.RuntimeMetadata(
                _task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK, __version__,
                "python"),
            timeout or _datetime.timedelta(seconds=0),
            _literals.RetryStrategy(retries),
            interruptible,
            discovery_version,
            "This is deprecated!"
        )

        # Here we set the routing_group, catalog, and schema as implicit
        # parameters for caching purposes
        i = _interface.TypedInterface(inputs=types_to_variable(inputs), outputs=types_to_variable(outputs))

        info = _copilot.CoPilot(input_path=input_data_dir, output_path=output_data_dir, metadata_format=metadata_format)

        super(SdkRawContainerTask, self).__init__(
            _constants.SdkTaskType.RAW_CONTAINER_TASK,
            metadata,
            i,
            _MessageToDict(info.to_flyte_idl()),
            container=_get_container_definition(
                image=image,
                args=args,
                command=command,
                storage_request=storage_request,
                cpu_request=cpu_request,
                gpu_request=gpu_request,
                memory_request=memory_request,
                storage_limit=storage_limit,
                cpu_limit=cpu_limit,
                gpu_limit=gpu_limit,
                memory_limit=memory_limit,
                environment=environment,
            )
        )

    # Override method in order to set the implicit inputs
    def __call__(self, *args, **kwargs):
        return super(SdkRawContainerTask, self).__call__(
            *args, **kwargs
        )

    @_exception_scopes.system_entry_point
    def add_inputs(self, inputs: Dict[str, Variable]):
        """
        Adds the inputs to this task.  This can be called multiple times, but it will fail if an input with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] inputs: names and variables
        """
        self._validate_inputs(inputs)
        self.interface.inputs.update(inputs)

    @_exception_scopes.system_entry_point
    def add_outputs(self, outputs: Dict[str, Variable]):
        """
        Adds the inputs to this task.  This can be called multiple times, but it will fail if an input with a given
        name is added more than once, a name collides with an output, or if the name doesn't exist as an arg name in
        the wrapped function.
        :param dict[Text, flytekit.models.interface.Variable] outputs: names and variables
        """
        self._validate_outputs(outputs)
        self.interface.outputs.update(outputs)
