from __future__ import absolute_import

import datetime as _datetime
from typing import Dict, List

from flytekit import __version__
from flytekit.common import constants as _constants
from flytekit.common import interface as _interface
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import task as _base_task
from flytekit.common.types.base_sdk_types import FlyteSdkType
from flytekit.configuration import resources as _resource_config
from flytekit.models import literals as _literals, task as _task_models
from flytekit.models.interface import Variable


def _get_container_definition(
        image,
        command,
        args,
        storage_request=None,
        cpu_request=None,
        gpu_request=None,
        memory_request=None,
        storage_limit=None,
        cpu_limit=None,
        gpu_limit=None,
        memory_limit=None,
        environment=None,
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

    def __init__(
            self,
            inputs: Dict[str, FlyteSdkType],
            outputs: Dict[str, FlyteSdkType],
            image: str,
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
        """

        # Set as class fields which are used down below to configure implicit
        # parameters
        self._command = command

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
        i = _interface.TypedInterface()

        super(SdkRawContainerTask, self).__init__(
            _constants.SdkTaskType.PRESTO_TASK,
            metadata,
            i,
            None,
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

        # Set user provided inputs
        self.add_inputs(inputs)
        self.add_outputs(outputs)

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
