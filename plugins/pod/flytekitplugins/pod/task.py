from typing import Any, Callable, Dict, Tuple, Union

from flyteidl.core import tasks_pb2 as _core_task
from kubernetes.client import ApiClient
from kubernetes.client.models import V1Container, V1EnvVar, V1PodSpec, V1ResourceRequirements

from flytekit import FlyteContext, PythonFunctionTask
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.extend import Promise, SerializationSettings, TaskPlugins

_PRIMARY_CONTAINER_NAME_FIELD = "primary_container_name"


class Pod(object):
    def __init__(self, pod_spec: V1PodSpec, primary_container_name: str):
        if not pod_spec:
            raise _user_exceptions.FlyteValidationException("A pod spec cannot be undefined")
        if not primary_container_name:
            raise _user_exceptions.FlyteValidationException("A primary container name cannot be undefined")

        self._pod_spec = pod_spec
        self._primary_container_name = primary_container_name

    @property
    def pod_spec(self) -> V1PodSpec:
        return self._pod_spec

    @property
    def primary_container_name(self) -> str:
        return self._primary_container_name


class PodFunctionTask(PythonFunctionTask[Pod]):
    def __init__(self, task_config: Pod, task_function: Callable, **kwargs):
        super(PodFunctionTask, self).__init__(
            task_config=task_config,
            task_type="sidecar",
            task_function=task_function,
            task_type_version=1,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        containers = self.task_config.pod_spec.containers
        primary_exists = False
        for container in containers:
            if container.name == self.task_config.primary_container_name:
                primary_exists = True
                break
        if not primary_exists:
            # insert a placeholder primary container if it is not defined in the pod spec.
            containers.append(V1Container(name=self.task_config.primary_container_name))

        final_containers = []
        for container in containers:
            # In the case of the primary container, we overwrite specific container attributes with the default values
            # used in an SDK runnable task.
            if container.name == self.task_config.primary_container_name:
                sdk_default_container = self.get_container(settings)

                container.image = sdk_default_container.image
                # clear existing commands
                container.command = sdk_default_container.command
                # also clear existing args
                container.args = sdk_default_container.args

                limits, requests = {}, {}
                for resource in sdk_default_container.resources.limits:
                    limits[_core_task.Resources.ResourceName.Name(resource.name).lower()] = resource.value
                for resource in sdk_default_container.resources.requests:
                    requests[_core_task.Resources.ResourceName.Name(resource.name).lower()] = resource.value

                resource_requirements = V1ResourceRequirements(limits=limits, requests=requests)
                if len(limits) > 0 or len(requests) > 0:
                    # Important! Only copy over resource requirements if they are non-empty.
                    container.resources = resource_requirements

                container.env = [V1EnvVar(name=key, value=val) for key, val in sdk_default_container.env.items()]

            final_containers.append(container)

        self.task_config._pod_spec.containers = final_containers

        return ApiClient().sanitize_for_serialization(self.task_config.pod_spec)

    def get_config(self, settings: SerializationSettings) -> Dict[str, str]:
        return {_PRIMARY_CONTAINER_NAME_FIELD: self.task_config.primary_container_name}

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, None]:
        raise _user_exceptions.FlyteUserException("Local execute is not currently supported for pod tasks")


TaskPlugins.register_pythontask_plugin(Pod, PodFunctionTask)
