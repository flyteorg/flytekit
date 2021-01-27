from typing import Any, Callable, Dict, Tuple, Union

from flyteidl.core import tasks_pb2 as _core_task
from google.protobuf.json_format import MessageToDict
from k8s.io.api.core.v1.generated_pb2 import Container, EnvVar, PodSpec, ResourceRequirements
from k8s.io.apimachinery.pkg.api.resource.generated_pb2 import Quantity

from flytekit.annotated.context_manager import FlyteContext, SerializationSettings
from flytekit.annotated.promise import Promise
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.task import TaskPlugins
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models import task as _task_models


class Pod(object):
    def __init__(self, pod_spec: PodSpec, primary_container_name: str):
        if not pod_spec:
            raise _user_exceptions.FlyteValidationException("A pod spec cannot be undefined")
        if not primary_container_name:
            raise _user_exceptions.FlyteValidationException("A primary container name cannot be undefined")

        self._pod_spec = pod_spec
        self._primary_container_name = primary_container_name

    @property
    def pod_spec(self) -> PodSpec:
        return self._pod_spec

    @property
    def primary_container_name(self) -> str:
        return self._primary_container_name


class PodFunctionTask(PythonFunctionTask[Pod]):
    def __init__(self, task_config: Pod, task_function: Callable, **kwargs):
        super(PodFunctionTask, self).__init__(
            task_config=task_config, task_type="sidecar", task_function=task_function, **kwargs,
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
            containers.extend([Container(name=self.task_config.primary_container_name)])

        final_containers = []
        for container in containers:
            # In the case of the primary container, we overwrite specific container attributes with the default values
            # used in an SDK runnable task.
            if container.name == self.task_config.primary_container_name:
                sdk_default_container = self.get_container(settings)

                container.image = sdk_default_container.image
                # clear existing commands
                del container.command[:]
                container.command.extend(sdk_default_container.command)
                # also clear existing args
                del container.args[:]
                container.args.extend(sdk_default_container.args)

                resource_requirements = ResourceRequirements()
                for resource in sdk_default_container.resources.limits:
                    resource_requirements.limits[
                        _core_task.Resources.ResourceName.Name(resource.name).lower()
                    ].CopyFrom(Quantity(string=resource.value))
                for resource in sdk_default_container.resources.requests:
                    resource_requirements.requests[
                        _core_task.Resources.ResourceName.Name(resource.name).lower()
                    ].CopyFrom(Quantity(string=resource.value))
                if resource_requirements.ByteSize():
                    # Important! Only copy over resource requirements if they are non-empty.
                    container.resources.CopyFrom(resource_requirements)

                del container.env[:]
                container.env.extend([EnvVar(name=key, value=val) for key, val in sdk_default_container.env.items()])

            final_containers.append(container)

        del self.task_config._pod_spec.containers[:]
        self.task_config._pod_spec.containers.extend(final_containers)

        pod_job_plugin = _task_models.SidecarJob(
            pod_spec=self.task_config.pod_spec, primary_container_name=self.task_config.primary_container_name,
        ).to_flyte_idl()
        return MessageToDict(pod_job_plugin)

    def _local_execute(self, ctx: FlyteContext, **kwargs) -> Union[Tuple[Promise], Promise, None]:
        raise _user_exceptions.FlyteUserException("Local execute is not currently supported for pod tasks")


TaskPlugins.register_pythontask_plugin(Pod, PodFunctionTask)
