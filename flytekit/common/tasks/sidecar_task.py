import six as _six
from flyteidl.core import tasks_pb2 as _core_task
from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import sdk_dynamic as _sdk_dynamic
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.models import task as _task_models
from flytekit.plugins import k8s as _lazy_k8s


class SdkSidecarTask(_sdk_runnable.SdkRunnableTask, metaclass=_sdk_bases.ExtendedSdkType):

    """
    This class includes the additional logic for building a task that executes as a Sidecar Job.

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
        pod_spec=None,
        primary_container_name=None,
    ):
        """
        :param _sdk_runnable.SdkRunnableTask sdk_runnable_task:
        :param generated_pb2.PodSpec pod_spec:
        :param Text primary_container_name:
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """
        if not pod_spec:
            raise _user_exceptions.FlyteValidationException("A pod spec cannot be undefined")
        if not primary_container_name:
            raise _user_exceptions.FlyteValidationException("A primary container name cannot be undefined")

        super(SdkSidecarTask, self).__init__(
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
            custom=None,
        )

        self.reconcile_partial_pod_spec_and_task(pod_spec, primary_container_name)

    def reconcile_partial_pod_spec_and_task(self, pod_spec, primary_container_name):
        """
        Assigns the custom field as a the reconciled primary container and pod spec defintion.
        :param _sdk_runnable.SdkRunnableTask sdk_runnable_task:
        :param generated_pb2.PodSpec pod_spec:
        :param Text primary_container_name:
        :rtype: SdkSidecarTask
        """

        # First, insert a placeholder primary container if it is not defined in the pod spec.
        containers = pod_spec.containers
        primary_exists = False
        for container in containers:
            if container.name == primary_container_name:
                primary_exists = True
                break
        if not primary_exists:
            containers.extend([_lazy_k8s.io.api.core.v1.generated_pb2.Container(name=primary_container_name)])

        final_containers = []
        for container in containers:
            # In the case of the primary container, we overwrite specific container attributes with the default values
            # used in an SDK runnable task.
            if container.name == primary_container_name:
                container.image = self._container.image
                # clear existing commands
                del container.command[:]
                container.command.extend(self._container.command)
                # also clear existing args
                del container.args[:]
                container.args.extend(self._container.args)

                resource_requirements = _lazy_k8s.io.api.core.v1.generated_pb2.ResourceRequirements()
                for resource in self._container.resources.limits:
                    resource_requirements.limits[
                        _core_task.Resources.ResourceName.Name(resource.name).lower()
                    ].CopyFrom(_lazy_k8s.io.apimachinery.pkg.api.resource.generated_pb2.Quantity(string=resource.value))
                for resource in self._container.resources.requests:
                    resource_requirements.requests[
                        _core_task.Resources.ResourceName.Name(resource.name).lower()
                    ].CopyFrom(_lazy_k8s.io.apimachinery.pkg.api.resource.generated_pb2.Quantity(string=resource.value))
                if resource_requirements.ByteSize():
                    # Important! Only copy over resource requirements if they are non-empty.
                    container.resources.CopyFrom(resource_requirements)

                del container.env[:]
                container.env.extend(
                    [
                        _lazy_k8s.io.api.core.v1.generated_pb2.EnvVar(name=key, value=val)
                        for key, val in _six.iteritems(self._container.env)
                    ]
                )

            final_containers.append(container)

        del pod_spec.containers[:]
        pod_spec.containers.extend(final_containers)

        sidecar_job_plugin = _task_models.SidecarJob(
            pod_spec=pod_spec, primary_container_name=primary_container_name,
        ).to_flyte_idl()

        self.assign_custom_and_return(_MessageToDict(sidecar_job_plugin))


class SdkDynamicSidecarTask(
    _sdk_dynamic.SdkDynamicTaskMixin, SdkSidecarTask, metaclass=_sdk_bases.ExtendedSdkType,
):

    """
    This class includes the additional logic for building a task that runs as
    a Sidecar Job and executes parent-child tasks.

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
        allowed_failure_ratio,
        max_concurrency,
        environment,
        pod_spec=None,
        primary_container_name=None,
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text task_type: string describing the task type
        :param Text discovery_version: string describing the version for task discovery purposes
        :param int retries: Number of retries to attempt
        :param bool interruptible: Whether or not task is interruptible
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
        :param float allowed_failure_ratio:
        :param int max_concurrency:
        :param dict[Text, Text] environment:
        :param generated_pb2.PodSpec pod_spec:
        :param Text primary_container_name:
        :raises: flytekit.common.exceptions.user.FlyteValidationException
        """

        SdkSidecarTask.__init__(
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
            pod_spec=pod_spec,
            primary_container_name=primary_container_name,
        )

        _sdk_dynamic.SdkDynamicTaskMixin.__init__(self, allowed_failure_ratio, max_concurrency)
