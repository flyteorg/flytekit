from flytekit.common import constants as _common_constants
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.contrib.sensors.base_sensor import Sensor as _Sensor


class SensorTask(_sdk_runnable.SdkRunnableTask):
    def _execute_user_code(self, context, inputs):
        sensor = super(SensorTask, self)._execute_user_code(context=context, inputs=inputs)
        if sensor is not None:
            if not isinstance(sensor, _Sensor):
                raise _user_exceptions.FlyteTypeException(
                    received_type=type(sensor),
                    expected_type=_Sensor,
                )
            succeeded = sensor.sense()
            if not succeeded:
                raise _user_exceptions.FlyteRecoverableException()


def sensor_task(
    _task_function=None,
    retries=0,
    interruptible=None,
    deprecated="",
    storage_request=None,
    cpu_request=None,
    gpu_request=None,
    memory_request=None,
    storage_limit=None,
    cpu_limit=None,
    gpu_limit=None,
    memory_limit=None,
    timeout=None,
    environment=None,
    cls=None,
):
    """
    Decorator to create a Sensor Task definition.  This task will run as a single unit of work on the platform.

    .. code-block:: python
        @sensor_task(retries=3)
        def my_task(wf_params):
            return HiveTableSensor(
                schema='default',
                table_name='mocked_table',
                host='localhost',
                port=1234,
            )

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs.  No keyword
        arguments are allowed for wrapped task functions.
    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.
        .. note::
            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.
    :param bool interruptible: Specify whether task is interruptible
    :param Text deprecated: [optional] string that should be provided if this task is deprecated.  The string
        will be logged as a warning so it should contain information regarding how to update to a newer task.
    :param Text storage_request: [optional] Kubernetes resource string for lower-bound of disk storage space
        for the task to run.  Default is set by platform-level configuration.
        .. note::
            This is currently not supported by the platform.
    :param Text cpu_request: [optional] Kubernetes resource string for lower-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text gpu_request: [optional] Kubernetes resource string for lower-bound of desired GPUs.
        Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text memory_request: [optional]  Kubernetes resource string for lower-bound of physical memory
        necessary for the task to execute.  Default is set by platform-level configuration.
        TODO: Add links to resource string documentation for Kubernetes
    :param Text storage_limit: [optional] Kubernetes resource string for upper-bound of disk storage space
        for the task to run.  This amount is not guaranteed!  If not specified, it is set equal to storage_request.
        .. note::
            This is currently not supported by the platform.
    :param Text cpu_limit: [optional] Kubernetes resource string for upper-bound of cores for the task to execute.
        This can be set to a fractional portion of a CPU. This amount is not guaranteed!  If not specified,
        it is set equal to cpu_request.
    :param Text gpu_limit: [optional] Kubernetes resource string for upper-bound of desired GPUs. This amount is not
        guaranteed!  If not specified, it is set equal to gpu_request.
    :param Text memory_limit: [optional]  Kubernetes resource string for upper-bound of physical memory
        necessary for the task to execute.  This amount is not guaranteed!  If not specified, it is set equal to
        memory_request.
    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.
    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.
    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  A user can use this to
        inject bespoke logic into the base Flyte programming model. Ideally, should be a sub-class of SensorTask or
        otherwise mimic the behavior.
    :rtype: SensorTask
    """

    def wrapper(fn):
        return (SensorTask or cls)(
            task_function=fn,
            task_type=_common_constants.SdkTaskType.SENSOR_TASK,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=storage_request,
            cpu_request=cpu_request,
            gpu_request=gpu_request,
            memory_request=memory_request,
            storage_limit=storage_limit,
            cpu_limit=cpu_limit,
            gpu_limit=gpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            environment=environment,
            custom={},
            discovery_version="",
            discoverable=False,
            cache_serializable=False,
        )

    # This is syntactic-sugar, so that when calling this decorator without args, you can either
    # do it with () or without any ()
    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper
