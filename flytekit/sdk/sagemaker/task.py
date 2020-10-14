import datetime as _datetime
import typing

from flytekit.common.tasks.sagemaker.custom_training_job_task import CustomTrainingJobTask
from flytekit.common.tasks.sagemaker.distributed_training import DefaultOutputPersistPredicate
from flytekit.models.sagemaker import training_job as _training_job_models


def custom_training_job_task(
    _task_function=None,
    algorithm_specification: _training_job_models.AlgorithmSpecification = None,
    training_job_resource_config: _training_job_models.TrainingJobResourceConfig = None,
    cache_version: str = "",
    retries: int = 0,
    deprecated: str = "",
    storage_request: str = None,
    cpu_request: str = None,
    gpu_request: str = None,
    memory_request: str = None,
    storage_limit: str = None,
    cpu_limit: str = None,
    gpu_limit: str = None,
    memory_limit: str = None,
    cache: bool = False,
    timeout: _datetime.timedelta = None,
    environment: typing.Dict[str, str] = None,
    cls: typing.Type = None,
    output_persist_predicate: typing.Callable = DefaultOutputPersistPredicate(),
):
    """
    Decorator to create a Custom Training Job definition.  This task will run as a single unit of work on the platform.

    .. code-block:: python

        @inputs(int_list=[Types.Integer])
        @outputs(sum_of_list=Types.Integer
        @custom_task
        def my_task(wf_params, int_list, sum_of_list):
            sum_of_list.set(sum(int_list))

    :param _task_function: this is the decorated method and shouldn't be declared explicitly.  The function must
        take a first argument, and then named arguments matching those defined in @inputs and @outputs.  No keyword
        arguments are allowed for wrapped task functions.

    :param _training_job_models.AlgorithmSpecification algorithm_specification: This represents the algorithm specification

    :param _training_job_models.TrainingJobResourceConfig training_job_resource_config: This represents the training job config.

    :param Text cache_version: [optional] string representing logical version for discovery.  This field should be
        updated whenever the underlying algorithm changes.

        .. note::

            This argument is required to be a non-empty string if `cache` is True.

    :param int retries: [optional] integer determining number of times task can be retried on
        :py:exc:`flytekit.sdk.exceptions.RecoverableException` or transient platform failures.  Defaults
        to 0.

        .. note::

            If retries > 0, the task must be able to recover from any remote state created within the user code.  It is
            strongly recommended that tasks are written to be idempotent.

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

    :param bool cache: [optional] boolean describing if the outputs of this task should be cached and
        re-usable.

    :param datetime.timedelta timeout: [optional] describes how long the task should be allowed to
        run at max before triggering a retry (if retries are enabled).  By default, tasks are allowed to run
        indefinitely.  If a null timedelta is passed (i.e. timedelta(seconds=0)), the task will not timeout.

    :param dict[Text,Text] environment: [optional] environment variables to set when executing this task.

    :param cls: This can be used to override the task implementation with a user-defined extension. The class
        provided must be a subclass of flytekit.common.tasks.sdk_runnable.SdkRunnableTask.  A user can use this to
        inject bespoke logic into the base Flyte programming model.

    :param Callable output_persist_predicate: [optional] This callable should return a boolean and is used to indicate whether
        the current copy (i.e., an instance of the task running on a particular node inside the worker pool) would
        write output.

    :rtype: flytekit.common.tasks.sagemaker.custom_training_job_task.CustomTrainingJobTask
    """

    def wrapper(fn):
        return (cls or CustomTrainingJobTask)(
            task_function=fn,
            cache_version=cache_version,
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
            cache=cache,
            timeout=timeout or _datetime.timedelta(seconds=0),
            environment=environment,
            algorithm_specification=algorithm_specification,
            training_job_resource_config=training_job_resource_config,
            output_persist_predicate=output_persist_predicate,
        )

    if _task_function:
        return wrapper(_task_function)
    else:
        return wrapper
