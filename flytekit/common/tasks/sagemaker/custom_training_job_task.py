from google.protobuf.json_format import MessageToDict

from flytekit.common.constants import SdkTaskType
from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.models.sagemaker import training_job as _training_job_models


class CustomTrainingJobTask(_sdk_runnable.SdkRunnableTask):
    """
    CustomTrainJobTask defines a python task that can run on SageMaker bring your own container.

    """

    def __init__(
        self,
        task_function,
        cache_version,
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
        cache,
        timeout,
        environment,
        algorithm_specification: _training_job_models.AlgorithmSpecification,
        training_job_resource_config: _training_job_models.TrainingJobResourceConfig,
    ):
        """
        :param task_function: Function container user code.  This will be executed via the SDK's engine.
        :param Text cache_version: string describing the version for task discovery purposes
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
        :param bool cache:
        :param datetime.timedelta timeout:
        :param dict[Text, Text] environment:
        :param _training_job_models.AlgorithmSpecification algorithm_specification:
        :param _training_job_models.TrainingJobResourceConfig training_job_resource_config:
        """

        # Use the training job model as a measure of type checking
        self._training_job_model = _training_job_models.TrainingJob(
            algorithm_specification=algorithm_specification, training_job_resource_config=training_job_resource_config
        )

        super().__init__(
            task_function=task_function,
            task_type=SdkTaskType.SAGEMAKER_CUSTOM_TRAINING_JOB_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=False,
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
            timeout=timeout,
            environment=environment,
            custom=MessageToDict(self._training_job_model.to_flyte_idl()),
        )

    @property
    def training_job_model(self) -> _training_job_models.TrainingJob:
        return self._training_job_model
