import datetime as _datetime
import typing

from google.protobuf.json_format import MessageToDict

from flytekit import __version__
from flytekit.common import interface as _interface
from flytekit.common.constants import SdkTaskType
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.tasks.sagemaker.built_in_training_job_task import SdkBuiltinAlgorithmTrainingJobTask
from flytekit.common.tasks.sagemaker.custom_training_job_task import CustomTrainingJobTask
from flytekit.common.tasks.sagemaker.types import HyperparameterTuningJobConfig, ParameterRange
from flytekit.models import interface as _interface_model
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models
from flytekit.models import types as _types_models
from flytekit.models.core import types as _core_types
from flytekit.models.sagemaker import hpo_job as _hpo_job_model


class SdkSimpleHyperparameterTuningJobTask(_sdk_task.SdkTask):
    def __init__(
        self,
        max_number_of_training_jobs: int,
        max_parallel_training_jobs: int,
        training_job: typing.Union[SdkBuiltinAlgorithmTrainingJobTask, CustomTrainingJobTask],
        retries: int = 0,
        cacheable: bool = False,
        cache_version: str = "",
        tunable_parameters: typing.List[str] = None,
    ):
        """
        :param int max_number_of_training_jobs: The maximum number of training jobs that can be launched by this
        hyperparameter tuning job
        :param int max_parallel_training_jobs: The maximum number of training jobs that can launched by this hyperparameter
        tuning job in parallel
        :param typing.Union[SdkBuiltinAlgorithmTrainingJobTask, CustomTrainingJobTask] training_job: The reference to the training job definition
        :param int retries: Number of retries to attempt
        :param bool cacheable: The flag to set if the user wants the output of the task execution to be cached
        :param str cache_version: String describing the caching version for task discovery purposes
        :param typing.List[str] tunable_parameters: A list of parameters that to tune. If you are tuning a built-int
                algorithm, refer to the algorithm's documentation to understand the possible values for the tunable
                parameters. E.g. Refer to https://docs.aws.amazon.com/sagemaker/latest/dg/IC-Hyperparameter.html for the
                list of hyperparameters for Image Classification built-in algorithm. If you are passing a custom
                training job, the list of tunable parameters must be a strict subset of the list of inputs defined on
                that job. Refer to https://docs.aws.amazon.com/sagemaker/latest/dg/automatic-model-tuning-define-ranges.html
                for the list of supported hyperparameter types.
        """
        # Use the training job model as a measure of type checking
        hpo_job = _hpo_job_model.HyperparameterTuningJob(
            max_number_of_training_jobs=max_number_of_training_jobs,
            max_parallel_training_jobs=max_parallel_training_jobs,
            training_job=training_job.training_job_model,
        ).to_flyte_idl()

        # Setting flyte-level timeout to 0, and let SageMaker respect the StoppingCondition of
        #   the underlying training job
        # TODO: Discuss whether this is a viable interface or contract
        timeout = _datetime.timedelta(seconds=0)

        inputs = {}
        inputs.update(training_job.interface.inputs)
        inputs.update(
            {
                "hyperparameter_tuning_job_config": _interface_model.Variable(
                    HyperparameterTuningJobConfig.to_flyte_literal_type(),
                    "",
                ),
            }
        )

        if tunable_parameters:
            inputs.update(
                {
                    param: _interface_model.Variable(ParameterRange.to_flyte_literal_type(), "")
                    for param in tunable_parameters
                }
            )

        super().__init__(
            type=SdkTaskType.SAGEMAKER_HYPERPARAMETER_TUNING_JOB_TASK,
            metadata=_task_models.TaskMetadata(
                runtime=_task_models.RuntimeMetadata(
                    type=_task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                    version=__version__,
                    flavor="sagemaker",
                ),
                discoverable=cacheable,
                timeout=timeout,
                retries=_literal_models.RetryStrategy(retries=retries),
                interruptible=False,
                discovery_version=cache_version,
                deprecated_error_message="",
            ),
            interface=_interface.TypedInterface(
                inputs=inputs,
                outputs={
                    "model": _interface_model.Variable(
                        type=_types_models.LiteralType(
                            blob=_core_types.BlobType(
                                format="",
                                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
                            )
                        ),
                        description="",
                    )
                },
            ),
            custom=MessageToDict(hpo_job),
        )

    def __call__(self, *args, **kwargs):
        # Overriding the call function just so we clear up the docs and avoid IDEs complaining about the signature.
        return super().__call__(*args, **kwargs)
