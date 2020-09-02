import datetime as _datetime
import typing

from flyteidl.plugins.sagemaker import hyperparameter_tuning_job_pb2 as _pb2_hpo_job
from google.protobuf.json_format import MessageToDict

from flytekit import __version__
from flytekit.common import interface as _interface
from flytekit.common.constants import SdkTaskType
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.tasks.sagemaker.built_in_training_job_task import SdkBuiltinAlgorithmTrainingJobTask
from flytekit.common.tasks.sagemaker.custom_training_job_task import CustomTrainingJobTask
from flytekit.models import interface as _interface_model
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models
from flytekit.models import types as _types_models
from flytekit.models.core import types as _core_types
from flytekit.models.sagemaker import hpo_job as _hpo_job_model
from flytekit.sdk import types as _sdk_types


class SdkSimpleHyperparameterTuningJobTask(_sdk_task.SdkTask):
    def __init__(
        self,
        max_number_of_training_jobs: int,
        max_parallel_training_jobs: int,
        training_job: typing.Union[SdkBuiltinAlgorithmTrainingJobTask, CustomTrainingJobTask],
        retries: int = 0,
        cacheable: bool = False,
        cache_version: str = "",
    ):
        """

        :param max_number_of_training_jobs: The maximum number of training jobs that can be launched by this
        hyperparameter tuning job
        :param max_parallel_training_jobs: The maximum number of training jobs that can launched by this hyperparameter
        tuning job in parallel
        :param training_job: The reference to the training job definition
        :param retries: Number of retries to attempt
        :param cacheable: The flag to set if the user wants the output of the task execution to be cached
        :param cache_version: String describing the caching version for task discovery purposes
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

        inputs = {
            "hyperparameter_tuning_job_config": _interface_model.Variable(
                _sdk_types.Types.Proto(_pb2_hpo_job.HyperparameterTuningJobConfig).to_flyte_literal_type(), "",
            ),
        }
        inputs.update(training_job.interface.inputs)

        super(SdkSimpleHyperparameterTuningJobTask, self).__init__(
            type=SdkTaskType.SAGEMAKER_HYPERPARAMETER_TUNING_JOB_TASK,
            metadata=_task_models.TaskMetadata(
                runtime=_task_models.RuntimeMetadata(
                    type=_task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK, version=__version__, flavor="sagemaker",
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
                                format="", dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
                            )
                        ),
                        description="",
                    )
                },
            ),
            custom=MessageToDict(hpo_job),
        )
