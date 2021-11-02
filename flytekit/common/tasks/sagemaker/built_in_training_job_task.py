import datetime as _datetime

from google.protobuf.json_format import MessageToDict

from flytekit import __version__
from flytekit.common import interface as _interface
from flytekit.common.constants import SdkTaskType
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.tasks import task as _sdk_task
from flytekit.models import interface as _interface_model
from flytekit.models import literals as _literal_models
from flytekit.models import task as _task_models
from flytekit.models import types as _idl_types
from flytekit.models.core import types as _core_types
from flytekit.models.sagemaker import training_job as _training_job_models


def _content_type_to_blob_format(content_type: _training_job_models) -> str:
    if content_type == _training_job_models.InputContentType.TEXT_CSV:
        return "csv"
    else:
        raise _user_exceptions.FlyteValueException("Unsupported InputContentType: {}".format(content_type))


class SdkBuiltinAlgorithmTrainingJobTask(_sdk_task.SdkTask):
    def __init__(
        self,
        training_job_resource_config: _training_job_models.TrainingJobResourceConfig,
        algorithm_specification: _training_job_models.AlgorithmSpecification,
        retries: int = 0,
        cacheable: bool = False,
        cache_version: str = "",
    ):
        """

        :param training_job_resource_config: The options to configure the training job
        :param algorithm_specification: The options to configure the target algorithm of the training
        :param retries: Number of retries to attempt
        :param cacheable: The flag to set if the user wants the output of the task execution to be cached
        :param cache_version: String describing the caching version for task discovery purposes
        """
        # Use the training job model as a measure of type checking
        self._training_job_model = _training_job_models.TrainingJob(
            algorithm_specification=algorithm_specification,
            training_job_resource_config=training_job_resource_config,
        )

        # Setting flyte-level timeout to 0, and let SageMaker takes the StoppingCondition and terminate the training
        # job gracefully
        timeout = _datetime.timedelta(seconds=0)

        super(SdkBuiltinAlgorithmTrainingJobTask, self).__init__(
            type=SdkTaskType.SAGEMAKER_TRAINING_JOB_TASK,
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
                inputs={
                    "static_hyperparameters": _interface_model.Variable(
                        type=_idl_types.LiteralType(simple=_idl_types.SimpleType.STRUCT),
                        description="",
                    ),
                    "train": _interface_model.Variable(
                        type=_idl_types.LiteralType(
                            blob=_core_types.BlobType(
                                format=_content_type_to_blob_format(algorithm_specification.input_content_type),
                                dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,
                            ),
                        ),
                        description="",
                    ),
                    "validation": _interface_model.Variable(
                        type=_idl_types.LiteralType(
                            blob=_core_types.BlobType(
                                format=_content_type_to_blob_format(algorithm_specification.input_content_type),
                                dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART,
                            ),
                        ),
                        description="",
                    ),
                },
                outputs={
                    "model": _interface_model.Variable(
                        type=_idl_types.LiteralType(
                            blob=_core_types.BlobType(
                                format="",
                                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
                            )
                        ),
                        description="",
                    )
                },
            ),
            custom=MessageToDict(self._training_job_model.to_flyte_idl()),
        )

    @property
    def training_job_model(self) -> _training_job_models.TrainingJob:
        return self._training_job_model
