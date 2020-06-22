from __future__ import absolute_import

from flytekit import __version__
from flytekit.common.tasks import task as _sdk_task
from flytekit.sdk import types as _sdk_types
from flytekit.models import task as _task_models
from flytekit.models import interface as _interface_model
from flytekit.common import interface as _interface
from flytekit.models.sagemaker import training_job as _training_job_models
from google.protobuf.json_format import MessageToDict
import datetime as _datetime
from flytekit.models import literals as _literal_models


class SdkSimpleTrainingJobTask(_sdk_task.SdkTask):
    def __init__(
            self,
            task_type,
            training_job_config,
            algorithm_specification,
            interruptible=False,
            retries=0,
            cacheable=False,
            cache_version="",
    ):

        # Use the training job model as a measure of type checking
        training_job = _training_job_models.TrainingJob(
            algorithm_specification=algorithm_specification,
            training_job_config=training_job_config,
        ).to_flyte_idl()

        # Setting flyte-level timeout to 0, and let SageMaker takes the StoppingCondition and terminate the training
        # job gracefully
        timeout = _datetime.timedelta(seconds=0)

        super(SdkSimpleTrainingJobTask, self).__init__(
            type=task_type,
            metadata=_task_models.TaskMetadata(
                runtime=_task_models.RuntimeMetadata(
                    type=_task_models.RuntimeMetadata.RuntimeType.FLYTE_SDK,
                    version=__version__,
                    flavor='sagemaker'
                ),
                discoverable=cacheable,
                timeout=timeout,
                retries=_literal_models.RetryStrategy(retries=retries),
                interruptible=interruptible,
                discovery_version=cache_version,
                deprecated_error_message="",
            ),
            interface=_interface.TypedInterface(
                inputs={
                    "static_hyperparameters": _interface_model.Variable(
                        _sdk_types.Types.Generic.to_flyte_literal_type(), ""
                    ),
                    "train": _interface_model.Variable(
                        _sdk_types.Types.MultiPartCSV.to_flyte_literal_type(), ""
                    ),
                    "validation": _interface_model.Variable(
                        _sdk_types.Types.MultiPartCSV.to_flyte_literal_type(), ""
                    )
                },
                outputs={
                    "model": _interface_model.Variable(
                        _sdk_types.Types.Blob.to_flyte_literal_type(), ""
                    )
                }
            ),
            custom=MessageToDict(training_job),
        )
