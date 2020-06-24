from __future__ import absolute_import
import datetime as _datetime

from google.protobuf.json_format import MessageToDict

from flytekit import __version__
from flytekit.common.tasks import task as _sdk_task
from flytekit.common import interface as _interface
from flytekit.sdk import types as _sdk_types
from flytekit.models import task as _task_models
from flytekit.models import interface as _interface_model
from flytekit.models.sagemaker import hpo_job as _hpo_job_model
from flytekit.models import literals as _literal_models


class SdkSimpleHPOJobTask(_sdk_task.SdkTask):

    def __init__(
            self,
            task_type: str,
            max_number_of_training_jobs: int,
            max_parallel_training_jobs: int,
            training_job: _sdk_task.SdkTask,
            interruptible: bool = False,
            retries: int = 0,
            cacheable: bool = False,
            cache_version: str = "",
    ):
        """

        :param task_type:
        :param max_number_of_training_jobs:
        :param max_parallel_training_jobs:
        :param training_job:
        :param interruptible:
        :param retries:
        :param cacheable:
        :param cache_version:
        """
        # Use the training job model as a measure of type checking
        hpo_job = _hpo_job_model.HPOJob(
            max_number_of_training_jobs=max_number_of_training_jobs,
            max_parallel_training_jobs=max_parallel_training_jobs,
            training_job=training_job,
        ).to_flyte_idl()

        # Setting flyte-level timeout to 0, and let SageMaker respect the StoppingCondition of
        #   the underlying training job
        # TODO: Discuss whether this is a viable interface or contract
        timeout = _datetime.timedelta(seconds=0)

        super(SdkSimpleHPOJobTask, self).__init__(
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
                    "hpo_job_config": _interface_model.Variable(
                        _sdk_types.Types.Proto(_hpo_job_model.HPOJobConfig).to_flyte_literal_type(), ""
                    ),
                },
                outputs={
                    "model": _interface_model.Variable(
                        _sdk_types.Types.Blob.to_flyte_literal_type(), ""
                    )
                }
            ),
            custom=MessageToDict(hpo_job),
        )

        self.add_inputs(training_job.interface.inputs)
