from __future__ import absolute_import

import datetime as _datetime
from flytekit.sdk import types as _sdk_types
from flytekit.common.tasks import task as _sdk_task
from flytekit.common import interface as _interface
from flytekit.models import (
    literals as _literal_models,
    task as _task_models,
    interface as _interface_model,
)
from flytekit.models.sagemaker import (
    training_job as _training_job_models,
)
from google.protobuf.json_format import MessageToDict, ParseDict
import gen.pb_python.sagemaker_pb2 as sagemaker_pb2
from flytekit.common.exceptions import scopes as _exception_scopes

_TASK_TYPE = "sagemaker_training"

class SdkTrainingJobTask(_sdk_task.SdkTask):
    def __init__(
            self,
            algorithm_specification,
            training_job_config,
    ):
        """

        :param algorithm_specification:
        :param training_job_config:
        """




