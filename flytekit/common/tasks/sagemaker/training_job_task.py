from __future__ import absolute_import

from typing import Dict, Callable
import datetime as _datetime

from flytekit import __version__
from flytekit.common.tasks import task as _sdk_task, sdk_runnable as _sdk_runnable
from flytekit.sdk import types as _sdk_types
from flytekit.models import task as _task_models
from flytekit.models import interface as _interface_model
from flytekit.common import interface as _interface
from flytekit.models.sagemaker import training_job as _training_job_models
from flyteidl.plugins.sagemaker import training_job_pb2 as _training_job_pb2
from google.protobuf.json_format import MessageToDict
from flytekit.models import literals as _literals, types as _idl_types, \
    task as _task_model
from flytekit.models.core import types as _core_types
from flytekit.models import literals as _literal_models
from flytekit.common.constants import SdkTaskType


class SdkSimpleTrainingJobTask(_sdk_task.SdkTask):
    def __init__(
            self,
            training_job_config: _training_job_models.TrainingJobConfig,
            algorithm_specification: _training_job_models.AlgorithmSpecification,
            interruptible: bool = False,
            retries: int = 0,
            cacheable: bool = False,
            cache_version: str = "",
    ):
        """

        :param training_job_config:
        :param algorithm_specification:
        :param interruptible:
        :param retries:
        :param cacheable:
        :param cache_version:
        """
        # Use the training job model as a measure of type checking
        training_job = _training_job_models.TrainingJob(
            algorithm_specification=algorithm_specification,
            training_job_config=training_job_config,
        ).to_flyte_idl()

        # Setting flyte-level timeout to 0, and let SageMaker takes the StoppingCondition and terminate the training
        # job gracefully
        timeout = _datetime.timedelta(seconds=0)

        super(SdkSimpleTrainingJobTask, self).__init__(
            type=SdkTaskType.SAGEMAKER_TRAINING_JOB_TASK,
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
                        type=_idl_types.LiteralType(simple=_idl_types.SimpleType.STRUCT),
                        description="",
                    ),
                    "train": _interface_model.Variable(
                        type=_idl_types.LiteralType(
                            blob=_core_types.BlobType(
                                format="csv",
                                dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART
                            ),
                        ),
                        description="",
                    ),
                    "validation": _interface_model.Variable(
                        type=_idl_types.LiteralType(
                            blob=_core_types.BlobType(
                                format="csv",
                                dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART
                            ),
                        ),
                        description="",
                    ),
                    "stopping_condition": _interface_model.Variable(
                        _sdk_types.Types.Proto(_training_job_pb2.StoppingCondition).to_flyte_literal_type(), ""
                    )
                },
                outputs={
                    "model": _interface_model.Variable(
                        type=_idl_types.LiteralType(
                            blob=_core_types.BlobType(
                                format="",
                                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
                            )
                        ),
                        description=""
                    )
                }
            ),
            custom=MessageToDict(training_job),
        )


class SdkCustomTrainingJobTask(_sdk_runnable.SdkRunnableTask):
    def __init__(
            self,
            task_function: Callable,
            training_job_config: _training_job_models.TrainingJobConfig,
            algorithm_specification: _training_job_models.AlgorithmSpecification,
            cache_version: str,
            retries: int = 0,
            interruptible: bool = False,
            deprecated: bool = False,
            cacheable: bool = False,
            environment: Dict[str, str] = None,
    ):
        """

        :param task_function:

        :param training_job_config:
        :param algorithm_specification:
        :param cache_version:
        :param retries:
        :param interruptible:
        :param deprecated:
        :param cacheable:
        :param environment:
        """
        # Use the training job model as a measure of type checking
        training_job = _training_job_models.TrainingJob(
            algorithm_specification=algorithm_specification,
            training_job_config=training_job_config,
        ).to_flyte_idl()

        # Setting flyte-level timeout to 0, and let SageMaker takes the StoppingCondition and terminate the training
        # job gracefully
        timeout = _datetime.timedelta(seconds=0)

        super(SdkCustomTrainingJobTask, self).__init__(
            task_function=task_function,
            task_type=SdkTaskType.SAGEMAKER_TRAINING_JOB_TASK,
            discovery_version=cache_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request="",
            cpu_request="",
            gpu_request="",
            memory_request="",
            storage_limit="",
            cpu_limit="",
            gpu_limit="",
            memory_limit="",
            discoverable=cacheable,
            timeout=timeout,
            environment=environment,
            custom=MessageToDict(training_job),
        ),
        self.add_inputs(
            {
                "static_hyperparameters": _interface_model.Variable(
                    type=_idl_types.LiteralType(simple=_idl_types.SimpleType.STRUCT),
                    description="",
                ),
                "train": _interface_model.Variable(
                    type=_idl_types.LiteralType(
                        blob=_core_types.BlobType(
                            format="csv",
                            dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART
                        ),
                    ),
                    description="",
                ),
                "validation": _interface_model.Variable(
                    type=_idl_types.LiteralType(
                        blob=_core_types.BlobType(
                            format="csv",
                            dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART
                        ),
                    ),
                    description="",
                ),
                "stopping_condition": _interface_model.Variable(
                    _sdk_types.Types.Proto(_training_job_pb2.StoppingCondition).to_flyte_literal_type(), ""
                )
            },
        )
        self.add_outputs(
            {
                "model": _interface_model.Variable(
                    type=_idl_types.LiteralType(
                        blob=_core_types.BlobType(
                            format="",
                            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE
                        )
                    ),
                    description=""
                )
            }
        )
