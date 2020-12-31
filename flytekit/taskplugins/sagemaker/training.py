import logging
import typing
from dataclasses import dataclass
from typing import Any, Callable, Dict, TypeVar

from google.protobuf.json_format import MessageToDict

import flytekit
from flytekit.annotated.base_task import PythonTask, kwtypes
from flytekit.annotated.context_manager import RegistrationSettings
from flytekit.annotated.interface import Interface
from flytekit.annotated.python_function_task import PythonFunctionTask
from flytekit.annotated.task import TaskPlugins
from flytekit.common.tasks.sdk_runnable import ExecutionParameters
from flytekit.models.sagemaker import training_job as _training_job_models
from flytekit.taskplugins.sagemaker.distributed_training import DistributedTrainingContext
from flytekit.types.directory.types import FlyteDirectory
from flytekit.types.file import FlyteFile


@dataclass
class SagemakerTrainingJobConfig(object):
    """
    Configuration for Running Training Jobs on Sagemaker. This config can be used to run either the built-in algorithms
    or custom algorithms.

    Args:
        training_job_resource_config: Configuration for Resources to use during the training
        algorithm_specification: Specification of the algorithm to use
        should_persist_output: This method will be invoked and will decide if the generated model should be persisted
                               as the output. ``NOTE: Useful only for distributed training``
                               ``default: single node training - always persist output``
                               ``default: distributed training - always persist output on node with rank-0``
    """

    training_job_resource_config: _training_job_models.TrainingJobResourceConfig
    algorithm_specification: _training_job_models.AlgorithmSpecification
    # The default output-persisting predicate.
    # With this predicate, only the copy running on the first host in the list of hosts would persist its output
    should_persist_output: typing.Callable[[DistributedTrainingContext], bool] = lambda dctx: (
        dctx.current_host == dctx.hosts[0]
    )


class SagemakerBuiltinAlgorithmsTask(PythonTask[SagemakerTrainingJobConfig]):
    """
    Implements an interface that allows execution of a SagemakerBuiltinAlgorithms.
    Refer to `Sagemaker Builtin Algorithms<https://docs.aws.amazon.com/sagemaker/latest/dg/algos.html>`_ for details.
    """

    _SAGEMAKER_TRAINING_JOB_TASK = "sagemaker_training_job_task"

    OUTPUT_TYPE = TypeVar("tar.gz")

    def __init__(
        self, name: str, task_config: SagemakerTrainingJobConfig, **kwargs,
    ):
        """
        Args:
            name: name of this specific task. This should be unique within the project. A good strategy is to prefix
                  with the module name
            metadata: Metadata for the task
            task_config: Config to use for the SagemakerBuiltinAlgorithms
        """
        if (
            task_config is None
            or task_config.algorithm_specification is None
            or task_config.training_job_resource_config is None
        ):
            raise ValueError("TaskConfig, algorithm_specification, training_job_resource_config are required")

        input_type = TypeVar(self._content_type_to_blob_format(task_config.algorithm_specification.input_content_type))

        interface = Interface(
            # TODO change train and validation to be FlyteDirectory when available
            inputs=kwtypes(
                static_hyperparameters=dict, train=FlyteDirectory[input_type], validation=FlyteDirectory[input_type]
            ),
            outputs=kwtypes(model=FlyteFile[self.OUTPUT_TYPE]),
        )
        super().__init__(
            self._SAGEMAKER_TRAINING_JOB_TASK, name, interface=interface, task_config=task_config, **kwargs,
        )

    def get_custom(self, settings: RegistrationSettings) -> Dict[str, Any]:
        training_job = _training_job_models.TrainingJob(
            algorithm_specification=self._task_config.algorithm_specification,
            training_job_resource_config=self._task_config.training_job_resource_config,
        )
        return MessageToDict(training_job.to_flyte_idl())

    def execute(self, **kwargs) -> Any:
        raise NotImplementedError(
            "Cannot execute Sagemaker Builtin Algorithms locally, for local testing, please mock!"
        )

    @classmethod
    def _content_type_to_blob_format(cls, content_type: int) -> str:
        """
        TODO Convert InputContentType to Enum and others
        """
        if content_type == _training_job_models.InputContentType.TEXT_CSV:
            return "csv"
        else:
            raise ValueError("Unsupported InputContentType: {}".format(content_type))


class SagemakerCustomTrainingTask(PythonFunctionTask[SagemakerTrainingJobConfig]):
    """
    Allows a custom training algorithm to be executed on Amazon Sagemaker. For this to work, make sure your container
    is built according to Flyte plugin documentation (TODO point the link here)
    """

    _SAGEMAKER_CUSTOM_TRAINING_JOB_TASK = "sagemaker_custom_training_job_task"

    def __init__(
        self, task_config: SagemakerTrainingJobConfig, task_function: Callable, **kwargs,
    ):
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            task_type=self._SAGEMAKER_CUSTOM_TRAINING_JOB_TASK,
            **kwargs,
        )

    def get_custom(self, settings: RegistrationSettings) -> Dict[str, Any]:
        training_job = _training_job_models.TrainingJob(
            algorithm_specification=self.task_config.algorithm_specification,
            training_job_resource_config=self.task_config.training_job_resource_config,
        )
        return MessageToDict(training_job.to_flyte_idl())

    def _is_distributed(self) -> bool:
        return (
            self.task_config.training_job_resource_config
            and self.task_config.training_job_resource_config.instance_count > 1
        )

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        """
        Pre-execute for Sagemaker will automatically add the distributed context to the execution params, only
        if the number of execution instances is > 1. Otherwise this is considered to be a single node execution
        """
        if self._is_distributed():
            logging.info("Distributed context detected!")
            return (
                user_params.builder()
                .add_attr("DISTRIBUTED_TRAINING_CONTEXT", DistributedTrainingContext.from_env())
                .build()
            )
        return user_params

    def execute(self, **kwargs) -> Any:
        """
        Overload the default execute behavior to allow adding additional checks for the returned values.
        In the case of distributed execution, we check the should_persist_predicate in the configuration to determine
        if the output should be persisted. This is because in distributed training, multiple nodes may produce partial
        outputs and only the user process knows the output that should be generated. They can control the choice using
        the predicate.
        """
        rval = super(SagemakerCustomTrainingTask, self).execute(**kwargs)
        if self._is_distributed():
            logging.info("Distributed context detected!")
            dctx = flytekit.current_context().distributed_training_context
            if self.task_config.should_persist_output(dctx):
                logging.info("output persistence predicate not met, Flytekit will ignore outputs")
                return {}
        return rval


# Register the Tensorflow Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(SagemakerTrainingJobConfig, SagemakerCustomTrainingTask)
