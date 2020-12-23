from dataclasses import dataclass
from typing import Any, Union, Dict, List

from google.protobuf.json_format import MessageToDict

from flytekit.annotated.base_task import PythonTask
from flytekit.annotated.context_manager import RegistrationSettings
from flytekit.models import task as _task_model
from flytekit.models.sagemaker import hpo_job as _hpo_job_model, training_job as _training_job_model
from flytekit.taskplugins.sagemaker.training import SagemakerBuiltinAlgorithmsTask, SagemakerCustomTrainingTask


@dataclass
class HPOJob(object):
    max_number_of_training_jobs: int
    max_parallel_training_jobs: int
    tunable_params: List[str]


# TODO Not done yet, but once we clean this up, the client interface should be simplified. The interface should
# Just take a list of Union of different types of Parameter Ranges. Lets see how simplify that
class SagemakerHPOTask(PythonTask):
    _SAGEMAKER_HYPERPARAMETER_TUNING_JOB_TASK = "sagemaker_hyperparameter_tuning_job_task"

    def __init__(self, name: str, metadata: _task_model.TaskMetadata, task_config: HPOJob,
                 training_task: Union[SagemakerCustomTrainingTask, SagemakerBuiltinAlgorithmsTask], *args, **kwargs):
        if training_task is None or not (
                isinstance(training_task, SagemakerCustomTrainingTask) or
                isinstance(training_task, SagemakerBuiltinAlgorithmsTask)):
            raise ValueError(
                "Training Task of type SagemakerCustomTrainingTask/SagemakerBuiltinAlgorithmsTask is required to work"
                " with Sagemaker HPO")

        self._training_task = training_task
        iface = training_task.python_interface

        # TODO Ideally these should not be dict, but the actual classes as DataClasses
        # HyperparameterTuningJobConfig
        extra_inputs = {"hyperparameter_tuning_job_config": dict}

        self._hpo_job_config = task_config

        if self._hpo_job_config.tunable_params:
            # TODO Ideally these should not be dict, but the actual classes as DataClasses ParameterRange
            extra_inputs.update({param: dict for param in self._hpo_job_config.tunable_params})

        updated_iface = iface.with_inputs(extra_inputs)
        super().__init__(
            task_type=self._SAGEMAKER_HYPERPARAMETER_TUNING_JOB_TASK,
            name=name, interface=updated_iface, metadata=metadata, *args, **kwargs)

    def execute(self, **kwargs) -> Any:
        raise AssertionError("Sagemaker HPO Task cannot be executed locally, to execute locally mock it!")

    def get_custom(self, settings: RegistrationSettings) -> Dict[str, Any]:
        training_job = _training_job_model.TrainingJob(
            algorithm_specification=self._training_task.task_config.algorithm_specification,
            training_job_resource_config=self._training_task.task_config.training_job_resource_config,
        )
        return MessageToDict(_hpo_job_model.HyperparameterTuningJob(
            max_number_of_training_jobs=self._hpo_job_config.max_number_of_training_jobs,
            max_parallel_training_jobs=self._hpo_job_config.max_parallel_training_jobs,
            training_job=training_job,
        ).to_flyte_idl())
