from __future__ import absolute_import, annotations

from flyteidl.plugins.sagemaker import hyperparameter_tuning_job_pb2 as _pb2_hpo_job
from flytekit.models import common as _common
from flytekit.sdk.sagemaker import types as _sdk_sagemaker_types
from flytekit.models.sagemaker import parameter_ranges as _parameter_ranges_models, training_job as _training_job


class HyperparameterTuningObjectiveType(object):
    MINIMIZE = _pb2_hpo_job.HyperparameterTuningObjectiveType.MINIMIZE
    MAXIMIZE = _pb2_hpo_job.HyperparameterTuningObjectiveType.MAXIMIZE


class HyperparameterTuningObjective(_common.FlyteIdlEntity):
    def __init__(
            self,
            objective_type: int,
            metric_name: str,
    ):
        self._objective_type = objective_type
        self._metric_name = metric_name

    @property
    def objective_type(self: HyperparameterTuningObjective) -> int:
        """
        :rtype: int
        """
        return self._objective_type

    @property
    def metric_name(self: HyperparameterTuningObjective) -> str:
        """
        :rtype: str
        """
        return self._metric_name

    def to_flyte_idl(self: HyperparameterTuningObjective) -> _pb2_hpo_job.HyperparameterTuningObjective:

        return _pb2_hpo_job.HyperparameterTuningObjective(
            objective_type=self.objective_type,
            metric_name=self._metric_name,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _pb2_hpo_job.HyperparameterTuningObjective) -> HyperparameterTuningObjective:

        return cls(
            objective_type=pb2_object.objective_type,
            metric_name=pb2_object.metric_name,
        )


class HyperparameterTuningStrategy:
    BAYESIAN = _pb2_hpo_job.HyperparameterTuningStrategy.BAYESIAN
    RANDOM = _pb2_hpo_job.HyperparameterTuningStrategy.RANDOM


class TrainingJobEarlyStoppingType:
    OFF = _pb2_hpo_job.TrainingJobEarlyStoppingType.OFF
    AUTO = _pb2_hpo_job.TrainingJobEarlyStoppingType.AUTO


class HyperparameterTuningJobConfig(_common.FlyteIdlEntity):
    def __init__(
            self,
            hyperparameter_ranges: _parameter_ranges_models.ParameterRanges,
            tuning_strategy: _sdk_sagemaker_types.HyperparameterTuningStrategy,
            tuning_objective: HyperparameterTuningObjective,
            training_job_early_stopping_type: _sdk_sagemaker_types.TrainingJobEarlyStoppingType,
    ):
        self._hyperparameter_ranges = hyperparameter_ranges
        self._tuning_strategy = tuning_strategy
        self._tuning_objective = tuning_objective
        self._training_job_early_stopping_type = training_job_early_stopping_type

    def to_flyte_idl(self: HyperparameterTuningJobConfig) -> _pb2_hpo_job.HyperparameterTuningJobConfig:

        return _pb2_hpo_job.HyperparameterTuningJobConfig(
            hyperparameter_ranges=self._hyperparameter_ranges.to_flyte_idl(),
            tuning_strategy=self._tuning_strategy,
            tuning_objective=self._tuning_objective.to_flyte_idl(),
            training_job_early_stopping_type=self._training_job_early_stopping_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _pb2_hpo_job.HyperparameterTuningJobConfig) -> HyperparameterTuningJobConfig:

        return cls(
            hyperparameter_ranges=(
                _parameter_ranges_models.ParameterRanges.from_flyte_idl(pb2_object.hyperparameter_ranges)),
            tuning_strategy=pb2_object.tuning_strategy,
            tuning_objective=HyperparameterTuningObjective.from_flyte_idl(pb2_object.tuning_objective),
            training_job_early_stopping_type=pb2_object.training_job_early_stopping_type,
        )


class HyperparameterTuningJob(_common.FlyteIdlEntity):

    def __init__(
            self,
            max_number_of_training_jobs: int,
            max_parallel_training_jobs: int,
            training_job: _training_job.TrainingJob,
    ):
        self._max_number_of_training_jobs = max_number_of_training_jobs
        self._max_parallel_training_jobs = max_parallel_training_jobs
        self._training_job = training_job

    @property
    def max_number_of_training_jobs(self: HyperparameterTuningJob) -> int:
        return self._max_number_of_training_jobs

    @property
    def max_parallel_training_jobs(self: HyperparameterTuningJob) -> int:
        return self._max_parallel_training_jobs

    def to_flyte_idl(self: HyperparameterTuningJob) -> _pb2_hpo_job.HyperparameterTuningJob:
        return _pb2_hpo_job.HyperparameterTuningJob(
            max_number_of_training_jobs=self._max_number_of_training_jobs,
            max_parallel_training_jobs=self._max_parallel_training_jobs,
            training_job=self._training_job.to_flyte_idl(),  # SDK task has already serialized it
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _pb2_hpo_job.HyperparameterTuningJob) -> HyperparameterTuningJob:
        return cls(
            max_number_of_training_jobs=pb2_object.max_number_of_training_jobs,
            max_parallel_training_jobs=pb2_object.max_parallel_training_jobs,
            training_job=_training_job.TrainingJob.from_flyte_idl(pb2_object.training_job),
        )
