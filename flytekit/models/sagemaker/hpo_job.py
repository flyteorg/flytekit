from __future__ import absolute_import

from flyteidl.plugins.sagemaker import hpo_job_pb2 as _hpo_job
from flytekit.models import common as _common
from flytekit.sdk.sagemaker import types as _sdk_sagemaker_types
from flytekit.common.exceptions import user as _user_exceptions


class HyperparameterTuningObjective(_common.FlyteIdlEntity):
    def __init__(
            self,
            objective_type,
            metric_name,
    ):
        self._objective_type = objective_type
        self._metric_name = metric_name

    @property
    def objective_type(self):
        """
        :return: _hpo_job.HyperparameterTuningObjective.HyperparameterTuningObjectiveType
        """
        return self._objective_type

    @property
    def metric_name(self):
        """
        :return: string
        """
        return self._metric_name

    def to_flyte_idl(self):

        if self.objective_type == _sdk_sagemaker_types.HyperparameterTuningObjectiveType.MINIMIZE:
            objective_type = _hpo_job.HyperparameterTuningObjective.MINIMIZE
        elif self.objective_type == _sdk_sagemaker_types.HyperparameterTuningObjectiveType.MAXIMIZE:
            objective_type = _hpo_job.HyperparameterTuningObjective.MAXIMIZE
        else:
            raise _user_exceptions.FlyteValidationException(
                "Invalid SageMaker Hyperparameter Tuning Objective Type Specified"
            )

        return _hpo_job.HyperparameterTuningObjective(
            objective_type=objective_type,
            metric_name=self._metric_name,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        objective_type = _sdk_sagemaker_types.HyperparameterTuningObjectiveType.MINIMIZE
        if pb2_object.objective_type == _hpo_job.HyperparameterTuningObjective.MAXIMIZE:
            objective_type = _sdk_sagemaker_types.HyperparameterTuningObjectiveType.MAXIMIZE

        return cls(
            objective_type=objective_type,
            metric_name=pb2_object.metric_name,
        )


class HPOJobConfig(_common.FlyteIdlEntity):
    def __init__(
            self,
            hyperparameter_ranges,
            tuning_strategy,
            tuning_objective,
            training_job_early_stopping_type
    ):
        self._hyperparameter_ranges = hyperparameter_ranges
        self._tuning_strategy = tuning_strategy
        self._tuning_objective = tuning_objective
        self._training_job_early_stopping_type = training_job_early_stopping_type

    def to_flyte_idl(self):
        return _hpo_job.HPOJobConfig(
            hyperparameter_ranges=self._hyperparameter_ranges,
            tuning_strategy=self._tuning_strategy,
            tuning_objective=self._tuning_objective,
            training_job_early_stopping_type=self._training_job_early_stopping_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            hyperparameter_ranges=pb2_object.hyperparameter_ranges,
            tuning_strategy=pb2_object.tuning_strategy,
            tuning_objective=pb2_object.tuning_objective,
            training_job_early_stopping_type=pb2_object.training_job_early_stopping_type,
        )


class HPOJob(_common.FlyteIdlEntity):

    def __init__(
            self,
            max_number_of_training_jobs,
            max_parallel_training_jobs,
            training_job,
    ):
        self._max_number_of_training_jobs = max_number_of_training_jobs
        self._max_parallel_training_jobs = max_parallel_training_jobs
        self._training_job = training_job

    @property
    def max_number_of_training_jobs(self):
        return self._max_number_of_training_jobs

    @property
    def max_parallel_training_jobs(self):
        return self._max_parallel_training_jobs

    def to_flyte_idl(self):
        return _hpo_job.HPOJob(
            max_number_of_training_jobs=self._max_number_of_training_jobs,
            max_parallel_training_jobs=self._max_parallel_training_jobs,
            training_job=self._training_job,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            max_number_of_training_jobs=pb2_object.max_number_of_training_jobs,
            max_parallel_training_jobs=pb2_object.max_parallel_training_jobs,
            training_job=pb2_object.training_job,
        )
