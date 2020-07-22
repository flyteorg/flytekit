from __future__ import absolute_import

from flyteidl.plugins.sagemaker import hpo_job_pb2 as _idl_hpo_job
from flytekit.models import common as _common
from flytekit.models import task as _task_model
from flytekit.sdk.sagemaker import types as _sdk_sagemaker_types
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models.sagemaker import parameter_ranges as _model_parameter_ranges, training_job as _training_job


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
            objective_type = _idl_hpo_job.HyperparameterTuningObjective.MINIMIZE
        elif self.objective_type == _sdk_sagemaker_types.HyperparameterTuningObjectiveType.MAXIMIZE:
            objective_type = _idl_hpo_job.HyperparameterTuningObjective.MAXIMIZE
        else:
            raise _user_exceptions.FlyteValidationException(
                "Invalid SageMaker Hyperparameter Tuning Objective Type Specified"
            )

        return _idl_hpo_job.HyperparameterTuningObjective(
            objective_type=objective_type,
            metric_name=self._metric_name,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        objective_type = _sdk_sagemaker_types.HyperparameterTuningObjectiveType.MINIMIZE
        if pb2_object.objective_type == _idl_hpo_job.HyperparameterTuningObjective.MAXIMIZE:
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

        if self._tuning_strategy == _sdk_sagemaker_types.HyperparameterTuningStrategy.BAYESIAN:
            idl_strategy = _idl_hpo_job.HPOJobConfig.HyperparameterTuningStrategy.BAYESIAN
        elif self._tuning_strategy == _sdk_sagemaker_types.HyperparameterTuningStrategy.RANDOM:
            idl_strategy = _idl_hpo_job.HPOJobConfig.HyperparameterTuningStrategy.RANDOM
        else:
            raise _user_exceptions.FlyteValidationException(
                "Invalid Hyperparameter Tuning Strategy: {}".format(self._tuning_strategy))

        if self._training_job_early_stopping_type == _sdk_sagemaker_types.TrainingJobEarlyStoppingType.OFF:
            idl_training_early_stopping_type = _idl_hpo_job.HPOJobConfig.TrainingJobEarlyStoppingType.OFF
        elif self._training_job_early_stopping_type == _sdk_sagemaker_types.TrainingJobEarlyStoppingType.AUTO:
            idl_training_early_stopping_type = _idl_hpo_job.HPOJobConfig.TrainingJobEarlyStoppingType.AUTO
        else:
            raise _user_exceptions.FlyteValidationException(
                "Invalid Training Job Early Stopping Type (in HPO Config): {}".format(
                    self._training_job_early_stopping_type))

        return _idl_hpo_job.HPOJobConfig(
            hyperparameter_ranges=self._hyperparameter_ranges.to_flyte_idl(),
            tuning_strategy=idl_strategy,
            tuning_objective=self._tuning_objective.to_flyte_idl(),
            training_job_early_stopping_type=idl_training_early_stopping_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        sdk_strategy = _sdk_sagemaker_types.HyperparameterTuningStrategy.BAYESIAN
        if pb2_object.tuning_strategy == _idl_hpo_job.HPOJobConfig.HyperparameterTuningStrategy.RANDOM:
            sdk_strategy = _sdk_sagemaker_types.HyperparameterTuningStrategy.RANDOM

        sdk_training_early_stopping_type = _sdk_sagemaker_types.TrainingJobEarlyStoppingType.OFF
        if pb2_object.training_job_early_stopping_type == _idl_hpo_job.HPOJobConfig.TrainingJobEarlyStoppingType.AUTO:
            sdk_training_early_stopping_type = _sdk_sagemaker_types.TrainingJobEarlyStoppingType.AUTO

        return cls(
            hyperparameter_ranges=(
                _model_parameter_ranges.ParameterRanges.from_flyte_idl(pb2_object.hyperparameter_ranges)),
            tuning_strategy=sdk_strategy,
            tuning_objective=HyperparameterTuningObjective.from_flyte_idl(pb2_object.tuning_objective),
            training_job_early_stopping_type=sdk_training_early_stopping_type,
        )


class HPOJob(_common.FlyteIdlEntity):
    def __init__(
            self,
            max_number_of_training_jobs,
            max_parallel_training_jobs,
            training_job: _training_job.TrainingJob,
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
        return _idl_hpo_job.HPOJob(
            max_number_of_training_jobs=self._max_number_of_training_jobs,
            max_parallel_training_jobs=self._max_parallel_training_jobs,
            training_job=self._training_job.to_flyte_idl(),  # SDK task has already serialized it
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            max_number_of_training_jobs=pb2_object.max_number_of_training_jobs,
            max_parallel_training_jobs=pb2_object.max_parallel_training_jobs,
            training_job=_training_job.TrainingJob.from_flyte_idl(pb2_object.training_job),
        )


class HPOJobCustom(_common.FlyteIdlEntity):
    def __init__(
            self,
            hpo_job_core: HPOJob,
            training_job_task_template: _task_model.TaskTemplate,
    ):
        self._hpo_job_core = hpo_job_core
        self._training_job_task_template = training_job_task_template

    @property
    def hpo_job_core(self):
        return self._hpo_job_core

    @property
    def training_job_task_template(self):
        return self._training_job_task_template

    def to_flyte_idl(self):
        return _idl_hpo_job.HPOJobCustom(
            hpo_job_core=self._hpo_job_core.to_flyte_idl(),
            training_job_task_template=self._training_job_task_template.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            hpo_job_core=HPOJob.from_flyte_idl(pb2_object.hpo_job_core),
            training_job_task_template=_task_model.TaskTemplate.from_flyte_idl(pb2_object.training_job_task_template)
        )