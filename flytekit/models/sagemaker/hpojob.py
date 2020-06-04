from __future__ import absolute_import

from flyteidl.plugins.sagemaker import hpojob_pb2 as _hpojob
from flytekit.models import common as _common


class HPOJobObjective(_common.FlyteIdlEntity):
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
        :return: _hpojob.HPOJobObjective.HPOJobObjectiveType
        """
        return self._objective_type

    @property
    def metric_name(self):
        """
        :return: string
        """
        return self._metric_name

    def to_flyte_idl(self):
        return _hpojob.HPOJobTypeObjective(
            objective_type=self._objective_type,
            metric_name=self._metric_name,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            objective_type=pb2_object.objective_type,
            metric_name=pb2_object.metric_name,
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
        return _hpojob.HPOJob(
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
