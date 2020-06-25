from __future__ import absolute_import

from flyteidl.plugins.sagemaker import training_job_pb2 as _training_job

from flytekit.models import common as _common
from flytekit.sdk.sagemaker import types as _sdk_sagemaker_types
from flytekit.common.exceptions import user as _user_exceptions


class StoppingCondition(_common.FlyteIdlEntity):
    def __init__(
            self,
            max_runtime_in_seconds,
            max_wait_time_in_seconds,
    ):
        self._max_runtime_in_seconds = max_runtime_in_seconds
        self._max_wait_time_in_seconds = max_wait_time_in_seconds

    @property
    def max_runtime_in_seconds(self):
        """

        :return: int
        """
        return self._max_runtime_in_seconds

    @property
    def max_wait_time_in_seconds(self):
        """

        :return: int
        """
        return self._max_wait_time_in_seconds

    def to_flyte_idl(self):
        return _training_job.StoppingCondition(
            max_runtime_in_seconds=self.max_runtime_in_seconds,
            max_wait_time_in_seconds=self.max_wait_time_in_seconds,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            max_runtime_in_seconds=pb2_object.max_runtime_in_seconds,
            max_wait_time_in_seconds=pb2_object.max_wait_time_in_seconds,
        )


class TrainingJobConfig(_common.FlyteIdlEntity):
    def __init__(
            self,
            instance_count,
            instance_type,
            volume_size_in_gb,
    ):
        self._instance_count = instance_count
        self._instance_type = instance_type
        self._volume_size_in_gb = volume_size_in_gb

    @property
    def instance_count(self):
        """
        :return: int
        """
        return self._instance_count

    @property
    def instance_type(self):
        """
        :return: string
        """
        return self._instance_type

    @property
    def volume_size_in_gb(self):
        """
        :return: string
        """
        return self._volume_size_in_gb

    def to_flyte_idl(self):
        return _training_job.TrainingJobConfig(
            instance_count=self.instance_count,
            instance_type=self.instance_type,
            volume_size_in_gb=self.volume_size_in_gb,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            instance_count=pb2_object.instance_count,
            instance_type=pb2_object.instance_type,
            volume_size_in_gb=pb2_object.volume_size_in_gb,
        )


class MetricDefinition(_common.FlyteIdlEntity):
    def __init__(
            self,
            name,
            regex,
    ):
        self._name = name
        self._regex = regex

    @property
    def name(self):
        return self._name

    @property
    def regex(self):
        return self._regex

    def to_flyte_idl(self):
        return _training_job.AlgorithmSpecification.MetricDefinition(
            name=self.name,
            regex=self.regex,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            name=pb2_object.name,
            regex=pb2_object.regex,
        )


class AlgorithmSpecification(_common.FlyteIdlEntity):
    def __init__(
            self,
            input_mode,
            algorithm_name,
            algorithm_version,
            metric_definitions,
    ):
        self._input_mode = input_mode
        self._algorithm_name = algorithm_name
        self._algorithm_version = algorithm_version
        self._metric_definitions = metric_definitions

    @property
    def input_mode(self):
        return self._input_mode

    @property
    def algorithm_name(self):
        return self._algorithm_name

    @property
    def algorithm_version(self):
        return self._algorithm_version

    @property
    def metric_definitions(self):
        """

        :return: [MetricDefinition]
        """
        return self._metric_definitions

    def to_flyte_idl(self):

        if self.input_mode == _sdk_sagemaker_types.InputMode.FILE:
            input_mode = _training_job.InputMode.FILE
        elif self.input_mode == _sdk_sagemaker_types.InputMode.PIPE:
            input_mode = _training_job.InputMode.PIPE
        else:
            raise _user_exceptions.FlyteValidationException("Invalid SageMaker Input Mode Specified: [{}]".format(self.input_mode))

        alg_name = _sdk_sagemaker_types.AlgorithmName.CUSTOM
        if self.algorithm_name == _sdk_sagemaker_types.AlgorithmName.CUSTOM:
            alg_name = _training_job.AlgorithmName.CUSTOM
        elif self.algorithm_name == _sdk_sagemaker_types.AlgorithmName.XGBOOST:
            alg_name = _training_job.AlgorithmName.XGBOOST
        else:
            raise _user_exceptions.FlyteValidationException("Invalid SageMaker Algorithm Name Specified: [{}]".format(self.algorithm_name))

        return _training_job.AlgorithmSpecification(
            input_mode=input_mode,
            algorithm_name=alg_name,
            algorithm_version=self.algorithm_version,
            metric_definitions=[m.to_flyte_idl() for m in self.metric_definitions],
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        input_mode = _sdk_sagemaker_types.InputMode.FILE
        if pb2_object.input_mode == _training_job.InputMode.PIPE:
            input_mode = _sdk_sagemaker_types.InputMode.PIPE

        algorithm_name = _sdk_sagemaker_types.AlgorithmName.CUSTOM
        if pb2_object.algorithm_name == _training_job.AlgorithmName.XGBOOST:
            algorithm_name = _sdk_sagemaker_types.AlgorithmName.XGBOOST

        return cls(
            input_mode=input_mode,
            algorithm_name=algorithm_name,
            algorithm_version=pb2_object.algorithm_version,
            metric_definitions=[MetricDefinition.from_flyte_idl(m) for m in pb2_object.metric_definitions],
        )


class TrainingJob(_common.FlyteIdlEntity):
    def __init__(
            self,
            algorithm_specification,
            training_job_config,
    ):
        self._algorithm_specification = algorithm_specification
        self._training_job_config = training_job_config

    @property
    def algorithm_specification(self):
        """
        :return:
        """
        return self._algorithm_specification

    @property
    def training_job_config(self):
        """
        :return:
        """
        return self._training_job_config

    def to_flyte_idl(self):
        """
        :return: _training_job.TrainingJob
        """

        return _training_job.TrainingJob(
            algorithm_specification=self.algorithm_specification.to_flyte_idl(),
            training_job_config=self.training_job_config.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param _training_job.TrainingJob pb2_object:
        :return: TrainingJob
        """
        return cls(
            algorithm_specification=pb2_object.algorithm_specification,
            training_job_config=pb2_object.training_job_config,
        )
