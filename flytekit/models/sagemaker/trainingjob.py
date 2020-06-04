from __future__ import absolute_import

from flyteidl.plugins.sagemaker import trainingjob_pb2 as _trainingjob

from flytekit.models import common as _common


class TrainingJob(_common.FlyteIdlEntity):

    def __init__(
            self,
            region,
            role_arn,
            algorithm_specification,
            resource_config,
            stopping_condition,
            vpc_config,
            enable_spot_training,
    ):
        self._region = region
        self._role_arn = role_arn
        self._algorithm_specification = algorithm_specification
        self._resource_config = resource_config
        self._stopping_condition = stopping_condition
        self._vpc_config = vpc_config
        self._enable_spot_training = enable_spot_training

    @property
    def region(self):
        """
        :return: string
        """
        return self._region

    @property
    def role_arn(self):
        """
        :return: string
        """
        return self._role_arn

    @property
    def algorithm_specification(self):
        """
        :return:
        """
        return self._algorithm_specification

    @property
    def resource_config(self):
        """
        :return:
        """
        return self._resource_config

    @property
    def stopping_condition(self):
        """
        :return:
        """
        return self._stopping_condition

    @property
    def vpc_config(self):
        """
        :return:
        """
        return self._vpc_config

    @property
    def enable_spot_training(self):
        """
        :return: bool
        """
        return self._enable_spot_training

    def to_flyte_idl(self):
        """
        :return: _trainingjob.TrainingJob
        """

        return _trainingjob.TrainingJob(
            region=self._region,
            role_arn=self._role_arn,
            algorithm_specification=self._algorithm_specification,
            resource_config=self._resource_config,
            stopping_condition=self._stopping_condition,
            vpc_config=self._vpc_config,
            enable_spot_training=self._enable_spot_training,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param _trainingjob.TrainingJob pb2_object:
        :return: TrainingJob
        """
        return cls(
            region=pb2_object.region,
            role_arn=pb2_object.role_arn,
            algorithm_specification=pb2_object.algorithm_specification,
            resource_config=pb2_object.resource_config,
            stopping_condition=pb2_object.stopping_condition,
            vpc_config=pb2_object.vpc_config,
            enable_spot_training=pb2_object.enable_spot_training,
        )
