from __future__ import absolute_import

from typing import Dict, List
from flyteidl.plugins.sagemaker import parameter_ranges_pb2 as _idl_parameter_ranges
from flytekit.models import common as _common
from flytekit.sdk.sagemaker import types as _sdk_sagemaker_types
from flytekit.common.exceptions import user as _user_exceptions


class ContinuousParameterRange(_common.FlyteIdlEntity):
    def __init__(
            self,
            max_value: float,
            min_value: float,
            scaling_type: _sdk_sagemaker_types.HyperparameterScalingType,
    ):
        """

        :param float max_value:
        :param float min_value:
        :param _sdk_sagemaker_types.HyperparameterScalingType scaling_type:
        """
        self._max_value = max_value
        self._min_value = min_value
        self._scaling_type = scaling_type

    @property
    def max_value(self):
        """

        :return: float
        """
        return self._max_value

    @property
    def min_value(self):
        """

        :return: float
        """
        return self._min_value

    @property
    def scaling_type(self):
        """
        enum value from HyperparameterScalingType
        :return: int
        """
        return self._scaling_type

    def to_flyte_idl(self):
        """
        :return: _idl_parameter_ranges.ContinuousParameterRange
        """

        if self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.AUTO:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.AUTO
        elif self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.LINEAR:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.LINEAR
        elif self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.LOGARITHMIC:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.LOGARITHMIC
        elif self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.REVERSELOGARITHMIC:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC
        else:
            raise _user_exceptions.FlyteValidationException("Invalid SageMaker HyperparameterScalingType specified")

        return _idl_parameter_ranges.ContinuousParameterRange(
            max_value=self._max_value,
            min_value=self._min_value,
            scaling_type=scaling_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.AUTO
        if pb2_object.type == _idl_parameter_ranges.HyperparameterScalingType.LINEAR:
            scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.LINEAR
        elif pb2_object.type == _idl_parameter_ranges.HyperparameterScalingType.LOGARITHMIC:
            scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.LOGARITHMIC
        elif pb2_object.type == _idl_parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC:
            scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.REVERSELOGARITHMIC

        return cls(
            max_value=pb2_object.max_value,
            min_value=pb2_object.min_value,
            scaling_type=scaling_type,
        )


class IntegerParameterRange(_common.FlyteIdlEntity):
    def __init__(
            self,
            max_value: int,
            min_value: int,
            scaling_type: _sdk_sagemaker_types.HyperparameterScalingType,
    ):
        """
        :param int max_value:
        :param int min_value:
        :param _sdk_sagemaker_types.HyperparameterScalingType scaling_type:
        """
        self._max_value = max_value
        self._min_value = min_value
        self._scaling_type = scaling_type

    @property
    def max_value(self):
        """
        :return: int
        """
        return self._max_value

    @property
    def min_value(self):
        """

        :return: int
        """
        return self._min_value

    @property
    def scaling_type(self):
        """
        enum value from HyperparameterScalingType
        :return: int
        """
        return self._scaling_type

    def to_flyte_idl(self):
        """
        :return: _idl_parameter_ranges.IntegerParameterRange
        """

        if self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.AUTO:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.AUTO
        elif self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.LINEAR:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.LINEAR
        elif self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.LOGARITHMIC:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.LOGARITHMIC
        elif self.scaling_type == _sdk_sagemaker_types.HyperparameterScalingType.REVERSELOGARITHMIC:
            scaling_type = _idl_parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC
        else:
            raise _user_exceptions.FlyteValidationException("Invalid SageMaker HyperparameterScalingType specified")

        return _idl_parameter_ranges.IntegerParameterRange(
            max_value=self._max_value,
            min_value=self._min_value,
            scaling_type=scaling_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.AUTO
        if pb2_object.type == _idl_parameter_ranges.HyperparameterScalingType.LINEAR:
            scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.LINEAR
        elif pb2_object.type == _idl_parameter_ranges.HyperparameterScalingType.LOGARITHMIC:
            scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.LOGARITHMIC
        elif pb2_object.type == _idl_parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC:
            scaling_type = _sdk_sagemaker_types.HyperparameterScalingType.REVERSELOGARITHMIC

        return cls(
            max_value=pb2_object.max_value,
            min_value=pb2_object.min_value,
            scaling_type=scaling_type,
        )


class CategoricalParameterRange(_common.FlyteIdlEntity):
    def __init__(
            self,
            values: List[str],
    ):
        """

        :param List[str] values: list of strings representing categorical values
        """
        self._values = values

    @property
    def values(self):
        """
        :return: list[string]
        """
        return self._values

    def to_flyte_idl(self):
        """
        :return: _idl_parameter_ranges.CategoricalParameterRange
        """
        return _idl_parameter_ranges.CategoricalParameterRange(
            values=self._values
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            values=pb2_object.values
        )


class ParameterRanges(_common.FlyteIdlEntity):
    def __init__(
            self,
            parameter_range_map: Dict[str, _common.FlyteIdlEntity],
    ):
        self._parameter_range_map = parameter_range_map

    def to_flyte_idl(self):
        converted = {}
        for k, v in self._parameter_range_map.items():
            if isinstance(v, IntegerParameterRange):
                converted[k] = _idl_parameter_ranges.ParameterRangeOneOf(integer_parameter_range=v.to_flyte_idl())
            elif isinstance(v, ContinuousParameterRange):
                converted[k] = _idl_parameter_ranges.ParameterRangeOneOf(continuous_parameter_range=v.to_flyte_idl())
            else:
                converted[k] = _idl_parameter_ranges.ParameterRangeOneOf(categorical_parameter_range=v.to_flyte_idl())

        return _idl_parameter_ranges.ParameterRanges(
            parameter_range_map=converted,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        converted = {}
        for k, v in pb2_object.parameter_range_map.items():
            if isinstance(v, _idl_parameter_ranges.ContinuousParameterRange):
                converted[k] = ContinuousParameterRange.from_flyte_idl(v)
            elif isinstance(v, _idl_parameter_ranges.IntegerParameterRange):
                converted[k] = IntegerParameterRange.from_flyte_idl(v)
            else:
                converted[k] = CategoricalParameterRange.from_flyte_idl(v)

        return cls(
            parameter_range_map=converted,
        )
