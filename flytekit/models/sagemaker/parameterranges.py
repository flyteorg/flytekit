from __future__ import absolute_import

from flyteidl.plugins.sagemaker import parameterranges_pb2 as _idl_parameterranges
from flytekit.models import common as _common
from flytekit.sdk.sagemaker_types import HyperparameterScalingType as _sdk_hyperparameter_scaling_type
from flytekit.common.exceptions import user as _user_exceptions


class ContinuousParameterRange(_common.FlyteIdlEntity):
    def __init__(
            self,
            max_value,
            min_value,
            scaling_type
    ):
        """

        :param float max_value:
        :param float min_value:
        :param flytekit.sdk.sagemaker_types.HyperparameterScalingType scaling_type:
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
        :return: flyteidl.plugins.sagemaker.ContinuousParameterRange
        """

        if self.scaling_type == _sdk_hyperparameter_scaling_type.AUTO:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.AUTO
        elif self.scaling_type == _sdk_hyperparameter_scaling_type.LINEAR:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.LINEAR
        elif self.scaling_type == _sdk_hyperparameter_scaling_type.LOGARITHMIC:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.LOGARITHMIC
        elif self.scaling_type == _sdk_hyperparameter_scaling_type.REVERSELOGARITHMIC:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.REVERSELOGARITHMIC
        else:
            raise _user_exceptions.FlyteValidationException("Invalid SageMaker HyperparameterScalingType specified")

        return _idl_parameterranges.ContinuousParameterRange(
            max_value=self._max_value,
            min_value=self._min_value,
            scaling_type=scaling_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        scaling_type = _sdk_hyperparameter_scaling_type.AUTO
        if pb2_object.type == _idl_parameterranges.HyperparameterScalingType.LINEAR:
            scaling_type = _sdk_hyperparameter_scaling_type.LINEAR
        elif pb2_object.type == _idl_parameterranges.HyperparameterScalingType.LOGARITHMIC:
            scaling_type = _sdk_hyperparameter_scaling_type.LOGARITHMIC
        elif pb2_object.type == _idl_parameterranges.HyperparameterScalingType.REVERSELOGARITHMIC:
            scaling_type = _sdk_hyperparameter_scaling_type.REVERSELOGARITHMIC

        return cls(
            max_value=pb2_object.max_value,
            min_value=pb2_object.min_value,
            scaling_type=scaling_type,
        )


class IntegerParameterRange(_common.FlyteIdlEntity):
    def __init__(
            self,
            max_value,
            min_value,
            scaling_type
    ):
        """
        :param int max_value:
        :param int min_value:
        :param flytekit.sdk.sagemaker_types.HyperparameterScalingType scaling_type:
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
        :return: flyteidl.plugins.sagemaker.IntegerParameterRange
        """

        if self.scaling_type == _sdk_hyperparameter_scaling_type.AUTO:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.AUTO
        elif self.scaling_type == _sdk_hyperparameter_scaling_type.LINEAR:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.LINEAR
        elif self.scaling_type == _sdk_hyperparameter_scaling_type.LOGARITHMIC:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.LOGARITHMIC
        elif self.scaling_type == _sdk_hyperparameter_scaling_type.REVERSELOGARITHMIC:
            scaling_type = _idl_parameterranges.HyperparameterScalingType.REVERSELOGARITHMIC
        else:
            raise _user_exceptions.FlyteValidationException("Invalid SageMaker HyperparameterScalingType specified")

        return _idl_parameterranges.IntegerParameterRange(
            max_value=self._max_value,
            min_value=self._min_value,
            scaling_type=scaling_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):

        scaling_type = _sdk_hyperparameter_scaling_type.AUTO
        if pb2_object.type == _idl_parameterranges.HyperparameterScalingType.LINEAR:
            scaling_type = _sdk_hyperparameter_scaling_type.LINEAR
        elif pb2_object.type == _idl_parameterranges.HyperparameterScalingType.LOGARITHMIC:
            scaling_type = _sdk_hyperparameter_scaling_type.LOGARITHMIC
        elif pb2_object.type == _idl_parameterranges.HyperparameterScalingType.REVERSELOGARITHMIC:
            scaling_type = _sdk_hyperparameter_scaling_type.REVERSELOGARITHMIC

        return cls(
            max_value=pb2_object.max_value,
            min_value=pb2_object.min_value,
            scaling_type=scaling_type,
        )


class CategoricalParameterRange(_common.FlyteIdlEntity):
    def __init__(
            self,
            values,
    ):
        """

        :param list[string] values: list of strings representing categorical values
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
        :return: flyteidl.plugins.sagemaker.CategoricalParameterRange
        """
        return _idl_parameterranges.CategoricalParameterRange(
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
            parameter_range_map,
    ):
        self._parameter_range_map = parameter_range_map

    def to_flyte_idl(self):
        return _idl_parameterranges.ParameterRanges(
            parameter_range_map=self._parameter_range_map,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            parameter_range_map=pb2_object.parameter_range_map
        )


class ParameterRangeOneOf(_common.FluteIdlEntity):
    def __init__(
            self,
            continuous_parameter_range=None,
            integer_parameter_range=None,
            categorical_parameter_range=None,
    ):
        """
        Defines a parameter range. It can either be a continuous parameter range
        or a integer parameter range or a categorical one.

        :param continuous_parameter_range:
        :param integer_parameter_range:
        :param categorical_parameter_range:
        """

        self._continuous_parameter_range = continuous_parameter_range
        self._integer_parameter_range = integer_parameter_range
        self._categorical_parameter_range = categorical_parameter_range

    @property
    def continuous_parameter_range(self):
        """
        :return ContinuousParameterRange:
        """

        return self._continuous_parameter_range

    @property
    def integer_parameter_range(self):
        """
        :return IntegerParameterRange:
        """

        return self._integer_parameter_range

    @property
    def categorical_parameter_range(self):
        """
        :return CategoricalParameterRange:
        """
        return self._categorical_parameter_range

    def to_flyte_idl(self):
        """

        :return: flyteidl.plugins.sagemaker._parameterranges_pb2.ParameterRangeOneOf
        """
        return _idl_parameterranges.ParameterRangeOneOf(
            continious_parameter_range=(self.continuous_parameter_range.to_flyte_idl()
                                        if self.continuous_parameter_range else None),
            integer_parameter_range=(self.integer_parameter_range.to_flyte_idl()
                                     if self.integer_parameter_range else None),
            categorical_parameter_range=(self.categorical_parameter_range.to_flyte_idl()
                                         if self.categorical_parameter_range else None),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            continuous_parameter_range=(
                ContinuousParameterRange.from_flyte_idl(pb2_object.continuous_parameter_range)
                if pb2_object.HasField('continuous_parameter_range') else None
            ),
            integer_parameter_range=(
                IntegerParameterRange.from_flyte_idl(pb2_object.integer_parameter_range)
                if pb2_object.HasField('integer_parameter_range') else None
            ),
            categorical_parameter_range=(
                CategoricalParameterRange.from_flyte_idl(pb2_object.continuous_parameter_range)
                if pb2_object.HasField('categorical_parameter_range') else None
            ),
        )
