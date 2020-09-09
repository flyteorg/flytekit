from typing import Dict, List

from flyteidl.plugins.sagemaker import parameter_ranges_pb2 as _idl_parameter_ranges

from flytekit.models import common as _common


class HyperparameterScalingType(object):
    AUTO = _idl_parameter_ranges.HyperparameterScalingType.AUTO
    LINEAR = _idl_parameter_ranges.HyperparameterScalingType.LINEAR
    LOGARITHMIC = _idl_parameter_ranges.HyperparameterScalingType.LOGARITHMIC
    REVERSELOGARITHMIC = _idl_parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC


class ContinuousParameterRange(_common.FlyteIdlEntity):
    def __init__(
        self, max_value: float, min_value: float, scaling_type: int,
    ):
        """

        :param float max_value:
        :param float min_value:
        :param int scaling_type:
        """
        self._max_value = max_value
        self._min_value = min_value
        self._scaling_type = scaling_type

    @property
    def max_value(self) -> float:
        """

        :rtype: float
        """
        return self._max_value

    @property
    def min_value(self) -> float:
        """

        :rtype: float
        """
        return self._min_value

    @property
    def scaling_type(self) -> int:
        """
        enum value from HyperparameterScalingType
        :rtype: int
        """
        return self._scaling_type

    def to_flyte_idl(self) -> _idl_parameter_ranges.ContinuousParameterRange:
        """
        :rtype: _idl_parameter_ranges.ContinuousParameterRange
        """

        return _idl_parameter_ranges.ContinuousParameterRange(
            max_value=self._max_value, min_value=self._min_value, scaling_type=self.scaling_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _idl_parameter_ranges.ContinuousParameterRange):
        """

        :param pb2_object:
        :rtype: ContinuousParameterRange
        """
        return cls(
            max_value=pb2_object.max_value, min_value=pb2_object.min_value, scaling_type=pb2_object.scaling_type,
        )


class IntegerParameterRange(_common.FlyteIdlEntity):
    def __init__(
        self, max_value: int, min_value: int, scaling_type: int,
    ):
        """
        :param int max_value:
        :param int min_value:
        :param int scaling_type:
        """
        self._max_value = max_value
        self._min_value = min_value
        self._scaling_type = scaling_type

    @property
    def max_value(self) -> int:
        """
        :rtype: int
        """
        return self._max_value

    @property
    def min_value(self) -> int:
        """

        :rtype: int
        """
        return self._min_value

    @property
    def scaling_type(self) -> int:
        """
        enum value from HyperparameterScalingType
        :rtype: int
        """
        return self._scaling_type

    def to_flyte_idl(self) -> _idl_parameter_ranges.IntegerParameterRange:
        """
        :rtype: _idl_parameter_ranges.IntegerParameterRange
        """
        return _idl_parameter_ranges.IntegerParameterRange(
            max_value=self._max_value, min_value=self._min_value, scaling_type=self.scaling_type,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _idl_parameter_ranges.IntegerParameterRange):
        """

        :param pb2_object:
        :rtype: IntegerParameterRange
        """
        return cls(
            max_value=pb2_object.max_value, min_value=pb2_object.min_value, scaling_type=pb2_object.scaling_type,
        )


class CategoricalParameterRange(_common.FlyteIdlEntity):
    def __init__(
        self, values: List[str],
    ):
        """

        :param List[str] values: list of strings representing categorical values
        """
        self._values = values

    @property
    def values(self) -> List[str]:
        """
        :rtype: List[str]
        """
        return self._values

    def to_flyte_idl(self) -> _idl_parameter_ranges.CategoricalParameterRange:
        """
        :rtype: _idl_parameter_ranges.CategoricalParameterRange
        """
        return _idl_parameter_ranges.CategoricalParameterRange(values=self._values)

    @classmethod
    def from_flyte_idl(cls, pb2_object: _idl_parameter_ranges.CategoricalParameterRange):
        return cls(values=pb2_object.values)


class ParameterRanges(_common.FlyteIdlEntity):
    def __init__(
        self, parameter_range_map: Dict[str, _common.FlyteIdlEntity],
    ):
        self._parameter_range_map = parameter_range_map

    def to_flyte_idl(self) -> _idl_parameter_ranges.ParameterRanges:
        converted = {}
        for k, v in self._parameter_range_map.items():
            if isinstance(v, IntegerParameterRange):
                converted[k] = _idl_parameter_ranges.ParameterRangeOneOf(integer_parameter_range=v.to_flyte_idl())
            elif isinstance(v, ContinuousParameterRange):
                converted[k] = _idl_parameter_ranges.ParameterRangeOneOf(continuous_parameter_range=v.to_flyte_idl())
            else:
                converted[k] = _idl_parameter_ranges.ParameterRangeOneOf(categorical_parameter_range=v.to_flyte_idl())

        return _idl_parameter_ranges.ParameterRanges(parameter_range_map=converted,)

    @classmethod
    def from_flyte_idl(cls, pb2_object: _idl_parameter_ranges.ParameterRanges):
        converted = {}
        for k, v in pb2_object.parameter_range_map.items():
            if isinstance(v, _idl_parameter_ranges.ContinuousParameterRange):
                converted[k] = ContinuousParameterRange.from_flyte_idl(v)
            elif isinstance(v, _idl_parameter_ranges.IntegerParameterRange):
                converted[k] = IntegerParameterRange.from_flyte_idl(v)
            else:
                converted[k] = CategoricalParameterRange.from_flyte_idl(v)

        return cls(parameter_range_map=converted,)
