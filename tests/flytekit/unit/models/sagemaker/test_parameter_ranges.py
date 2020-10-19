import unittest

from flytekit.models.sagemaker import parameter_ranges


def test_continuous_parameter_range():
    pr = parameter_ranges.ContinuousParameterRange(
        max_value=10, min_value=0.5, scaling_type=parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC
    )

    pr2 = parameter_ranges.ContinuousParameterRange.from_flyte_idl(pr.to_flyte_idl())
    assert pr == pr2
    assert type(pr2.max_value) == float
    assert type(pr2.min_value) == float
    assert pr2.max_value == 10.0
    assert pr2.min_value == 0.5
    assert pr2.scaling_type == parameter_ranges.HyperparameterScalingType.REVERSELOGARITHMIC


def test_integer_parameter_range():
    pr = parameter_ranges.IntegerParameterRange(
        max_value=1, min_value=0, scaling_type=parameter_ranges.HyperparameterScalingType.LOGARITHMIC
    )

    pr2 = parameter_ranges.IntegerParameterRange.from_flyte_idl(pr.to_flyte_idl())
    assert pr == pr2
    assert type(pr2.max_value) == int
    assert type(pr2.min_value) == int
    assert pr2.max_value == 1
    assert pr2.min_value == 0
    assert pr2.scaling_type == parameter_ranges.HyperparameterScalingType.LOGARITHMIC


def test_categorical_parameter_range():
    case = unittest.TestCase()
    pr = parameter_ranges.CategoricalParameterRange(values=["abc", "cat"])

    pr2 = parameter_ranges.CategoricalParameterRange.from_flyte_idl(pr.to_flyte_idl())
    assert pr == pr2
    assert isinstance(pr2.values, list)
    case.assertCountEqual(pr2.values, pr.values)


def test_parameter_ranges():
    pr = parameter_ranges.ParameterRanges(
        {
            "a": parameter_ranges.CategoricalParameterRange(values=["a-1", "a-2"]),
            "b": parameter_ranges.IntegerParameterRange(
                min_value=1, max_value=5, scaling_type=parameter_ranges.HyperparameterScalingType.LINEAR
            ),
            "c": parameter_ranges.ContinuousParameterRange(
                min_value=0.1, max_value=1.0, scaling_type=parameter_ranges.HyperparameterScalingType.LOGARITHMIC
            ),
        },
    )
    pr2 = parameter_ranges.ParameterRanges.from_flyte_idl(pr.to_flyte_idl())
    assert pr == pr2
