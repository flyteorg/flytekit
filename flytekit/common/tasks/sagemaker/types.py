from typing import Union

from flyteidl.plugins.sagemaker import hyperparameter_tuning_job_pb2 as _pb2_hpo_job, \
    parameter_ranges_pb2 as _pb2_parameter_ranges
from flyteidl.plugins.sagemaker import parameter_ranges_pb2 as _idl_parameter_ranges

from flytekit.models.sagemaker.parameter_ranges import IntegerParameterRange, ContinuousParameterRange, \
    CategoricalParameterRange
from flytekit.sdk import types as _sdk_types

HyperparameterTuningJobConfig = _sdk_types.Types.GenericProto(_pb2_hpo_job.HyperparameterTuningJobConfig)


class ParameterRange(_sdk_types.Types.GenericProto(_pb2_parameter_ranges.ParameterRangeOneOf)):
    def __init__(self, param: Union[IntegerParameterRange, ContinuousParameterRange, CategoricalParameterRange]):
        """
        Initializes a new ParameterRange
        :param Union[IntegerParameterRange, ContinuousParameterRange, CategoricalParameterRange] param: One of the
                supported parameter ranges.
        """
        if isinstance(param, IntegerParameterRange):
            super().__init__(pb_object=_idl_parameter_ranges.ParameterRangeOneOf(
                integer_parameter_range=param.to_flyte_idl()))
        elif isinstance(param, ContinuousParameterRange):
            super().__init__(pb_object=_idl_parameter_ranges.ParameterRangeOneOf(
                continuous_parameter_range=param.to_flyte_idl()))
        elif isinstance(param, CategoricalParameterRange):
            super().__init__(pb_object=_idl_parameter_ranges.ParameterRangeOneOf(
                categorical_parameter_range=param.to_flyte_idl()))
