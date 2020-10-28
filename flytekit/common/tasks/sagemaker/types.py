from flytekit.models.sagemaker import hpo_job as _hpo_models
from flytekit.models.sagemaker import parameter_ranges as _parameter_range_models
from flytekit.sdk import types as _sdk_types

HyperparameterTuningJobConfig = _sdk_types.Types.GenericProto(_hpo_models.HyperparameterTuningJobConfig)

ParameterRange = _sdk_types.Types.GenericProto(_parameter_range_models.ParameterRangeOneOf)
