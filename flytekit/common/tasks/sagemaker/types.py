from flyteidl.plugins.sagemaker import hyperparameter_tuning_job_pb2 as _pb2_hpo_job
from flyteidl.plugins.sagemaker import parameter_ranges_pb2 as _pb2_parameter_ranges

from flytekit.sdk import types as _sdk_types

HyperparameterTuningJobConfig = _sdk_types.Types.GenericProto(_pb2_hpo_job.HyperparameterTuningJobConfig)

ParameterRange = _sdk_types.Types.GenericProto(_pb2_parameter_ranges.ParameterRangeOneOf)
