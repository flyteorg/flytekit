from __future__ import absolute_import
import json
import os as _os

from flytekit.sdk.tasks import inputs, outputs
from flytekit.common.tasks.sagemaker.training_job_task import SdkSimpleTrainingJobTask
from flytekit.common.tasks.sagemaker.hpo_job_task import SdkSimpleHPOJobTask
from flytekit.common.tasks.sdk_runnable import SdkRunnableTask
from flytekit.sdk.types import Types
from flytekit.common import constants as _common_constants
# from flytekit.common.tasks import sdk_runnable as _sdk_runnable
# from flytekit.common.tasks import sagemaker_task as _sagemaker_task
from flytekit.common.tasks import task as _sdk_task
from flytekit.models import types as _type_models
from flytekit.models.core import identifier as _identifier
import datetime as _datetime
from flytekit.models.sagemaker.training_job import TrainingJobConfig, AlgorithmSpecification, MetricDefinition, StoppingCondition
from flytekit.sdk.sagemaker.types import InputMode, AlgorithmName
from flytekit.models import literals as _literals, types as _idl_types, \
    task as _task_model
from flytekit.models.core import types as _core_types
from google.protobuf.json_format import ParseDict
from flyteidl.plugins.sagemaker.training_job_pb2 import TrainingJobConfig as _pb2_TrainingJobConfig, StoppingCondition as _pb2_StoppingCondition
from flyteidl.plugins.sagemaker.parameter_ranges_pb2 import ParameterRanges as _pb2_ParameterRanges
from flyteidl.plugins.sagemaker.hpo_job_pb2 import HPOJobConfig as _pb2_HPOJobConfig
from flytekit.sdk import types as _sdk_types
from flytekit.sdk.sagemaker import types as _sdk_sagemaker_types
from flytekit.common.tasks.sagemaker import hpo_job_task
from flytekit.configuration import TemporaryConfiguration as _TemporaryConfiguration
from flytekit.models.sagemaker.training_job import StoppingCondition
from flytekit.models.sagemaker.hpo_job import HPOJobConfig, HyperparameterTuningObjective
from flytekit.models.sagemaker.parameter_ranges import ParameterRanges, CategoricalParameterRange, ContinuousParameterRange, IntegerParameterRange

example_hyperparams = {
    "base_score": "0.5",
    "booster": "gbtree",
    "csv_weights": "0",
    "dsplit": "row",
    "grow_policy": "depthwise",
    "lambda_bias": "0.0",
    "max_bin": "256",
    "max_leaves": "0",
    "normalize_type": "tree",
    "objective": "reg:linear",
    "one_drop": "0",
    "prob_buffer_row": "1.0",
    "process_type": "default",
    "rate_drop": "0.0",
    "refresh_leaf": "1",
    "sample_type": "uniform",
    "scale_pos_weight": "1.0",
    "silent": "0",
    "sketch_eps": "0.03",
    "skip_drop": "0.0",
    "tree_method": "auto",
    "tweedie_variance_power": "1.5",
    "updater": "grow_colmaker,prune",
}

simple_training_job_task = SdkSimpleTrainingJobTask(
    training_job_config=TrainingJobConfig(
        instance_type="ml.m4.xlarge",
        instance_count=1,
        volume_size_in_gb=25,
    ),
    algorithm_specification=AlgorithmSpecification(
        input_mode=InputMode.FILE,
        algorithm_name=AlgorithmName.XGBOOST,
        algorithm_version="0.72",
        metric_definitions=[MetricDefinition(name="Minimize", regex="validation:error")]
    ),
)




def test_simple_training_job_task():
    with _TemporaryConfiguration(
            _os.path.join(_os.path.dirname(__file__), 'unit.config'),
            internal_overrides={'image': 'unit_image'}
    ):
        train_task_exec = simple_training_job_task(
            train='s3://my-bucket/training.csv',
            validation='s3://my-bucket/validation.csv',
            static_hyperparameters=example_hyperparams,
            stopping_condition=StoppingCondition(
                max_runtime_in_seconds=43200,
                max_wait_time_in_seconds=43200,
            ).to_flyte_idl(),
        )
    # verify the custom field doesn't change

    train_task_exec._id = _identifier.Identifier(
        _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")

    assert isinstance(simple_training_job_task, SdkSimpleTrainingJobTask)
    assert isinstance(simple_training_job_task, _sdk_task.SdkTask)
    assert simple_training_job_task.interface.inputs['train'].description == ''
    assert simple_training_job_task.interface.inputs['train'].type == \
        _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    assert simple_training_job_task.interface.inputs['validation'].description == ''
    assert simple_training_job_task.interface.inputs['validation'].type == \
        _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    assert simple_training_job_task.interface.inputs['static_hyperparameters'].description == ''
    assert simple_training_job_task.interface.inputs['static_hyperparameters'].type == \
        _sdk_types.Types.Generic.to_flyte_literal_type()
    assert simple_training_job_task.interface.inputs['stopping_condition'].type == \
        _sdk_types.Types.Proto(_pb2_StoppingCondition).to_flyte_literal_type()
    assert simple_training_job_task.interface.outputs['model'].description == ''
    assert simple_training_job_task.interface.outputs['model'].type == \
        _sdk_types.Types.Blob.to_flyte_literal_type()
    assert simple_training_job_task.type == _common_constants.SdkTaskType.SAGEMAKER_TRAINING_JOB_TASK
    assert simple_training_job_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert simple_training_job_task.metadata.deprecated_error_message == ''
    assert simple_training_job_task.metadata.discoverable is False
    assert simple_training_job_task.metadata.discovery_version == ''
    assert simple_training_job_task.metadata.retries.retries == 0
    ParseDict(simple_training_job_task.custom['trainingJobConfig'], _pb2_TrainingJobConfig)  # fails the test if it cannot be parsed

    pb2 = simple_training_job_task.to_flyte_idl()
    print(pb2)


simple_xgboost_hpo_job_task = hpo_job_task.SdkSimpleHPOJobTask(
    training_job=simple_training_job_task,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
    cache_version='1',
    retries=2,
    cacheable=True,
)

hpo_task_exec = simple_xgboost_hpo_job_task(
    train='s3://my-bucket/hpo.csv',
    validation='s3://my-bucket/hpo.csv',
    static_hyperparameters=example_hyperparams,
    stopping_condition=StoppingCondition(
        max_runtime_in_seconds=43200,
        max_wait_time_in_seconds=43200,
    ).to_flyte_idl(),
    hpo_job_config=HPOJobConfig(
        hyperparameter_ranges=ParameterRanges(
            parameter_range_map={
                "max_depth": IntegerParameterRange(min_value=5, max_value=7,
                                                   scaling_type=_sdk_sagemaker_types.HyperparameterScalingType.LINEAR),
            }
        ),
        tuning_strategy=_sdk_sagemaker_types.HyperparameterTuningStrategy.BAYESIAN,
        tuning_objective=HyperparameterTuningObjective(
            objective_type=_sdk_sagemaker_types.HyperparameterTuningObjectiveType.MINIMIZE,
            metric_name="validation:error",
        ),
        training_job_early_stopping_type=_sdk_sagemaker_types.TrainingJobEarlyStoppingType.AUTO
    ).to_flyte_idl(),
)

simple_xgboost_hpo_job_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")

def test_simple_hpo_job_task():
    assert isinstance(simple_xgboost_hpo_job_task, SdkSimpleHPOJobTask)
    assert isinstance(simple_xgboost_hpo_job_task, _sdk_task.SdkTask)
    # Checking if the input of the underlying SdkTrainingJobTask has been embedded
    assert simple_training_job_task.interface.inputs['train'].description == ''
    assert simple_training_job_task.interface.inputs['train'].type == \
        _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    assert simple_training_job_task.interface.inputs['validation'].description == ''
    assert simple_training_job_task.interface.inputs['validation'].type == \
        _sdk_types.Types.MultiPartCSV.to_flyte_literal_type()
    assert simple_training_job_task.interface.inputs['static_hyperparameters'].description == ''
    assert simple_training_job_task.interface.inputs['static_hyperparameters'].type == \
        _sdk_types.Types.Generic.to_flyte_literal_type()
    assert simple_training_job_task.interface.inputs['stopping_condition'].type == \
        _sdk_types.Types.Proto(_pb2_StoppingCondition).to_flyte_literal_type()

    # Checking if the hpo-specific input is defined
    assert simple_xgboost_hpo_job_task.interface.inputs['hpo_job_config'].description == ''
    assert simple_xgboost_hpo_job_task.interface.inputs['hpo_job_config'].type == \
           _sdk_types.Types.Proto(_pb2_HPOJobConfig).to_flyte_literal_type()
    assert simple_xgboost_hpo_job_task.interface.outputs['model'].description == ''
    assert simple_xgboost_hpo_job_task.interface.outputs['model'].type == \
           _sdk_types.Types.Blob.to_flyte_literal_type()
    assert simple_xgboost_hpo_job_task.type == _common_constants.SdkTaskType.SAGEMAKER_HPO_JOB_TASK

    # Checking if the spec of the TrainingJob is embedded into the custom field of this SdkSimpleHPOJobTask
    assert simple_xgboost_hpo_job_task.to_flyte_idl().custom["trainingJob"] == \
           simple_training_job_task.to_flyte_idl().custom

    assert simple_xgboost_hpo_job_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert simple_xgboost_hpo_job_task.metadata.discoverable is True
    assert simple_xgboost_hpo_job_task.metadata.discovery_version == '1'
    assert simple_xgboost_hpo_job_task.metadata.retries.retries == 2
    """
    assert simple_xgboost_hpo_job_task.task_module == __name__
    assert simple_xgboost_hpo_job_task.metadata.deprecated_error_message == ''
    assert simple_xgboost_hpo_job_task._get_container_definition().args[0] == 'pyflyte-execute'

    pb2 = simple_xgboost_hpo_job_task.to_flyte_idl()
    assert pb2.custom['hpojobConf']['C'] == 'D'
    """