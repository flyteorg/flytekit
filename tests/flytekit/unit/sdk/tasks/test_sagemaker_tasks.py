from __future__ import absolute_import
from flytekit.common.tasks.sagemaker.training_job_task import SdkSimpleTrainingJobTask
from flytekit.common.tasks.sagemaker.hpo_job_task import SdkSimpleHyperparameterTuningJobTask
from flytekit.common import constants as _common_constants
from flytekit.common.tasks import task as _sdk_task
from flytekit.models.core import identifier as _identifier
import datetime as _datetime
from flytekit.models.sagemaker.training_job import TrainingJobConfig, AlgorithmSpecification, MetricDefinition
from flytekit.sdk.sagemaker.types import InputMode, AlgorithmName
from google.protobuf.json_format import ParseDict
from flyteidl.plugins.sagemaker.training_job_pb2 import TrainingJobConfig as _pb2_TrainingJobConfig, StoppingCondition as _pb2_StoppingCondition
from flyteidl.plugins.sagemaker.hyperparameter_tuning_job_pb2 import HyperparameterTuningSpecification as _pb2_HPOSpec
from flytekit.sdk import types as _sdk_types
from flytekit.common.tasks.sagemaker import hpo_job_task

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

simple_training_job_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")


def test_simple_training_job_task():
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
    ParseDict(simple_training_job_task.custom['trainingJobConfig'],
              _pb2_TrainingJobConfig)  # fails the test if it cannot be parsed


simple_xgboost_hpo_job_task = hpo_job_task.SdkSimpleHyperparameterTuningJobTask(
    training_job=simple_training_job_task,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
    cache_version='1',
    retries=2,
    cacheable=True,
)

simple_xgboost_hpo_job_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")


def test_simple_hpo_job_task():
    assert isinstance(simple_xgboost_hpo_job_task, SdkSimpleHyperparameterTuningJobTask)
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
    assert simple_xgboost_hpo_job_task.interface.inputs['hyperparameter_tuning_specification'].description == ''
    assert simple_xgboost_hpo_job_task.interface.inputs['hyperparameter_tuning_specification'].type == \
           _sdk_types.Types.Proto(_pb2_HPOSpec).to_flyte_literal_type()
    assert simple_xgboost_hpo_job_task.interface.outputs['model'].description == ''
    assert simple_xgboost_hpo_job_task.interface.outputs['model'].type == \
           _sdk_types.Types.Blob.to_flyte_literal_type()
    assert simple_xgboost_hpo_job_task.type == _common_constants.SdkTaskType.SAGEMAKER_HPO_JOB_TASK

    # Checking if the spec of the TrainingJob is embedded into the custom field
    # of this SdkSimpleHyperparameterTuningJobTask
    assert simple_xgboost_hpo_job_task.to_flyte_idl().custom["trainingJob"] == (
        simple_training_job_task.to_flyte_idl().custom)

    assert simple_xgboost_hpo_job_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert simple_xgboost_hpo_job_task.metadata.discoverable is True
    assert simple_xgboost_hpo_job_task.metadata.discovery_version == '1'
    assert simple_xgboost_hpo_job_task.metadata.retries.retries == 2

    assert simple_xgboost_hpo_job_task.metadata.deprecated_error_message == ''

    """ These are attributes for SdkRunnable. We will need these when supporting CustomTrainingJobTask and CustomHPOJobTask 
    assert simple_xgboost_hpo_job_task.task_module == __name__
    assert simple_xgboost_hpo_job_task._get_container_definition().args[0] == 'pyflyte-execute'
    """
