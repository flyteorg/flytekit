from __future__ import absolute_import
from flytekit.sdk.tasks import inputs, outputs
from flytekit.common.tasks.sagemaker.training_job_task import SdkSimpleTrainingJobTask
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
    task_type="sagemaker_trianing_job_task",
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

run_train_task = simple_training_job_task(
    train='s3://my-bucket/training.csv',
    validation='s3://my-bucket/validation.csv',
    static_hyperparameters=example_hyperparams,
    stopping_condition=StoppingCondition(
        max_runtime_in_seconds=43200,
        max_wait_time_in_seconds=43200,
    ).to_flyte_idl(),
)

run_train_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")


def test_simple_training_job_task():
    t = type(run_train_task)
    # assert isinstance(run_train_task, SdkSimpleTrainingJobTask)
    assert isinstance(run_train_task, _sdk_task.SdkTask)
    assert run_train_task.interface.inputs['train'].description == ''
    assert run_train_task.interface.inputs['train'].type == \
        _type_models.LiteralType(schema=_type_models.SchemaType())
#    assert default_trainingjob_task.interface.outputs['model'].description == ''
#    assert default_trainingjob_task.interface.outputs['model'].type == \
#        _type_models.LiteralType(blob=_type_models.LiteralType.blob)
#    assert simple_training_job_task.type == _common_constants.SdkTaskType.SAGEMAKER_TRAININGJOB_TASK
#    assert simple_training_job_task.metadata.timeout == _datetime.timedelta(seconds=0)
#    assert simple_training_job_task.metadata.deprecated_error_message == ''
    assert simple_training_job_task.metadata.discoverable is False
    assert simple_training_job_task.metadata.discovery_version == ''
    assert simple_training_job_task.metadata.retries.retries == 0
#    assert len(simple_training_job_task.container.resources.limits) == 0
#    assert len(simple_training_job_task.container.resources.requests) == 0
    assert isinstance(simple_training_job_task.custom['training_job_config'], TrainingJobConfig)
#    assert simple_training_job_task._get_container_definition().args[0] == 'pyflyte-execute'

    pb2 = simple_training_job_task.to_flyte_idl()
    print(pb2)

"""
@inputs(
    hyperparameter_ranges=Types.Generic,
)
@outputs(
    model=Types.Blob
)
@hpojob_task(hpojob_conf={'C': 'D'})
def default_hpojob_task(wf_params, hjconf, hyperparameter_ranges, model):
    pass


default_hpojob_task._id = _identifier.Identifier(
    _identifier.ResourceType.TASK, "my_project", "my_domain", "my_name", "my_version")


def test_simple_training_job_task():
    assert isinstance(default_hpojob_task, _sagemaker_task.SdkHPOJobTask)
    assert isinstance(default_hpojob_task, _sdk_runnable.SdkRunnableTask)
    assert default_hpojob_task.interface.inputs['hyperparameter_ranges'].description == ''
    assert default_hpojob_task.interface.inputs['hyperparameter_ranges'].type == \
           _type_models.LiteralType(schema=_type_models.SchemaType)
    assert default_hpojob_task.interface.outputs['model'].description == ''
    assert default_hpojob_task.interface.outputs['model'].type == \
           _type_models.LiteralType(blob=_type_models.LiteralType.blob)
    assert default_hpojob_task.type == _common_constants.SdkTaskType.SAGEMAKER_HPOJOB_TASK
    assert default_hpojob_task.task_function_name == 'default_hpojob_task'
    assert default_hpojob_task.task_module == __name__
    assert default_hpojob_task.metadata.timeout == _datetime.timedelta(seconds=0)
    assert default_hpojob_task.metadata.deprecated_error_message == ''
    assert default_hpojob_task.metadata.discoverable is False
    assert default_hpojob_task.metadata.discovery_version == ''
    assert default_hpojob_task.metadata.retries.retries == 0
    assert len(default_hpojob_task.container.resources.limits) == 0
    assert len(default_hpojob_task.container.resources.requests) == 0
    assert default_hpojob_task.custom['hpojobConf']['C'] == 'D'
    assert default_hpojob_task._get_container_definition().args[0] == 'pyflyte-execute'

    pb2 = default_hpojob_task.to_flyte_idl()
    assert pb2.custom['hpojobConf']['C'] == 'D'
"""