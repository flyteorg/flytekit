import os as _os

from flytekit import configuration as _configuration
from flytekit.common.tasks.sagemaker import hpo_job_task
from flytekit.common.tasks.sagemaker.built_in_training_job_task import SdkBuiltinAlgorithmTrainingJobTask
from flytekit.common.tasks.sagemaker.types import HyperparameterTuningJobConfig
from flytekit.models.sagemaker.hpo_job import HyperparameterTuningJobConfig as _HyperparameterTuningJobConfig
from flytekit.models.sagemaker.hpo_job import (
    HyperparameterTuningObjective,
    HyperparameterTuningObjectiveType,
    HyperparameterTuningStrategy,
    TrainingJobEarlyStoppingType,
)
from flytekit.models.sagemaker.parameter_ranges import (
    ContinuousParameterRange,
    HyperparameterScalingType,
    IntegerParameterRange,
)
from flytekit.models.sagemaker.training_job import (
    AlgorithmName,
    AlgorithmSpecification,
    InputContentType,
    InputMode,
    TrainingJobResourceConfig,
    MetricDefinition,
)
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import Input, workflow_class
from flytekit.sdk.tasks import inputs, outputs
from flytekit.sdk.sagemaker.task import custom_training_job_task

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

builtin_algorithm_training_job_task2 = SdkBuiltinAlgorithmTrainingJobTask(
    training_job_resource_config=TrainingJobResourceConfig(
        instance_type="ml.m4.xlarge", instance_count=1, volume_size_in_gb=25,
    ),
    algorithm_specification=AlgorithmSpecification(
        input_mode=InputMode.FILE,
        input_content_type=InputContentType.TEXT_CSV,
        algorithm_name=AlgorithmName.XGBOOST,
        algorithm_version="0.72",
    ),
)

simple_xgboost_hpo_job_task = hpo_job_task.SdkSimpleHyperparameterTuningJobTask(
    training_job=builtin_algorithm_training_job_task2,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
    cache_version="1",
    retries=2,
    cacheable=True,
    tunable_parameters=["num_round", "max_depth", "gamma"],
)


@workflow_class
class SageMakerHPOwithBuiltinAlgorithmTraining(object):
    train_dataset = Input(Types.MultiPartCSV, default="s3://somelocation")
    validation_dataset = Input(Types.MultiPartCSV, default="s3://somelocation")
    static_hyperparameters = Input(Types.Generic, default=example_hyperparams)
    hyperparameter_tuning_job_config = Input(
        HyperparameterTuningJobConfig,
        default=_HyperparameterTuningJobConfig(
            tuning_strategy=HyperparameterTuningStrategy.BAYESIAN,
            tuning_objective=HyperparameterTuningObjective(
                objective_type=HyperparameterTuningObjectiveType.MINIMIZE, metric_name="validation:error",
            ),
            training_job_early_stopping_type=TrainingJobEarlyStoppingType.AUTO,
        ),
    )

    a = simple_xgboost_hpo_job_task(
        train=train_dataset,
        validation=validation_dataset,
        static_hyperparameters=static_hyperparameters,
        hyperparameter_tuning_job_config=hyperparameter_tuning_job_config,
        num_round=IntegerParameterRange(min_value=2, max_value=8, scaling_type=HyperparameterScalingType.LINEAR),
        max_depth=IntegerParameterRange(min_value=5, max_value=7, scaling_type=HyperparameterScalingType.LINEAR),
        gamma=ContinuousParameterRange(min_value=0.0, max_value=0.3, scaling_type=HyperparameterScalingType.LINEAR),
    )


sagemaker_hpo_lp = SageMakerHPOwithBuiltinAlgorithmTraining.create_launch_plan()

with _configuration.TemporaryConfiguration(
    _os.path.join(_os.path.dirname(_os.path.realpath(__file__)), "../../common/configs/local.config",),
    internal_overrides={"image": "myflyteimage:v123", "project": "myflyteproject", "domain": "development"},
):
    print("Printing WF definition")
    print(SageMakerHPOwithBuiltinAlgorithmTraining)

    print("Printing LP definition")
    print(sagemaker_hpo_lp)


@inputs(input_1=Types.Integer)
@outputs(model=Types.Blob)
@custom_training_job_task(
    training_job_resource_config=TrainingJobResourceConfig(
        instance_type="ml.m4.xlarge", instance_count=2, volume_size_in_gb=25,
    ),
    algorithm_specification=AlgorithmSpecification(
        input_mode=InputMode.FILE,
        input_content_type=InputContentType.TEXT_CSV,
        metric_definitions=[MetricDefinition(name="Validation error", regex="validation:error")],
    ),
)
def my_distributed_task_with_valid_dist_training_context(wf_params, input_1, model):
    if not wf_params.distributed_training_context:
        raise ValueError


hpo_with_custom_training = hpo_job_task.SdkSimpleHyperparameterTuningJobTask(
    tunable_parameters=["input_1"],
    training_job=my_distributed_task_with_valid_dist_training_context,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
)


@workflow_class
class SageMakerHPOWithCustomTraining(object):
    train_dataset = Input(Types.MultiPartCSV, default="s3://somelocation")
    validation_dataset = Input(Types.MultiPartCSV, default="s3://somelocation")
    static_hyperparameters = Input(Types.Generic, default=example_hyperparams)
    hyperparameter_tuning_job_config = Input(
        HyperparameterTuningJobConfig,
        default=_HyperparameterTuningJobConfig(
            tuning_strategy=HyperparameterTuningStrategy.BAYESIAN,
            tuning_objective=HyperparameterTuningObjective(
                objective_type=HyperparameterTuningObjectiveType.MINIMIZE, metric_name="validation:error",
            ),
            training_job_early_stopping_type=TrainingJobEarlyStoppingType.AUTO,
        ),
    )

    a = hpo_with_custom_training(
        train=train_dataset,
        validation=validation_dataset,
        static_hyperparameters=static_hyperparameters,
        hyperparameter_tuning_job_config=hyperparameter_tuning_job_config,
        input_1=IntegerParameterRange(min_value=2, max_value=8, scaling_type=HyperparameterScalingType.LINEAR),
    )
