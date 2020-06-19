from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
from operator import add

from six.moves import range

from flytekit.sdk.tasks.sagemaker.trainingjob import SdkSimpleTrainingJobTask, DataChannel, TrainingJobInputMode, TrainingJobAlgorithmName
from flytekit.sdk.tasks.sagemaker.hpojob import SdkSimpleHPOJobTask, HPOJobTuningStrategy, HPOJobConfig, HPOJobObjective
from flytekit.sdk.tasks.sagemaker.parameter_range import IntegerParameterRange, ContinuousParameterRange, CategoricalParameterRange
from flytekit.sdk.tasks.sagemaker import trainingjob_task
from flytekit.sdk.tasks.sagemaker.instances import InstanceType
from flytekit.sdk.tasks import inputs, outputs, python_task
from flytekit.sdk.types import Types
from flytekit.sdk.workflow import workflow_class, Input, Output
import xgboost as xgb

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

simple_xgboost_trainingjob_task = SdkSimpleTrainingJobTask(
    # The problem is that SageMaker currently doesn't support fractional resources
    # (that is, you have to be billed for the entire instance even though your program
    # uses only a part of it).
    #
    # This aforementioned billing policy of SageMaker conflicts with the current semantic
    # in flytekit around resources. So If we allow users to specify cpu_limit and memory_
    # limit for SageMaker tasks, users might be misled and found that they are "overcharged"
    # for the resource they didn't claim.
    #
    # Exposing InstanceType and InstanceCount to the users seems to be the most viable workaround
    # I can think of at the moment to address the above issue.
    trainingjob_conf={
        "InstanceType": InstanceType.ML_M4_XLARGE,
        "InstanceCount": 1,
        "VolumeSizeInGB": 25,
    },
    algorithm_specification={
        "TrainingInputMode": TrainingJobInputMode.FILE,
        "AlgorithmName": TrainingJobAlgorithmName.XGBOOST,
        "Version": "0.72",
    },
    cache_version='1',
    cachable=True,
)

simple_hpojob_wrapping_around_simple_xgboost_trainingjob_task = SdkSimpleHPOJobTask(
    trainingjob_task=simple_xgboost_trainingjob_task,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
    cache_version='1',
    retries=2,
    cachable=True,
)


def calculate_hyperparameter_max_range(a, b):
    return a + b

def sample_eval_function(y_predicted, y_true):
    assert(y_predicted.shape[0] > 0)
    assert(y_predicted.shape == y_true.shape)
    err_count = 0
    for i in enumerate(y_predicted):
        if y_predicted[i] != y_true[i]:
            err_count += 1

    return "err rate", err_count/len(y_predicted)


@inputs(
    custom_input1=Types.Integer,
    custom_input2=Types.Integer,
)
@outputs(
    custom_output1=Types.Blob,
)
@trainingjob_task(
    trainingjob_conf={
        "InstanceType": "ml.m4.xlarge",
        "InstanceCount": 1,
        "VolumeSizeInGB": 25,
    },
    algorithm_specification={
        "TrainingInputMode": TrainingJobInputMode.FILE,
        "AlgorithmName": TrainingJobAlgorithmName.CUSTOM,
    },
    cache_version='1',
    retries=2,
    cachable=True
)
def custom_trainingjob_task(
        wf_params,
        train,
        validation,
        static_hyperparameters,
        stopping_condition,
        custom_input1,
        custom_input2,
        model,
        custom_output1,
    ):

    with train as reader:
        train_df = reader.read(concat=True)
        dtrain_x = xgb.DMatrix(train_df[:-1])
        dtrain_y = xgb.DMatrix(train_df[-1])
    with validation as reader:
        validation_df = reader.read(concat=True)
        dvalidation_x = xgb.DMatrix(validation_df[:-1])
        dvalidation_y = xgb.DMatrix(validation_df[-1])

    my_model = xgb.XGBModel(**static_hyperparameters)

    my_model.fit(dtrain_x,
                 dtrain_y,
                 eval_set=[(dvalidation_x, dvalidation_y)],
                 eval_metric=sample_eval_function)

    model.set(my_model)
    custom_output1.set(my_model.evals_result())


simple_hpojob_wrapping_around_custom_xgboost_trainingjob_task = SdkSimpleHPOJobTask(
    trainingjob_task=custom_trainingjob_task,
    max_number_of_training_jobs=10,
    max_parallel_training_jobs=5,
    cache_version='1',
    retries=2,
    cachable=True,
)


@workflow_class
class SageMakerSimpleWorkflow(object):
    a = Input(Types.Integer, required=True, help="Test integer with no default")
    b = Input(Types.Integer, required=False, default="1", help="Test integer with default")
    custom_hpojob_config = Input(Types.Generic, required=True, help="Allowing specifying hpojob config at launchtime")

    my_simple_xgboost_trainingjob_task = simple_xgboost_trainingjob_task(
        train='s3://my-bucket/training.csv',
        validation='s3://my-bucket/validation.csv',
        static_hyperparameters=example_hyperparams,
        stopping_condition={
            "MaxRuntimeInSeconds": 43200,
            "MaxWaitTimeInSeconds": 43200
        },
    )

    # Note that both this task wraps around the simple_xgboost_trainingjob_task as well
    my_simple_hpojob_task = simple_hpojob_wrapping_around_simple_xgboost_trainingjob_task(
        # This input comes from simple_xgboost_trainingjob_task
        train='s3://my-bucket/training.csv',
        # This input comes from simple_xgboost_trainingjob_task
        validation='s3://my-bucket/validation.csv',
        # This input comes from simple_xgboost_trainingjob_task
        static_hyperparameters=example_hyperparams,
        # This input comes from simple_xgboost_trainingjob_task
        stopping_condition={
            "MaxRuntimeInSeconds": 43200,
            "MaxWaitTimeInSeconds": 43200
        },

        # This is the extra input needed by this hpojob wrapper
        hpojob_config=HPOJobConfig(
            hyperparameter_ranges={   # hyperparameters included here will automatically override the static counterpart
                # https://docs.aws.amazon.com/sagemaker/latest/dg/xgboost_hyperparameters.html
                "num_rounds": IntegerParameterRange(min_value="1", max_value="100", scaling_type="LOGARITHMIC"),
                "max_leaves": IntegerParameterRange(min_value="0", max_value="5", scaling_type="LINEAR"),
                "sketch_eps": ContinuousParameterRange(min_value="0.01", max_value="0.05", scaling_type="AUTO"),
                "tree_method": CategoricalParameterRange(values=["hist", "exact"]) # TODO: sagemaker doesn't accept gpu_hist. Figure out how to do gpu_hist
            },
            hyperparameter_tuning_strategy=HPOJobTuningStrategy.BAYESIAN,
            hyperparameter_tuning_objective=HPOJobObjective(type="MINIMIZE", metric_name="validation:error"),
            trainingjob_early_stopping_type="AUTO",
        ),
    )

    my_custom_trainingjob_task = custom_trainingjob_task(
        custom_input1=a,
        custom_input2=b,
        train='s3://my-bucket/training.csv',
        validation='s3://my-bucket/validation.csv',
        static_hyperparameters=example_hyperparams,
    )

    # Note that this job wraps around custom_trainingjob_task
    my_custom_task = simple_hpojob_wrapping_around_custom_xgboost_trainingjob_task(
        # This input comes from custom_trainingjob_task
        custom_input1=a,
        # This input comes from custom_trainingjob_task
        custom_input2=b,
        # This input comes from custom_trainingjob_task
        train='s3://my-bucket/training.csv',
        # This input comes from custom_trainingjob_task
        validation='s3://my-bucket/validation.csv',
        # This input comes from custom_trainingjob_task
        static_hyperparameters=example_hyperparams,

        # This is an extra output needed by the hpojob wrapper
        hpojob_config=custom_hpojob_config
    )

    simple_model = Output(my_simple_hpojob_task.outputs.model, sdk_type=Types.Blob)
    custom_model = Output(my_custom_task.outputs.model, sdk_type=Types.Blob)
