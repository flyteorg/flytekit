import pytest
from flytekitplugins.awssagemaker_hyperparameter_tuning import (
    SageMakerDescribeHyperParameterTuningJobTask,
    SageMakerHyperParameterTuningJobTask,
    SageMakerStopHyperParameterTuningJobTask,
)

from flytekit import kwtypes
from flytekit.configuration import Image, ImageConfig, SerializationSettings


def _ser_settings():
    default_img = Image(name="default", fqn="test", tag="tag")
    return SerializationSettings(
        project="project",
        domain="domain",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )


def test_hyperparameter_tuning_job_task_interface_and_custom():
    task = SageMakerHyperParameterTuningJobTask(
        name="tune_xgb",
        config={
            "HyperParameterTuningJobName": "xgb-tune-{idempotence_token}",
            "HyperParameterTuningJobConfig": {
                "Strategy": "Bayesian",
                "HyperParameterTuningJobObjective": {
                    "Type": "Maximize",
                    "MetricName": "validation:auc",
                },
                "ResourceLimits": {
                    "MaxNumberOfTrainingJobs": 4,
                    "MaxParallelTrainingJobs": 2,
                },
                "ParameterRanges": {
                    "ContinuousParameterRanges": [
                        {"Name": "eta", "MinValue": "0.01", "MaxValue": "0.5"},
                    ],
                },
            },
            "TrainingJobDefinition": {
                "AlgorithmSpecification": {
                    "TrainingImage": "{images.training_image}",
                    "TrainingInputMode": "File",
                },
                "RoleArn": "{inputs.execution_role_arn}",
                "OutputDataConfig": {"S3OutputPath": "{inputs.output_prefix}"},
                "ResourceConfig": {
                    "InstanceType": "ml.m5.large",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 10,
                },
                "StoppingCondition": {"MaxRuntimeInSeconds": 1800},
            },
        },
        region="us-east-2",
        images={"training_image": "1234567890.dkr.ecr.us-east-2.amazonaws.com/xgb:latest"},
        inputs=kwtypes(execution_role_arn=str, output_prefix=str),
    )

    assert len(task.interface.inputs) == 2
    assert len(task.interface.outputs) == 1
    assert "result" in task.interface.outputs

    custom = task.get_custom(_ser_settings())
    assert custom["region"] == "us-east-2"
    assert custom["config"]["HyperParameterTuningJobName"] == "xgb-tune-{idempotence_token}"
    assert custom["images"]["training_image"].endswith("/xgb:latest")


@pytest.mark.parametrize(
    "task_cls,method",
    [
        (SageMakerStopHyperParameterTuningJobTask, "stop_hyper_parameter_tuning_job"),
        (
            SageMakerDescribeHyperParameterTuningJobTask,
            "describe_hyper_parameter_tuning_job",
        ),
    ],
)
def test_helper_boto_tasks_use_correct_method(task_cls, method):
    task = task_cls(
        name="helper",
        config={"HyperParameterTuningJobName": "{inputs.tuning_job_name}"},
        region="us-east-2",
        inputs=kwtypes(tuning_job_name=str),
    )

    custom = task.get_custom(_ser_settings())
    assert custom["service"] == "sagemaker"
    assert custom["method"] == method
    assert custom["region"] == "us-east-2"
