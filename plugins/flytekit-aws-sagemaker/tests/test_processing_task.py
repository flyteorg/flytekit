import pytest
from flytekitplugins.awssagemaker_processing import (
    SageMakerDescribeProcessingJobTask,
    SageMakerProcessingJobTask,
    SageMakerStopProcessingJobTask,
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


def test_processing_job_task_interface_and_custom():
    task = SageMakerProcessingJobTask(
        name="preprocess",
        config={
            "ProcessingJobName": "prep-{idempotence_token}",
            "AppSpecification": {
                "ImageUri": "{images.processing_image}",
                "ContainerEntrypoint": ["python3", "/opt/ml/processing/preprocess.py"],
            },
            "RoleArn": "{inputs.execution_role_arn}",
            "ProcessingResources": {
                "ClusterConfig": {
                    "InstanceType": "ml.m5.xlarge",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 30,
                }
            },
            "ProcessingOutputConfig": {
                "Outputs": [
                    {
                        "OutputName": "train",
                        "S3Output": {
                            "S3Uri": "{inputs.output_prefix}",
                            "LocalPath": "/opt/ml/processing/output",
                            "S3UploadMode": "EndOfJob",
                        },
                    }
                ]
            },
            "StoppingCondition": {"MaxRuntimeInSeconds": 3600},
        },
        region="us-east-2",
        images={"processing_image": "1234567890.dkr.ecr.us-east-2.amazonaws.com/sklearn:latest"},
        inputs=kwtypes(execution_role_arn=str, output_prefix=str),
    )

    assert len(task.interface.inputs) == 2
    assert len(task.interface.outputs) == 1
    assert "result" in task.interface.outputs

    custom = task.get_custom(_ser_settings())
    assert custom["region"] == "us-east-2"
    assert custom["config"]["ProcessingJobName"] == "prep-{idempotence_token}"
    assert custom["images"]["processing_image"].endswith("/sklearn:latest")


@pytest.mark.parametrize(
    "task_cls,method",
    [
        (SageMakerStopProcessingJobTask, "stop_processing_job"),
        (SageMakerDescribeProcessingJobTask, "describe_processing_job"),
    ],
)
def test_helper_boto_tasks_use_correct_method(task_cls, method):
    task = task_cls(
        name="helper",
        config={"ProcessingJobName": "{inputs.processing_job_name}"},
        region="us-east-2",
        inputs=kwtypes(processing_job_name=str),
    )

    custom = task.get_custom(_ser_settings())
    assert custom["service"] == "sagemaker"
    assert custom["method"] == method
    assert custom["region"] == "us-east-2"
