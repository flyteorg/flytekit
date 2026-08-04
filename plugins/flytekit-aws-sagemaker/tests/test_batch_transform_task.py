import pytest
from flytekitplugins.awssagemaker_batch_transform import (
    SageMakerDescribeTransformJobTask,
    SageMakerStopTransformJobTask,
    SageMakerTransformJobTask,
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


def test_transform_job_task_interface_and_custom():
    task = SageMakerTransformJobTask(
        name="batch_score",
        config={
            "TransformJobName": "score-{idempotence_token}",
            "ModelName": "{inputs.model_name}",
            "TransformInput": {
                "DataSource": {
                    "S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "{inputs.input_data}"}
                },
                "ContentType": "text/csv",
                "SplitType": "Line",
            },
            "TransformOutput": {"S3OutputPath": "{inputs.output_prefix}"},
            "TransformResources": {"InstanceType": "ml.m5.xlarge", "InstanceCount": 1},
        },
        region="us-east-2",
        inputs=kwtypes(model_name=str, input_data=str, output_prefix=str),
    )

    assert len(task.interface.inputs) == 3
    assert len(task.interface.outputs) == 1
    assert "result" in task.interface.outputs

    custom = task.get_custom(_ser_settings())
    assert custom["region"] == "us-east-2"
    assert custom["config"]["TransformJobName"] == "score-{idempotence_token}"


@pytest.mark.parametrize(
    "task_cls,method",
    [
        (SageMakerStopTransformJobTask, "stop_transform_job"),
        (SageMakerDescribeTransformJobTask, "describe_transform_job"),
    ],
)
def test_helper_boto_tasks_use_correct_method(task_cls, method):
    task = task_cls(
        name="helper",
        config={"TransformJobName": "{inputs.transform_job_name}"},
        region="us-east-2",
        inputs=kwtypes(transform_job_name=str),
    )

    custom = task.get_custom(_ser_settings())
    assert custom["service"] == "sagemaker"
    assert custom["method"] == method
    assert custom["region"] == "us-east-2"
