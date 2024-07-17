from flytekitplugins.awssagemaker_inference import BotoConfig, BotoTask

from flytekit import kwtypes
from flytekit.configuration import Image, ImageConfig, SerializationSettings


def test_boto_task_and_config():
    boto_task = BotoTask(
        name="boto_task",
        task_config=BotoConfig(
            service="sagemaker",
            method="create_model",
            config={
                "ModelName": "{inputs.model_name}",
                "PrimaryContainer": {
                    "Image": "{images.deployment_image}",
                    "ModelDataUrl": "{inputs.model_data_url}",
                },
                "ExecutionRoleArn": "{inputs.execution_role_arn}",
            },
            region="us-east-2",
            images={
                "deployment_image": "1234567890.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost"
            },
        ),
        inputs=kwtypes(model_name=str, model_data_url=str, execution_role_arn=str),
    )

    assert len(boto_task.interface.inputs) == 3
    assert len(boto_task.interface.outputs) == 2

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="project",
        domain="domain",
        version="123",
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        env={},
    )

    retrieved_setttings = boto_task.get_custom(serialization_settings)

    assert retrieved_setttings["service"] == "sagemaker"
    assert retrieved_setttings["config"] == {
        "ModelName": "{inputs.model_name}",
        "PrimaryContainer": {
            "Image": "{images.deployment_image}",
            "ModelDataUrl": "{inputs.model_data_url}",
        },
        "ExecutionRoleArn": "{inputs.execution_role_arn}",
    }
    assert retrieved_setttings["region"] == "us-east-2"
    assert retrieved_setttings["method"] == "create_model"
    assert (
        retrieved_setttings["images"]["deployment_image"]
        == "1234567890.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost"
    )
