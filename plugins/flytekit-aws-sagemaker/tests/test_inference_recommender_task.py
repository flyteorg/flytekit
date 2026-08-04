import pytest
from flytekitplugins.awssagemaker_inference_recommender import (
    SageMakerDescribeInferenceRecommenderJobTask,
    SageMakerInferenceRecommenderJobTask,
    SageMakerStopInferenceRecommenderJobTask,
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


def test_inference_recommender_job_task_interface_and_custom():
    task = SageMakerInferenceRecommenderJobTask(
        name="recommend",
        config={
            "JobName": "rec-{idempotence_token}",
            "JobType": "Default",
            "RoleArn": "{inputs.role_arn}",
            # Minimal valid Default-job InputConfig — AWS rejects
            # JobDurationInSeconds / TrafficPattern / ResourceLimit /
            # EndpointConfigurations / top-level StoppingConditions for Default.
            "InputConfig": {
                "ModelPackageVersionArn": "{inputs.model_package_version_arn}",
            },
        },
        region="us-east-2",
        inputs=kwtypes(role_arn=str, model_package_version_arn=str),
    )

    assert len(task.interface.inputs) == 2
    assert len(task.interface.outputs) == 1
    assert "result" in task.interface.outputs

    custom = task.get_custom(_ser_settings())
    assert custom["region"] == "us-east-2"
    assert custom["config"]["JobName"] == "rec-{idempotence_token}"
    assert custom["config"]["JobType"] == "Default"


@pytest.mark.parametrize(
    "task_cls,method",
    [
        (SageMakerStopInferenceRecommenderJobTask, "stop_inference_recommendations_job"),
        (
            SageMakerDescribeInferenceRecommenderJobTask,
            "describe_inference_recommendations_job",
        ),
    ],
)
def test_helper_boto_tasks_use_correct_method(task_cls, method):
    task = task_cls(
        name="helper",
        config={"JobName": "{inputs.job_name}"},
        region="us-east-2",
        inputs=kwtypes(job_name=str),
    )

    custom = task.get_custom(_ser_settings())
    assert custom["service"] == "sagemaker"
    assert custom["method"] == method
    assert custom["region"] == "us-east-2"
