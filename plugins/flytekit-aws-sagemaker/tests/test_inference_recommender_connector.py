from datetime import datetime, timedelta
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException
from flytekitplugins.awssagemaker_inference_recommender.connector import (
    SageMakerInferenceRecommenderJobMetadata,
)

from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

idempotence_token = "74443947857331f7"

REGION = "us-east-2"
JOB_ARN = (
    "arn:aws:sagemaker:us-east-2:1234567890:inference-recommendations-job/"
    "rec-74443947857331f7"
)


def _task_config():
    return {
        "config": {
            "JobName": "rec-{idempotence_token}",
            "JobType": "Default",
            "JobDescription": "Smoke recommendations for ranker-prod",
            "RoleArn": "{inputs.role_arn}",
            # Default-job InputConfig allows only ModelPackageVersionArn (or
            # ModelName + ContainerConfig). JobDurationInSeconds /
            # TrafficPattern / ResourceLimit / EndpointConfigurations and the
            # top-level StoppingConditions are all Advanced-only — AWS rejects
            # them with a ValidationException if set here.
            "InputConfig": {
                "ModelPackageVersionArn": "{inputs.model_package_version_arn}",
            },
        },
        "region": REGION,
    }


def _task_template():
    task_id = Identifier(
        resource_type=ResourceType.TASK,
        project="project",
        domain="domain",
        name="name",
        version="version",
    )
    task_metadata = TaskMetadata(
        discoverable=True,
        runtime=RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timeout=timedelta(days=1),
        retries=literals.RetryStrategy(3),
        interruptible=True,
        discovery_version="0.1.1b0",
        deprecated_error_message="This is deprecated!",
        cache_serializable=True,
        pod_template_name="A",
        cache_ignore_input_vars=(),
    )
    return TaskTemplate(
        id=task_id,
        custom=_task_config(),
        metadata=task_metadata,
        interface=None,
        type="sagemaker-inference-recommender-job",
    )


def _completed_describe_response():
    return {
        "JobName": "rec-74443947857331f7",
        "JobArn": JOB_ARN,
        "JobType": "Default",
        "Status": "COMPLETED",
        "CompletionTime": datetime(2026, 4, 30, 10, 45, 0),
        "InferenceRecommendations": [
            {
                "RecommendationId": "rec-74443947857331f7/1",
                "Metrics": {
                    "CostPerHour": 0.42,
                    "CostPerInference": 0.0000012,
                    "MaxInvocations": 1200,
                    "ModelLatency": 38,
                    "CpuUtilization": 71.4,
                    "MemoryUtilization": 55.2,
                    "ModelSetupTime": 17,
                },
                "EndpointConfiguration": {
                    "EndpointName": "sm-epc-1",
                    "VariantName": "AllTraffic",
                    "InstanceType": "ml.m5.xlarge",
                    "InitialInstanceCount": 1,
                },
                "ModelConfiguration": {
                    "InferenceSpecificationName": "default",
                    "CompilationJobName": None,
                },
                "InvocationStartTime": datetime(2026, 4, 30, 10, 5, 0),
                "InvocationEndTime": datetime(2026, 4, 30, 10, 15, 0),
            }
        ],
        "EndpointPerformances": [],
    }


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_create_get_delete_happy_path(mock_call):
    mock_call.return_value = (_completed_describe_response(), idempotence_token)

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    metadata = SageMakerInferenceRecommenderJobMetadata(
        config=_task_config()["config"], region=REGION
    )

    response = await connector.create(_task_template())
    assert response == metadata

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    result = resource.outputs["result"]
    assert result["JobArn"] == JOB_ARN
    assert result["JobName"] == "rec-74443947857331f7"
    assert result["JobType"] == "Default"
    assert result["CompletionTime"] == "2026-04-30T10:45:00"

    assert len(result["InferenceRecommendations"]) == 1
    top = result["InferenceRecommendations"][0]
    assert top["RecommendationId"] == "rec-74443947857331f7/1"
    assert top["EndpointConfiguration"]["InstanceType"] == "ml.m5.xlarge"
    assert top["EndpointConfiguration"]["InitialInstanceCount"] == 1
    assert top["Metrics"]["CostPerHour"] == 0.42
    assert top["Metrics"]["ModelLatency"] == 38
    assert top["InvocationStartTime"] == "2026-04-30T10:05:00"
    assert top["InvocationEndTime"] == "2026-04-30T10:15:00"
    assert result["EndpointPerformances"] == []

    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_get_pending_and_inprogress_map_to_running(mock_call):
    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    metadata = SageMakerInferenceRecommenderJobMetadata(
        config={"JobName": "rec-x"}, region=REGION
    )

    mock_call.return_value = (
        {"JobName": "rec-x", "JobArn": JOB_ARN, "Status": "PENDING"},
        idempotence_token,
    )
    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.message is None
    assert resource.outputs is None

    mock_call.return_value = (
        {"JobName": "rec-x", "JobArn": JOB_ARN, "Status": "IN_PROGRESS"},
        idempotence_token,
    )
    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.RUNNING
    assert resource.message is None
    assert resource.outputs is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_get_failed_surfaces_failure_reason(mock_call):
    mock_call.return_value = (
        {
            "JobName": "rec-x",
            "JobArn": JOB_ARN,
            "Status": "FAILED",
            "FailureReason": "Model failed to load on ml.m5.large",
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    metadata = SageMakerInferenceRecommenderJobMetadata(
        config={"JobName": "rec-x"}, region=REGION
    )

    resource = await connector.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.message == "Model failed to load on ml.m5.large"


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_create_already_exists_returns_metadata(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceInUse",
                    "Message": "Inference recommendations job rec-74443947857331f7 already exists",
                }
            },
            operation_name="CreateInferenceRecommendationsJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    response = await connector.create(_task_template())
    assert response.config == _task_config()["config"]
    assert response.region == REGION


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_create_resource_limit_propagates(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceLimitExceeded",
                    "Message": "Inference Recommender job quota exceeded",
                }
            },
            operation_name="CreateInferenceRecommendationsJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    with pytest.raises(CustomException):
        await connector.create(_task_template())


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_delete_swallows_terminal_job_error(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ValidationException",
                    "Message": (
                        "The request was rejected because the inference "
                        "recommendations job is not in a non-running state"
                    ),
                }
            },
            operation_name="StopInferenceRecommendationsJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    metadata = SageMakerInferenceRecommenderJobMetadata(
        config={"JobName": "rec-x"}, region=REGION
    )

    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_delete_swallows_resource_not_found(mock_call):
    mock_call.side_effect = CustomException(
        message="An error occurred",
        idempotence_token=idempotence_token,
        original_exception=ClientError(
            error_response={
                "Error": {
                    "Code": "ResourceNotFound",
                    "Message": "Inference recommendations job does not exist",
                }
            },
            operation_name="StopInferenceRecommendationsJob",
        ),
    )

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    metadata = SageMakerInferenceRecommenderJobMetadata(
        config={"JobName": "rec-x"}, region=REGION
    )
    assert await connector.delete(metadata) is None


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference_recommender.connector.Boto3ConnectorMixin._call"
)
async def test_get_existing_endpoint_job_emits_endpoint_performances(mock_call):
    """Default jobs can benchmark existing endpoints and report their performance."""
    mock_call.return_value = (
        {
            "JobName": "rec-existing-endpoint",
            "JobArn": JOB_ARN,
            "JobType": "Default",
            "Status": "COMPLETED",
            "InferenceRecommendations": [],
            "EndpointPerformances": [
                {
                    "Metrics": {"MaxInvocations": 800, "ModelLatency": 52},
                    "EndpointInfo": {"EndpointName": "ranker-prod-canary"},
                }
            ],
        },
        idempotence_token,
    )

    connector = ConnectorRegistry.get_connector("sagemaker-inference-recommender-job")
    metadata = SageMakerInferenceRecommenderJobMetadata(
        config={"JobName": "rec-existing-endpoint"}, region=REGION
    )
    resource = await connector.get(metadata)
    result = resource.outputs["result"]
    assert result["JobType"] == "Default"
    assert result["InferenceRecommendations"] == []
    assert result["EndpointPerformances"] == [
        {
            "Metrics": {"MaxInvocations": 800, "ModelLatency": 52},
            "EndpointInfo": {"EndpointName": "ranker-prod-canary"},
        }
    ]
