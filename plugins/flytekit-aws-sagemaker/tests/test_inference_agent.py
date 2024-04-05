import json
from datetime import timedelta
from unittest import mock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.awssagemaker_inference.agent import SageMakerEndpointMetadata

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference.agent.Boto3AgentMixin._call",
    return_value={
        "EndpointName": "sagemaker-xgboost-endpoint",
        "EndpointArn": "arn:aws:sagemaker:us-east-2:1234567890:endpoint/sagemaker-xgboost-endpoint",
        "EndpointConfigName": "sagemaker-xgboost-endpoint-config",
        "ProductionVariants": [
            {
                "VariantName": "variant-name-1",
                "DeployedImages": [
                    {
                        "SpecifiedImage": "1234567890.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost:iL3_jIEY3lQPB4wnlS7HKA..",
                        "ResolvedImage": "1234567890.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost@sha256:0725042bf15f384c46e93bbf7b22c0502859981fc8830fd3aea4127469e8cf1e",
                        "ResolutionTime": "2024-01-31T22:14:07.193000+05:30",
                    }
                ],
                "CurrentWeight": 1.0,
                "DesiredWeight": 1.0,
                "CurrentInstanceCount": 1,
                "DesiredInstanceCount": 1,
            }
        ],
        "EndpointStatus": "InService",
        "CreationTime": "2024-01-31T22:14:06.553000+05:30",
        "LastModifiedTime": "2024-01-31T22:16:37.001000+05:30",
        "AsyncInferenceConfig": {
            "OutputConfig": {"S3OutputPath": "s3://sagemaker-agent-xgboost/inference-output/output"}
        },
        "ResponseMetadata": {
            "RequestId": "50d8bfa7-d84-4bd9-bf11-992832f42793",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amzn-requestid": "50d8bfa7-d840-4bd9-bf11-992832f42793",
                "content-type": "application/x-amz-json-1.1",
                "content-length": "865",
                "date": "Wed, 31 Jan 2024 16:46:38 GMT",
            },
            "RetryAttempts": 0,
        },
    },
)
async def test_agent(mock_boto_call):
    agent = AgentRegistry.get_agent("sagemaker-endpoint")
    task_id = Identifier(
        resource_type=ResourceType.TASK,
        project="project",
        domain="domain",
        name="name",
        version="version",
    )
    task_config = {
        "service": "sagemaker",
        "config": {
            "EndpointName": "sagemaker-endpoint",
            "EndpointConfigName": "sagemaker-endpoint-config",
        },
        "region": "us-east-2",
        "method": "create_endpoint",
    }
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

    task_template = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=None,
        type="sagemaker-endpoint",
    )

    # CREATE
    metadata = SageMakerEndpointMetadata(
        config={
            "EndpointName": "sagemaker-endpoint",
            "EndpointConfigName": "sagemaker-endpoint-config",
        },
        region="us-east-2",
    )
    response = await agent.create(task_template)
    assert response == metadata

    # GET
    resource = await agent.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    from_json = json.loads(resource.outputs["result"])
    assert from_json["EndpointName"] == "sagemaker-xgboost-endpoint"
    assert from_json["EndpointArn"] == "arn:aws:sagemaker:us-east-2:1234567890:endpoint/sagemaker-xgboost-endpoint"

    # DELETE
    delete_response = await agent.delete(metadata)
    assert delete_response is None
