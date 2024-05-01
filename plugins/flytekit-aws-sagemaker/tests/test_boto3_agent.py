from datetime import timedelta
from unittest import mock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


@pytest.mark.asyncio
@mock.patch(
    "flytekitplugins.awssagemaker_inference.boto3_agent.Boto3AgentMixin._call",
    return_value={
        "ResponseMetadata": {
            "RequestId": "66f80391-348a-4ee0-9158-508914d16db2",
            "HTTPStatusCode": 200.0,
            "RetryAttempts": 0.0,
            "HTTPHeaders": {
                "content-type": "application/x-amz-json-1.1",
                "date": "Wed, 31 Jan 2024 16:43:52 GMT",
                "x-amzn-requestid": "66f80391-348a-4ee0-9158-508914d16db2",
                "content-length": "114",
            },
        },
        "EndpointConfigArn": "arn:aws:sagemaker:us-east-2:000000000:endpoint-config/sagemaker-xgboost-endpoint-config",
    },
)
async def test_agent(mock_boto_call):
    agent = AgentRegistry.get_agent("boto")
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
            "EndpointConfigName": "endpoint-config-name",
            "ProductionVariants": [
                {
                    "VariantName": "variant-name-1",
                    "ModelName": "{inputs.model_name}",
                    "InitialInstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                },
            ],
            "AsyncInferenceConfig": {"OutputConfig": {"S3OutputPath": "{inputs.s3_output_path}"}},
        },
        "region": "us-east-2",
        "method": "create_endpoint_config",
        "images": None,
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
        type="boto",
    )
    task_inputs = literals.LiteralMap(
        {
            "model_name": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="sagemaker-model"))
            ),
            "s3_output_path": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="s3-output-path"))
            ),
        },
    )

    resource = await agent.do(task_template, task_inputs)

    assert resource.phase == TaskExecution.SUCCEEDED
    assert (
        resource.outputs["result"]["EndpointConfigArn"]
        == "arn:aws:sagemaker:us-east-2:000000000:endpoint-config/sagemaker-xgboost-endpoint-config"
    )
