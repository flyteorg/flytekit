from datetime import datetime, timedelta
from unittest import mock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import FlyteContext
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_return_value",
    [
        (
            {
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
            }
        ),
        (
            {
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
                "pickle_check": datetime(2024, 5, 5),
                "EndpointConfigArn": "arn:aws:sagemaker:us-east-2:000000000:endpoint-config/sagemaker-xgboost-endpoint-config",
            }
        ),
        (None),
    ],
)
@mock.patch(
    "flytekitplugins.awssagemaker_inference.boto3_agent.Boto3AgentMixin._call",
)
async def test_agent(mock_boto_call, mock_return_value):
    mock_boto_call.return_value = mock_return_value

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

    ctx = FlyteContext.current_context()
    output_prefix = ctx.file_access.get_random_remote_directory()
    resource = await agent.do(task_template=task_template, inputs=task_inputs, output_prefix=output_prefix)

    assert resource.phase == TaskExecution.SUCCEEDED

    if mock_return_value:
        outputs = literal_map_string_repr(resource.outputs)
        if "pickle_check" in mock_return_value:
            assert "pickle_file" in outputs["result"]
        else:
            assert (
                outputs["result"]["EndpointConfigArn"]
                == "arn:aws:sagemaker:us-east-2:000000000:endpoint-config/sagemaker-xgboost-endpoint-config"
            )
    elif mock_return_value is None:
        assert resource.outputs["result"] == {"result": None}
