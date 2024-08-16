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

from flytekitplugins.awssagemaker_inference.boto3_mixin import CustomException
from botocore.exceptions import ClientError

idempotence_token = "74443947857331f7"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_return_value",
    [
        (
            (
                {
                    "EndpointConfigArn": "arn:aws:sagemaker:us-east-2:000000000:endpoint-config/sagemaker-xgboost-endpoint-config",
                },
                idempotence_token,
            ),
            "create_endpoint_config",
        ),
        (
            (
                {
                    "pickle_check": datetime(2024, 5, 5),
                    "Location": "http://examplebucket.s3.amazonaws.com/",
                },
                idempotence_token,
            ),
            "create_bucket",
        ),
        ((None, idempotence_token), "create_endpoint_config"),
        (
            (
                CustomException(
                    message="An error occurred",
                    idempotence_token=idempotence_token,
                    original_exception=ClientError(
                        error_response={
                            "Error": {
                                "Code": "ValidationException",
                                "Message": "Cannot create already existing endpoint 'arn:aws:sagemaker:us-east-2:123456789:endpoint/stable-diffusion-endpoint-non-finetuned-06716dbe4b2c68e7'",
                            }
                        },
                        operation_name="DescribeEndpoint",
                    ),
                )
            ),
            "create_endpoint_config",
        ),
    ],
)
@mock.patch(
    "flytekitplugins.awssagemaker_inference.boto3_agent.Boto3AgentMixin._call",
)
async def test_agent(mock_boto_call, mock_return_value):
    mock_boto_call.return_value = mock_return_value[0]

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
            "AsyncInferenceConfig": {
                "OutputConfig": {"S3OutputPath": "{inputs.s3_output_path}"}
            },
        },
        "region": "us-east-2",
        "method": mock_return_value[1],
        "images": None,
    }
    task_metadata = TaskMetadata(
        discoverable=True,
        runtime=RuntimeMetadata(
            RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"
        ),
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
                scalar=literals.Scalar(
                    primitive=literals.Primitive(string_value="sagemaker-model")
                )
            ),
            "s3_output_path": literals.Literal(
                scalar=literals.Scalar(
                    primitive=literals.Primitive(string_value="s3-output-path")
                )
            ),
        },
    )

    ctx = FlyteContext.current_context()
    output_prefix = ctx.file_access.get_random_remote_directory()

    if isinstance(mock_return_value[0], Exception):
        mock_boto_call.side_effect = mock_return_value[0]

        resource = await agent.do(
            task_template=task_template,
            inputs=task_inputs,
            output_prefix=output_prefix,
        )
        assert resource.outputs["result"] == {
            "EndpointConfigArn": "arn:aws:sagemaker:us-east-2:123456789:endpoint/stable-diffusion-endpoint-non-finetuned-06716dbe4b2c68e7"
        }
        assert resource.outputs["idempotence_token"] == idempotence_token
        return

    resource = await agent.do(
        task_template=task_template, inputs=task_inputs, output_prefix=output_prefix
    )

    assert resource.phase == TaskExecution.SUCCEEDED

    if mock_return_value[0][0]:
        outputs = literal_map_string_repr(resource.outputs)
        if "pickle_check" in mock_return_value[0][0]:
            assert "pickle_file" in outputs["result"]
        else:
            assert (
                outputs["result"]["EndpointConfigArn"]
                == "arn:aws:sagemaker:us-east-2:000000000:endpoint-config/sagemaker-xgboost-endpoint-config"
            )
            assert outputs["idempotence_token"] == "74443947857331f7"
    elif mock_return_value[0][0] is None:
        assert resource.outputs["result"] == {"result": None}
