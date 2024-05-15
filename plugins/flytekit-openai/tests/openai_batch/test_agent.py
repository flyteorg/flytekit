from datetime import timedelta
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.openai.batch.agent import BatchEndpointMetadata
from openai.types import Batch, BatchError, BatchRequestCounts
from openai.types.batch import Errors

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate

batch_create_result = Batch(
    id="batch_abc123",
    object="batch",
    endpoint="/v1/completions",
    errors=None,
    input_file_id="file-abc123",
    completion_window="24h",
    status="completed",
    output_file_id="file-cvaTdG",
    error_file_id="file-HOWS94",
    created_at=1711471533,
    in_progress_at=1711471538,
    expires_at=1711557933,
    finalizing_at=1711493133,
    completed_at=1711493163,
    failed_at=None,
    expired_at=None,
    cancelling_at=None,
    cancelled_at=None,
    request_counts=BatchRequestCounts(completed=95, failed=5, total=100),
    metadata={
        "customer_id": "user_123456789",
        "batch_description": "Nightly eval job",
    },
)

batch_retrieve_result = Batch(
    id="batch_abc123",
    object="batch",
    endpoint="/v1/completions",
    errors=None,
    input_file_id="file-abc123",
    completion_window="24h",
    status="completed",
    output_file_id="file-cvaTdG",
    error_file_id="file-HOWS94",
    created_at=1711471533,
    in_progress_at=1711471538,
    expires_at=1711557933,
    finalizing_at=1711493133,
    completed_at=1711493163,
    failed_at=None,
    expired_at=None,
    cancelling_at=None,
    cancelled_at=None,
    request_counts=BatchRequestCounts(completed=95, failed=5, total=100),
    metadata={
        "customer_id": "user_123456789",
        "batch_description": "Nightly eval job",
    },
)

batch_retrieve_result_failure = Batch(
    id="batch_JneJt99rNcZZncptC5Ec58hw",
    object="batch",
    endpoint="/v1/chat/completions",
    errors=Errors(
        data=[
            BatchError(
                code="invalid_json_line",
                line=1,
                message="This line is not parseable as valid JSON.",
                param=None,
            ),
            BatchError(
                code="invalid_json_line",
                line=10,
                message="This line is not parseable as valid JSON.",
                param=None,
            ),
        ],
        object="list",
    ),
    input_file_id="file-3QV5EKbuUJjpACw0xPaVH6cV",
    completion_window="24h",
    status="failed",
    output_file_id=None,
    error_file_id=None,
    created_at=1713779467,
    in_progress_at=None,
    expires_at=1713865867,
    finalizing_at=None,
    completed_at=None,
    failed_at=1713779467,
    expired_at=None,
    cancelling_at=None,
    cancelled_at=None,
    request_counts=BatchRequestCounts(completed=0, failed=0, total=0),
    metadata=None,
)


@pytest.mark.asyncio
@mock.patch("flytekit.current_context")
@mock.patch("openai.resources.batches.AsyncBatches.create", new_callable=AsyncMock)
@mock.patch("openai.resources.batches.AsyncBatches.retrieve", new_callable=AsyncMock)
async def test_openai_batch_agent(mock_retrieve, mock_create, mock_context):
    agent = AgentRegistry.get_agent("openai-batch")
    task_id = Identifier(
        resource_type=ResourceType.TASK,
        project="project",
        domain="domain",
        name="name",
        version="version",
    )
    task_config = {
        "openai_organization": "test-openai-orgnization-id",
        "config": {"metadata": {"batch_description": "Nightly eval job"}},
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
        type="openai-batch",
    )

    mocked_token = "mocked_openai_api_key"
    mock_context.return_value.secrets.get.return_value = mocked_token

    metadata = BatchEndpointMetadata(openai_org="test-openai-orgnization-id", batch_id="batch_abc123")

    # GET
    # Status: Completed
    mock_retrieve.return_value = batch_retrieve_result
    resource = await agent.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED

    outputs = literal_map_string_repr(resource.outputs)
    result = outputs["result"]

    assert result == batch_retrieve_result.to_dict()

    # Status: Failed
    mock_retrieve.return_value = batch_retrieve_result_failure
    resource = await agent.get(metadata)
    assert resource.phase == TaskExecution.FAILED
    assert resource.outputs == {"result": {"result": None}}
    assert resource.message == "This line is not parseable as valid JSON."

    # CREATE
    mock_create.return_value = batch_create_result
    task_inputs = literals.LiteralMap(
        {
            "input_file_id": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="file-xuefauew"))
            )
        },
    )
    response = await agent.create(task_template, task_inputs)
    assert response == metadata
