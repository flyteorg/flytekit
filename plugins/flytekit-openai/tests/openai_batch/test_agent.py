import json
from datetime import timedelta
from unittest import mock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.openai_batch_api.agent import BatchEndpointMetadata
from openai.types import Batch

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


@pytest.mark.asyncio
async def test_openai_batch_agent(batch_endpoint_result, create_endpoint_result):
    agent = AgentRegistry.get_agent("openai-batch-endpoint")
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
        type="openai-batch-endpoint",
    )

    # CREATE
    async with mock.patch(
        "openai.batches.AsyncBatches.create",
        return_value=Batch(
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
            request_counts={"total": 100, "completed": 95, "failed": 5},
            metadata={
                "customer_id": "user_123456789",
                "batch_description": "Nightly eval job",
            },
        ),
    ):
        metadata = BatchEndpointMetadata(openai_org="test-openai-orgnization-id", batch_id="batch_abc123")
        response = await agent.create(task_template)
        assert response == metadata

    # GET
    # Status: Completed
    async with mock.patch(
        "openai.batches.AsyncBatches.retrieve",
        return_value=Batch(
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
            request_counts={"total": 100, "completed": 95, "failed": 5},
            metadata={
                "customer_id": "user_123456789",
                "batch_description": "Nightly eval job",
            },
        ),
    ):
        resource = await agent.get(metadata)
        assert resource.phase == TaskExecution.SUCCEEDED

        result = resource.outputs["result"]
        assert result == json.dumps(batch_endpoint_result)

    # Status: Failed
    async with mock.patch(
        "openai.batches.AsyncBatches.retrieve",
        return_value=Batch(
            id="batch_JneJt99rNcZZncptC5Ec58hw",
            object="batch",
            endpoint="/v1/chat/completions",
            errors={
                "object": "list",
                "data": [
                    {
                        "code": "missing_required_parameter",
                        "message": "Missing required parameter: 'custom_id'.",
                        "param": "custom_id",
                        "line": 1,
                    },
                    {
                        "code": "missing_required_parameter",
                        "message": "Missing required parameter: 'custom_id'.",
                        "param": "custom_id",
                        "line": 2,
                    },
                ],
            },
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
            request_counts={"total": 0, "completed": 0, "failed": 0},
            metadata=None,
        ),
    ):
        resource = await agent.get(metadata)
        assert resource.phase == TaskExecution.FAILED
        assert resource.outputs is None
        assert resource.message == "Missing required parameter: 'custom_id'."
