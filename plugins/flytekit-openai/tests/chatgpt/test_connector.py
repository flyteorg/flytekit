from datetime import timedelta
from unittest import mock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.openai.chatgpt.connector import TIMEOUT_SECONDS

from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.literals import LiteralMap
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


async def mock_acreate(*args, **kwargs) -> str:
    mock_response = mock.MagicMock()
    mock_choice = mock.MagicMock()
    mock_choice.message.content = "mocked_message"
    mock_response.choices = [mock_choice]
    return mock_response


@pytest.mark.asyncio
async def test_chatgpt_connector():
    connector = ConnectorRegistry.get_connector("chatgpt")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_config = {
        "openai_organization": "test-openai-orgnization-id",
        "chatgpt_config": {"model": "gpt-3.5-turbo", "temperature": 0.7},
    }
    task_metadata = TaskMetadata(
        True,
        RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
        (),
    )
    tmp = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=None,
        type="chatgpt",
    )

    task_inputs = LiteralMap(
        {
            "message": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="Test ChatGPT Plugin"))
            ),
        },
    )
    message = "mocked_message"

    # Create a more specific mock for the context
    mock_context = mock.MagicMock()
    mock_secrets = mock.MagicMock()
    # Return a string directly instead of a MagicMock
    mock_secrets.get.return_value = "mocked_openai_api_key"
    mock_context.secrets = mock_secrets

    # Patch the current_context function to return our specific mock
    with mock.patch("flytekit.FlyteContextManager.current_context", return_value=mock_context):
        with mock.patch("openai.resources.chat.completions.AsyncCompletions.create", new=mock_acreate):
            # Directly await the coroutine without using asyncio.run
            response = await connector.do(tmp, task_inputs)

    assert response.phase == TaskExecution.SUCCEEDED
    assert response.outputs == {"o0": message}


def _build_chatgpt_template(chatgpt_config):
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_config = {
        "openai_organization": "test-openai-orgnization-id",
        "chatgpt_config": chatgpt_config,
    }
    task_metadata = TaskMetadata(
        True,
        RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
        (),
    )
    return TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=None,
        type="chatgpt",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "chatgpt_config, expected_timeout",
    [
        ({"model": "gpt-3.5-turbo", "temperature": 0.7}, TIMEOUT_SECONDS),
        ({"model": "gpt-3.5-turbo", "temperature": 0.7, "timeout": 120}, 120),
    ],
)
async def test_chatgpt_connector_timeout_from_config(chatgpt_config, expected_timeout):
    connector = ConnectorRegistry.get_connector("chatgpt")
    tmp = _build_chatgpt_template(chatgpt_config)
    task_inputs = LiteralMap(
        {
            "message": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="Test ChatGPT Plugin"))
            ),
        },
    )

    captured = {}

    async def fake_wait_for(coro, timeout):
        captured["timeout"] = timeout
        return await coro

    mock_context = mock.MagicMock()
    mock_secrets = mock.MagicMock()
    mock_secrets.get.return_value = "mocked_openai_api_key"
    mock_context.secrets = mock_secrets

    with mock.patch("flytekit.FlyteContextManager.current_context", return_value=mock_context):
        with mock.patch("openai.resources.chat.completions.AsyncCompletions.create", new=mock_acreate):
            with mock.patch("asyncio.wait_for", new=fake_wait_for):
                response = await connector.do(tmp, task_inputs)

    assert response.phase == TaskExecution.SUCCEEDED
    assert captured["timeout"] == expected_timeout
