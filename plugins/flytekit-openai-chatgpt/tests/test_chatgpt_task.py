from unittest import mock

import pytest
from flyteidl.admin.agent_pb2 import SUCCEEDED
from flytekitplugins.chatgpt import ChatGPTTask


async def mock_acreate(*args, **kwargs) -> str:
    mock_response = mock.MagicMock()
    mock_choice = mock.MagicMock()
    mock_choice.message.content = "mocked_message"
    mock_response.choices = [mock_choice]
    return mock_response


@pytest.mark.asyncio
async def test_chatgpt_task_do():
    message = "TEST MESSAGE"
    organization = "TEST ORGANIZATION"

    chatgpt_job = ChatGPTTask(
        name="chatgpt",
        config={
            "openai_organization": organization,
            "chatgpt_conf": {
                "model": "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": message}],
                "temperature": 0.7,
            },
        },
    )

    with mock.patch("openai.ChatCompletion.acreate", new=mock_acreate):
        with mock.patch("flytekit.extend.backend.base_agent.get_agent_secret", return_value="mocked_secret"):
            response = await chatgpt_job.do(message=message)

    assert response.resource.state == SUCCEEDED
    assert "mocked_message" in str(response.resource.outputs)
