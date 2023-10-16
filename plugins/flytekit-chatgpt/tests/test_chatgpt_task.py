from unittest import mock

import aioresponses
import pytest
from flyteidl.admin.agent_pb2 import SUCCEEDED, DoTaskResponse, Resource
from flytekitplugins.chatgpt import ChatGPTTask

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.models.literals import LiteralMap


@pytest.mark.asyncio
async def test_chatgpt_task():
    message = "TEST MESSAGE"
    response_message = "Hello! How can I assist you today?"
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
    ctx = FlyteContextManager.current_context()

    assert chatgpt_job._chatgpt_conf == {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": message}],
        "temperature": 0.7,
    }
    assert chatgpt_job._openai_organization == organization

    mocked_token = "mocked_chatgpt_token"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token
    openai_url = "https://api.openai.com/v1/chat/completions"
    mock_do_response = {
        "id": "chatcmpl-8AJDGV3GdDsTcdFJfc68OxuLqIJZr",
        "object": "chat.completion",
        "created": 1697467826,
        "model": "gpt-3.5-turbo-0613",
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": response_message}, "finish_reason": "stop"}
        ],
        "usage": {"prompt_tokens": 8, "completion_tokens": 9, "total_tokens": 17},
    }
    with aioresponses.aioresponses() as mocked:
        mocked.post(openai_url, status=200, payload=mock_do_response)
        res = await chatgpt_job.do(message=message)

        assert res.resource.state == SUCCEEDED
        assert (
            res.resource.outputs
            == LiteralMap(
                {
                    "o0": TypeEngine.to_literal(
                        ctx,
                        response_message,
                        type(response_message),
                        TypeEngine.to_literal_type(type(response_message)),
                    )
                }
            ).to_flyte_idl()
        )

    mock.patch.stopall()
