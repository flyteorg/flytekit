from unittest.mock import patch, MagicMock, AsyncMock

import pytest
import httpx

from flytekit.core.context_manager import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extras.webhook.agent import WebhookAgent
from flytekit.extras.webhook.constants import SHOW_DATA_KEY, DATA_KEY, METHOD_KEY, URL_KEY, HEADERS_KEY, SHOW_URL_KEY, TIMEOUT_SEC
from flytekit.models.core.execution import TaskExecutionPhase as TaskExecution
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@pytest.fixture
def mock_task_template():
    task_template = MagicMock(spec=TaskTemplate)
    task_template.custom = {
        URL_KEY: "http://example.com",
        METHOD_KEY: "POST",
        HEADERS_KEY: {"Content-Type": "application/json"},
        DATA_KEY: {"key": "value"},
        SHOW_DATA_KEY: True,
        SHOW_URL_KEY: True,
        TIMEOUT_SEC: 60,
    }
    return task_template


@pytest.mark.asyncio
async def test_do_post_success(mock_task_template):
    mock_response = AsyncMock(name="httpx.Response")
    mock_response.status_code = 200
    mock_response.text = "Success"
    mock_httpx_client = AsyncMock(name="httpx.AsyncClient")
    mock_httpx_client.post.return_value = mock_response

    agent = WebhookAgent(client=mock_httpx_client)
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.SUCCEEDED
    assert result.outputs["info"]["status_code"] == 200
    assert result.outputs["info"]["response_data"] == "Success"
    assert result.outputs["info"]["url"] == "http://example.com"


@pytest.mark.asyncio
async def test_do_get_success(mock_task_template):
    mock_task_template.custom[METHOD_KEY] = "GET"
    mock_task_template.custom.pop(DATA_KEY)
    mock_task_template.custom[SHOW_DATA_KEY] = False

    mock_response = AsyncMock(name="httpx.Response")
    mock_response.status_code = 200
    mock_response.text = "Success"
    mock_httpx_client = AsyncMock(name="httpx.AsyncClient")
    mock_httpx_client.get.return_value = mock_response

    agent = WebhookAgent(client=mock_httpx_client)
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.SUCCEEDED
    assert result.outputs["info"]["status_code"] == 200
    assert result.outputs["info"]["response_data"] == "Success"
    assert result.outputs["info"]["url"] == "http://example.com"


@pytest.mark.asyncio
async def test_do_failure(mock_task_template):
    mock_response = AsyncMock(name="httpx.Response")
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_httpx_client = AsyncMock(name="httpx.AsyncClient")
    mock_httpx_client.post.return_value = mock_response

    agent = WebhookAgent(client=mock_httpx_client)
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.FAILED
    assert "Webhook failed with status code 500" in result.message


@pytest.mark.asyncio
async def test_do_get_failure(mock_task_template):
    mock_task_template.custom[METHOD_KEY] = "GET"
    mock_task_template.custom.pop(DATA_KEY)
    mock_task_template.custom[SHOW_DATA_KEY] = False

    mock_response = AsyncMock(name="httpx.Response")
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_httpx_client = AsyncMock(name="httpx.AsyncClient")
    mock_httpx_client.get.return_value = mock_response

    agent = WebhookAgent(client=mock_httpx_client)
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.FAILED
    assert "Webhook failed with status code 500" in result.message


def test_conversion_of_inputs():
    agent = WebhookAgent()
    result = agent._build_response(200, "Success", {"key": "value"}, "http://example.com", True, True)

    assert result["status_code"] == 200
    assert result["response_data"] == "Success"
    assert result["input_data"] == {"key": "value"}
    assert result["url"] == "http://example.com"


def test_get_final_dict():
    agent = WebhookAgent()
    ctx = FlyteContextManager.current_context()
    inputs = TypeEngine.dict_to_literal_map(ctx, {"x": "value_x", "y": "value_y", "z": "value_z"})
    task_template = MagicMock(spec=TaskTemplate)
    task_template.custom = {
        URL_KEY: "http://example.com/{inputs.x}",
        METHOD_KEY: "POST",
        HEADERS_KEY: {"Content-Type": "application/json", "Authorization": "{inputs.y}"},
        DATA_KEY: {"key": "{inputs.z}"},
        SHOW_DATA_KEY: True,
        SHOW_URL_KEY: True,
        TIMEOUT_SEC: 60,
    }
    result = agent._get_final_dict(task_template, inputs)

    expected_result = {
        "url": "http://example.com/value_x",
        "method": "POST",
        "headers": {"Content-Type": "application/json", "Authorization": "value_y"},
        "data": {"key": "value_z"},
        "show_data": True,
        "show_url": True,
        "timeout_sec": 60,
    }

    assert result == expected_result
