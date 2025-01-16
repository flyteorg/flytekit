from unittest.mock import patch, MagicMock, AsyncMock

import pytest

from flytekit.extras.webhook.agent import WebhookAgent
from flytekit.extras.webhook.constants import SHOW_DATA_KEY, DATA_KEY, METHOD_KEY, URL_KEY, HEADERS_KEY, SHOW_URL_KEY
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
    }
    return task_template


@pytest.fixture
def mock_aiohttp_session():
    with patch('aiohttp.ClientSession') as mock_session:
        yield mock_session


@pytest.mark.asyncio
async def test_do_post_success(mock_task_template, mock_aiohttp_session):
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="Success")
    mock_aiohttp_session.return_value.post = AsyncMock(return_value=mock_response)

    agent = WebhookAgent()
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.SUCCEEDED
    assert result.outputs["info"]
    assert result.outputs["info"]["status_code"] == 200
    assert result.outputs["info"]["url"] == "http://example.com"


@pytest.mark.asyncio
async def test_do_get_success(mock_task_template, mock_aiohttp_session):
    mock_task_template.custom[METHOD_KEY] = "GET"
    mock_task_template.custom.pop(DATA_KEY)
    mock_task_template.custom[SHOW_DATA_KEY] = False

    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value="Success")
    mock_aiohttp_session.return_value.get = AsyncMock(return_value=mock_response)

    agent = WebhookAgent()
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.SUCCEEDED
    assert result.outputs["info"]["status_code"] == 200
    assert result.outputs["info"]["url"] == "http://example.com"


@pytest.mark.asyncio
async def test_do_failure(mock_task_template, mock_aiohttp_session):
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text = AsyncMock(return_value="Internal Server Error")
    mock_aiohttp_session.return_value.post = AsyncMock(return_value=mock_response)

    agent = WebhookAgent()
    result = await agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.FAILED
    assert "Webhook failed with status code 500" in result.message
