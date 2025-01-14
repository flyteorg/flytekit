from unittest.mock import patch, MagicMock

import pytest
from flytekit.models.core.execution import TaskExecutionPhase as TaskExecution
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.extras.webhook.agent import WebhookAgent


@pytest.fixture
def mock_task_template():
    task_template = MagicMock(spec=TaskTemplate)
    task_template.custom = {
        "url": "http://example.com",
        "method": "POST",
        "headers": {"Content-Type": "application/json"},
        "body": {"key": "value"},
        "show_body": True,
        "show_url": True,
    }
    return task_template

@patch('flytekit.extras.webhook.agent.httpx')
def test_do_post_success(mock_httpx, mock_task_template):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "Success"
    mock_httpx.post.return_value = mock_response

    agent = WebhookAgent()
    result = agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.SUCCEEDED
    assert result.outputs["status_code"] == 200
    assert result.outputs["body"] == "Success"
    assert result.outputs["url"] == "http://example.com"

@patch('flytekit.extras.webhook.agent.httpx')
def test_do_get_success(mock_httpx, mock_task_template):
    mock_task_template.custom["method"] = "GET"
    mock_task_template.custom.pop("body")
    mock_task_template.custom["show_body"] = False

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "Success"
    mock_httpx.get.return_value = mock_response

    agent = WebhookAgent()
    result = agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.SUCCEEDED
    assert result.outputs["status_code"] == 200
    assert result.outputs["url"] == "http://example.com"

@patch('flytekit.extras.webhook.agent.httpx')
def test_do_failure(mock_httpx, mock_task_template):
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"
    mock_httpx.post.return_value = mock_response

    agent = WebhookAgent()
    result = agent.do(mock_task_template, output_prefix="", inputs=LiteralMap({}))

    assert result.phase == TaskExecution.FAILED
    assert "Webhook failed with status code 500" in result.message
