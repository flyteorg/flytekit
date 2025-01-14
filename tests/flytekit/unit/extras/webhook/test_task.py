import http

from flytekit.configuration import SerializationSettings, ImageConfig
from flytekit.extras.webhook.task import WebhookTask


def test_webhook_task_constructor():
    task = WebhookTask(
        name="test_task",
        url="http://example.com",
        method=http.HTTPMethod.POST,
        headers={"Content-Type": "application/json"},
        body={"key": "value"},
        show_body=True,
        show_url=True,
        description="Test Webhook Task"
    )

    assert task.name == "test_task"
    assert task._url == "http://example.com"
    assert task._method == http.HTTPMethod.POST
    assert task._headers == {"Content-Type": "application/json"}
    assert task._body == {"key": "value"}
    assert task._show_body is True
    assert task._show_url is True
    assert task.docs.short_description == "Test Webhook Task"


def test_webhook_task_get_custom():
    task = WebhookTask(
        name="test_task",
        url="http://example.com",
        method=http.HTTPMethod.POST,
        headers={"Content-Type": "application/json"},
        body={"key": "value"},
        show_body=True,
        show_url=True
    )

    settings = SerializationSettings(image_config=ImageConfig.auto_default_image())
    custom = task.get_custom(settings)

    assert custom["url"] == "http://example.com"
    assert custom["method"] == "POST"
    assert custom["headers"] == {"Content-Type": "application/json"}
    assert custom["body"] == {"key": "value"}
    assert custom["show_body"] is True
    assert custom["show_url"] is True
