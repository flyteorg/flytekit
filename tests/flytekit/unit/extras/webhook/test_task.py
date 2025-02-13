import http
from asyncio import timeout
from datetime import timedelta

from flytekit.configuration import SerializationSettings, ImageConfig
from flytekit.extras.webhook.constants import HEADERS_KEY, URL_KEY, METHOD_KEY, DATA_KEY, SHOW_DATA_KEY, SHOW_URL_KEY, \
    TIMEOUT_SEC
from flytekit.extras.webhook.task import WebhookTask


def test_webhook_task_constructor():
    task = WebhookTask(
        name="test_task",
        url="http://example.com",
        method="POST",
        headers={"Content-Type": "application/json"},
        data={"key": "value"},
        show_data=True,
        show_url=True,
        description="Test Webhook Task",
        timeout=60,
    )

    assert task.name == "test_task"
    assert task._url == "http://example.com"
    assert task._method == "POST"
    assert task._headers == {"Content-Type": "application/json"}
    assert task._data == {"key": "value"}
    assert task._show_data is True
    assert task._show_url is True
    assert task.docs.short_description == "Test Webhook Task"
    assert task._timeout_sec == 60


def test_webhook_task_get_custom():
    task = WebhookTask(
        name="test_task",
        url="http://example.com",
        method="POST",
        headers={"Content-Type": "application/json"},
        data={"key": "value"},
        show_data=True,
        show_url=True,
        timeout=timedelta(minutes=1),
    )

    settings = SerializationSettings(image_config=ImageConfig.auto_default_image())
    custom = task.get_custom(settings)

    assert custom[URL_KEY] == "http://example.com"
    assert custom[METHOD_KEY] == "POST"
    assert custom[HEADERS_KEY] == {"Content-Type": "application/json"}
    assert custom[DATA_KEY] == {"key": "value"}
    assert custom[SHOW_DATA_KEY] is True
    assert custom[SHOW_URL_KEY] is True
    assert custom[TIMEOUT_SEC] == 60
