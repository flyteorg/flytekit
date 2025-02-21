import http
from unittest.mock import patch

import flytekit as fk
from flytekit.extras.webhook import WebhookTask


@fk.task
def hello(s: str) -> str:
    return "Hello " + s


simple_get = WebhookTask(
    name="simple-get",
    url="http://localhost:8000/",
    method="GET",
    headers={"Content-Type": "application/json"},
)

get_with_params = WebhookTask(
    name="get-with-params",
    url="http://localhost:8000/items/{inputs.item_id}",
    method="GET",
    headers={"Content-Type": "application/json"},
    dynamic_inputs={"s": str, "item_id": int},
    show_data=True,
    show_url=True,
    description="Test Webhook Task",
    data={"q": "{inputs.s}"},
)


@fk.workflow
def wf(s: str) -> (dict, dict, dict):
    v = hello(s=s)
    w = WebhookTask(
        name="invoke-slack",
        url="https://hooks.slack.com/services/test/test/test",
        headers={"Content-Type": "application/json"},
        data={"text": "{inputs.s}"},
        show_data=True,
        show_url=True,
        description="Test Webhook Task",
        dynamic_inputs={"s": str},
    )
    return simple_get(), get_with_params(s=v, item_id=10), w(s=v)


@patch("flytekit.extras.webhook.WebhookAgent._make_http_request")
def test_wf(request_mock):
    request_mock.return_value = (200, "Hello")
    # Execute the workflow
    res = wf("world")
    # Check the output
    assert res[0] == {"status_code": 200, "response_data": "Hello"}
    assert res[1] == {
        "status_code": 200,
        "response_data": "Hello",
        "input_data": {"q": "Hello world"},
        "url": "http://localhost:8000/items/10",
    }
    assert res[2] == {
        "status_code": 200,
        "response_data": "Hello",
        "input_data": {"text": "Hello world"},
        "url": "https://hooks.slack.com/services/test/test/test",
    }
