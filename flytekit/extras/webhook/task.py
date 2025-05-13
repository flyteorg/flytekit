from datetime import timedelta
from typing import Any, Dict, Optional, Type, Union

from flytekit import Documentation
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.extend.backend.base_connector import SyncConnectorExecutorMixin

from ...core.interface import Interface
from .constants import DATA_KEY, HEADERS_KEY, METHOD_KEY, SHOW_DATA_KEY, SHOW_URL_KEY, TASK_TYPE, TIMEOUT_SEC, URL_KEY


class WebhookTask(SyncConnectorExecutorMixin, PythonTask):
    """
    The WebhookTask is used to invoke a webhook. The webhook can be invoked with a POST or GET method.

    All the parameters can be formatted using python format strings.

    Example:
    ```python
    simple_get = WebhookTask(
    name="simple-get",
    url="http://localhost:8000/",
    method=http.HTTPMethod.GET,
    headers={"Content-Type": "application/json"},
    )

    get_with_params = WebhookTask(
        name="get-with-params",
        url="http://localhost:8000/items/{inputs.item_id}",
        method=http.HTTPMethod.GET,
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
            url="https://hooks.slack.com/services/xyz/zaa/aaa",
            headers={"Content-Type": "application/json"},
            data={"text": "{inputs.s}"},
            show_data=True,
            show_url=True,
            description="Test Webhook Task",
            dynamic_inputs={"s": str},
        )
        return simple_get(), get_with_params(s=v, item_id=10), w(s=v)
    ```

     All the parameters can be formatted using python format strings. The following parameters are available for
    formatting:
    - dynamic_inputs: These are the dynamic inputs to the task. The keys are the names of the inputs and the values
        are the values of the inputs. All inputs are available under the prefix `inputs.`.
        For example, if the inputs are {"input1": 10, "input2": "hello"}, then you can
        use {inputs.input1} and {inputs.input2} in the URL and the body. Define the dynamic_inputs argument in the
        constructor to use these inputs. The dynamic inputs should not be actual values, but the types of the inputs.

    TODO Coming soon secrets support
    - secrets: These are the secrets that are requested by the task. The keys are the names of the secrets and the
        values are the values of the secrets. All secrets are available under the prefix `secrets.`.
        For example, if the secret requested are Secret(name="secret1") and Secret(name="secret), then you can use
        {secrets.secret1} and {secrets.secret2} in the URL and the body. Define the secret_requests argument in the
        constructor to use these secrets. The secrets should not be actual values, but the types of the secrets.

    Args:
        name (str): Name of this task, should be unique in the project.
        url (str): The endpoint or URL to invoke for this webhook. This can be a static string or a Python format string,
            where the format arguments are the dynamic_inputs to the task, secrets, etc. Refer to the description for more
            details of available formatting parameters.
        method (str): The HTTP method to use for the request. Default is POST.
        headers (Optional[Dict[str, str]]): The headers to send with the request. This can be a static dictionary or a Python format string,
            where the format arguments are the dynamic_inputs to the task, secrets, etc. Refer to the description for more
            details of available formatting parameters.
        data (Optional[Dict[str, Any]]): The body to send with the request. This can be a static dictionary or a Python format string,
            where the format arguments are the dynamic_inputs to the task, secrets, etc. Refer to the description for more
            details of available formatting parameters. The data should be a JSON-serializable dictionary and will be
            sent as the JSON body of the POST request and as the query parameters of the GET request.
        dynamic_inputs (Optional[Dict[str, Type]]): The dynamic inputs to the task. The keys are the names of the inputs and the values
            are the types of the inputs. These inputs are available under the prefix `inputs.` to be used in the URL,
            headers, body, and other formatted fields.
        show_data (bool): If True, the body of the request will be logged in the UI as the output of the task.
        show_url (bool): If True, the URL of the request will be logged in the UI as the output of the task.
        description (Optional[str]): Description of the task.
        timeout (Union[int, timedelta]): The timeout for the request (connection and read). Default is 10 seconds. If an int is provided,
            it is considered as seconds.

    Raises:
        ValueError: if the method is not 'POST' or 'GET'.
    """

    def __init__(
        self,
        name: str,
        url: str,
        method: str = "POST",
        headers: Optional[Dict[str, str]] = None,
        data: Optional[Dict[str, Any]] = None,
        dynamic_inputs: Optional[Dict[str, Type]] = None,
        show_data: bool = False,
        show_url: bool = False,
        description: Optional[str] = None,
        timeout: Union[int, timedelta] = timedelta(seconds=10),
        # secret_requests: Optional[List[Secret]] = None,  TODO Secret support is coming soon
    ):
        if method not in {"GET", "POST"}:
            raise ValueError(f"Method should be either GET or POST. Got {method}")

        interface = Interface(
            inputs=dynamic_inputs or {},
            outputs={"info": dict},
        )
        super().__init__(
            name=name,
            interface=interface,
            task_type=TASK_TYPE,
            # secret_requests=secret_requests,
            docs=Documentation(short_description=description) if description else None,
        )
        self._url = url
        self._method = method
        self._headers = headers
        self._data = data
        self._show_data = show_data
        self._show_url = show_url
        self._timeout_sec = timeout if isinstance(timeout, int) else timeout.total_seconds()

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        config = {
            URL_KEY: self._url,
            METHOD_KEY: self._method,
            HEADERS_KEY: self._headers or {},
            DATA_KEY: self._data or {},
            SHOW_DATA_KEY: self._show_data,
            SHOW_URL_KEY: self._show_url,
            TIMEOUT_SEC: self._timeout_sec,
        }
        return config
