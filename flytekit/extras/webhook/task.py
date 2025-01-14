import http
from typing import Any, Dict, Optional, Type

from flytekit import Documentation
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.extend.backend.base_agent import SyncAgentExecutorMixin

from .constants import BODY_KEY, HEADERS_KEY, METHOD_KEY, SHOW_BODY_KEY, SHOW_URL_KEY, TASK_TYPE, URL_KEY


class WebhookTask(SyncAgentExecutorMixin, PythonTask):
    """
    This is the simplest form of a BigQuery Task, that can be used even for tasks that do not produce any output.
    """

    def __init__(
        self,
        name: str,
        url: str,
        method: http.HTTPMethod = http.HTTPMethod.POST,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Dict[str, Any]] = None,
        dynamic_inputs: Optional[Dict[str, Type]] = None,
        show_body: bool = False,
        show_url: bool = False,
        description: Optional[str] = None,
        # secret_requests: Optional[List[Secret]] = None,  TODO Secret support is coming soon
    ):
        """
        This task is used to invoke a webhook. The webhook can be invoked with a POST or GET method.

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

        :param name: Name of this task, should be unique in the project
        :param url: The endpoint or URL to invoke for this webhook. This can be a static string or a python format string,
            where the format arguments are the dynamic_inputs to the task, secrets etc. Refer to the description for more
            details of available formatting parameters.
        :param method: The HTTP method to use for the request. Default is POST.
        :param headers: The headers to send with the request. This can be a static dictionary or a python format string,
            where the format arguments are the dynamic_inputs to the task, secrets etc. Refer to the description for more
            details of available formatting parameters.
        :param body: The body to send with the request. This can be a static dictionary or a python format string,
            where the format arguments are the dynamic_inputs to the task, secrets etc. Refer to the description for more
            details of available formatting parameters.
        :param dynamic_inputs: The dynamic inputs to the task. The keys are the names of the inputs and the values
            are the types of the inputs. These inputs are available under the prefix `inputs.` to be used in the URL,
            headers and body and other formatted fields.
        :param secret_requests: The secrets that are requested by the task. (TODO not yet supported)
        :param show_body: If True, the body of the request will be logged in the UI as the output of the task.
        :param show_url: If True, the URL of the request will be logged in the UI as the output of the task.
        :param description: Description of the task
        """
        if method not in {http.HTTPMethod.GET, http.HTTPMethod.POST}:
            raise ValueError(f"Method should be either GET or POST. Got {method}")
        if method == http.HTTPMethod.GET:
            if body:
                raise ValueError("GET method cannot have a body")
            if show_body:
                raise ValueError("GET method cannot show body")
        outputs = {
            "status_code": int,
        }
        if show_body:
            outputs["body"] = dict
        if show_url:
            outputs["url"] = bool
        super().__init__(
            name=name,
            inputs=dynamic_inputs,
            outputs=outputs,
            task_type=TASK_TYPE,
            # secret_requests=secret_requests,
            docs=Documentation(short_description=description) if description else None,
        )
        self._url = url
        self._method = method
        self._headers = headers
        self._body = body
        self._show_body = show_body
        self._show_url = show_url

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        config = {
            URL_KEY: self._url,
            METHOD_KEY: self._method.value,
            HEADERS_KEY: self._headers or {},
            BODY_KEY: self._body or {},
            SHOW_BODY_KEY: self._show_body,
            SHOW_URL_KEY: self._show_url,
        }
        return config
