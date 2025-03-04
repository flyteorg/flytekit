from typing import Optional

import httpx
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit.extend.backend.base_connector import ConnectorRegistry, Resource, SyncConnectorBase
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.utils.dict_formatter import format_dict

from .constants import DATA_KEY, HEADERS_KEY, METHOD_KEY, SHOW_DATA_KEY, SHOW_URL_KEY, TASK_TYPE, TIMEOUT_SEC, URL_KEY


class WebhookConnector(SyncConnectorBase):
    """
    WebhookConnector is responsible for handling webhook tasks.

    This connector sends HTTP requests based on the task template and inputs provided,
    and processes the responses to determine the success or failure of the task.

    :param client: An optional HTTP client to use for sending requests.
    """

    name: str = "Webhook Connector"

    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        super().__init__(task_type_name=TASK_TYPE)
        self._client = client or httpx.AsyncClient()

    async def do(
        self, task_template: TaskTemplate, output_prefix: str, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> Resource:
        """
        This method processes the webhook task and sends an HTTP request.

        It uses asyncio to send the request and process the response using the httpx library.
        """
        try:
            final_dict = self._get_final_dict(task_template, inputs)
            return await self._process_webhook(final_dict)
        except Exception as e:
            return Resource(phase=TaskExecution.FAILED, message=str(e))

    def _get_final_dict(self, task_template: TaskTemplate, inputs: LiteralMap) -> dict:
        custom_dict = task_template.custom
        input_dict = {
            "inputs": literal_map_string_repr(inputs),
        }
        return format_dict("test", custom_dict, input_dict)

    async def _make_http_request(self, method: str, url: str, headers: dict, data: dict, timeout: int) -> tuple:
        if method == "GET":
            response = await self._client.get(url, headers=headers, params=data, timeout=timeout)
        else:
            response = await self._client.post(url, json=data, headers=headers, timeout=timeout)
        return response.status_code, response.text

    @staticmethod
    def _build_response(
        status: int,
        text: str,
        data: dict = None,
        url: str = None,
        show_data: bool = False,
        show_url: bool = False,
    ) -> dict:
        final_response = {
            "status_code": status,
            "response_data": text,
        }
        if show_data:
            final_response["input_data"] = data
        if show_url:
            final_response["url"] = url
        return final_response

    async def _process_webhook(self, final_dict: dict) -> Resource:
        url = final_dict.get(URL_KEY)
        body = final_dict.get(DATA_KEY)
        headers = final_dict.get(HEADERS_KEY)
        method = str(final_dict.get(METHOD_KEY)).upper()
        show_data = final_dict.get(SHOW_DATA_KEY, False)
        show_url = final_dict.get(SHOW_URL_KEY, False)
        timeout_sec = final_dict.get(TIMEOUT_SEC, 10)

        status, text = await self._make_http_request(method, url, headers, body, timeout_sec)
        if status != 200:
            return Resource(
                phase=TaskExecution.FAILED,
                message=f"Webhook failed with status code {status}, response: {text}",
            )
        final_response = self._build_response(status, text, body, url, show_data, show_url)
        return Resource(
            phase=TaskExecution.SUCCEEDED,
            outputs={"info": final_response},
            message="Webhook was successfully invoked!",
        )


ConnectorRegistry.register(WebhookConnector())
