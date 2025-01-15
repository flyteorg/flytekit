import asyncio
import http
from typing import Optional

import aiohttp
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit.extend.backend.base_agent import AgentRegistry, Resource, SyncAgentBase
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.utils.dict_formatter import format_dict

from .constants import DATA_KEY, HEADERS_KEY, METHOD_KEY, SHOW_DATA_KEY, SHOW_URL_KEY, TASK_TYPE, URL_KEY


class WebhookAgent(SyncAgentBase):
    name = "Webhook Agent"

    def __init__(self):
        super().__init__(task_type_name=TASK_TYPE)
        self._session = None
        self._lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None:
                self._session = aiohttp.ClientSession()
            return self._session

    async def do(
        self, task_template: TaskTemplate, output_prefix: str, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> Resource:
        try:
            custom_dict = task_template.custom
            input_dict = {
                "inputs": literal_map_string_repr(inputs),
            }

            final_dict = format_dict("test", custom_dict, input_dict)
            return await self._process_webhook(final_dict)
        except aiohttp.ClientError as e:
            return Resource(phase=TaskExecution.FAILED, message=str(e))

    async def _make_http_request(self, method: http.HTTPMethod, url: str, headers: dict, data: dict = None) -> tuple:
        session = await self._get_session()
        if method == http.HTTPMethod.GET:
            response = await session.get(url, headers=headers, params=data)
        else:
            response = await session.post(url, json=data, headers=headers)
        text = await response.text()
        return response, text

    @staticmethod
    def _build_response(
        response: aiohttp.ClientResponse,
        text: str,
        data: dict = None,
        url: str = None,
        show_data: bool = False,
        show_url: bool = False,
    ) -> dict:
        final_response = {
            "status_code": response.status,
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
        method = http.HTTPMethod(final_dict.get(METHOD_KEY))
        show_data = final_dict.get(SHOW_DATA_KEY, False)
        show_url = final_dict.get(SHOW_URL_KEY, False)
        response, text = await self._make_http_request(method, url, headers, body)
        if response.status != 200:
            return Resource(
                phase=TaskExecution.FAILED,
                message=f"Webhook failed with status code {response.status}, response: {text}",
            )
        final_response = self._build_response(response, text, body, url, show_data, show_url)
        return Resource(
            phase=TaskExecution.SUCCEEDED,
            outputs={"info": final_response},
            message="Webhook was successfully invoked!",
        )


AgentRegistry.register(WebhookAgent())
