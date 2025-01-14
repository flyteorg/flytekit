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

from .constants import BODY_KEY, HEADERS_KEY, METHOD_KEY, SHOW_BODY_KEY, SHOW_URL_KEY, TASK_TYPE, URL_KEY


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
            url = final_dict.get(URL_KEY)
            body = final_dict.get(BODY_KEY)
            headers = final_dict.get(HEADERS_KEY)
            method = final_dict.get(METHOD_KEY)
            method = http.HTTPMethod(method)
            show_body = final_dict.get(SHOW_BODY_KEY, False)
            show_url = final_dict.get(SHOW_URL_KEY, False)

            session = await self._get_session()

            if method == http.HTTPMethod.GET:
                response = await session.get(url, headers=headers)
            else:
                response = await session.post(url, data=body, headers=headers)
            if response.status != 200:
                return Resource(
                    phase=TaskExecution.FAILED,
                    message=f"Webhook failed with status code {response.status}, response: {response.text}",
                )
            final_response = {
                "status_code": response.status,
                "body": response.text,
            }
            if show_body:
                final_response["input_body"] = body
            if show_url:
                final_response["url"] = url

            return Resource(
                phase=TaskExecution.SUCCEEDED, outputs=final_response, message="Webhook was successfully invoked!"
            )
        except Exception as e:
            return Resource(phase=TaskExecution.FAILED, message=str(e))


AgentRegistry.register(WebhookAgent())
