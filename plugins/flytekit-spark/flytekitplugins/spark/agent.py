import json
import pickle
import typing
from dataclasses import dataclass
from typing import Optional

import aiohttp
import grpc
from flyteidl.admin.agent_pb2 import PENDING, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource

from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state, get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class Metadata:
    databricks_instance: str
    run_id: str


class DatabricksAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="spark")

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        custom = task_template.custom
        container = task_template.container
        databricks_job = custom["databricksConf"]
        if not databricks_job["new_cluster"].get("docker_image"):
            databricks_job["new_cluster"]["docker_image"] = {"url": container.image}
        if not databricks_job["new_cluster"].get("spark_conf"):
            databricks_job["new_cluster"]["spark_conf"] = custom["sparkConf"]
        databricks_job["spark_python_task"] = {
            "python_file": custom["mainApplicationFile"],
            "parameters": tuple(container.args),
        }

        databricks_instance = custom["databricksInstance"]
        databricks_url = f"https://{databricks_instance}/api/2.0/jobs/runs/submit"
        data = json.dumps(databricks_job)

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=get_header(), data=data) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to create databricks job with error: {resp.reason}")
                response = await resp.json()

        metadata = Metadata(
            databricks_instance=databricks_instance,
            run_id=str(response["run_id"]),
        )
        return CreateTaskResponse(resource_meta=pickle.dumps(metadata))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = pickle.loads(resource_meta)
        databricks_instance = metadata.databricks_instance
        databricks_url = f"https://{databricks_instance}/api/2.0/jobs/runs/get?run_id={metadata.run_id}"

        async with aiohttp.ClientSession() as session:
            async with session.get(databricks_url, headers=get_header()) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to get databricks job {metadata.run_id} with error: {resp.reason}")
                response = await resp.json()

        cur_state = PENDING
        if response.get("state") and response["state"].get("result_state"):
            cur_state = convert_to_flyte_state(response["state"]["result_state"])

        return GetTaskResponse(resource=Resource(state=cur_state))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = pickle.loads(resource_meta)

        databricks_url = f"https://{metadata.databricks_instance}/api/2.0/jobs/runs/cancel"
        data = json.dumps({"run_id": metadata.run_id})

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=get_header(), data=data) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to cancel databricks job {metadata.run_id} with error: {resp.reason}")
                await resp.json()

        return DeleteTaskResponse()


def get_header() -> typing.Dict[str, str]:
    token = get_agent_secret("FLYTE_DATABRICKS_ACCESS_TOKEN")
    return {"Authorization": f"Bearer {token}", "content-type": "application/json"}


AgentRegistry.register(DatabricksAgent())
