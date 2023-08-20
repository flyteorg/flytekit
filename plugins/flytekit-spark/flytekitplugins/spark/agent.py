import json
from dataclasses import asdict, dataclass
from typing import Optional

import aiohttp
import grpc
from flyteidl.admin.agent_pb2 import PENDING, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource

import flytekit
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


@dataclass
class Metadata:
    databricks_endpoint: Optional[str]
    databricks_instance: Optional[str]
    token: str
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
        databricks_job = custom["databricks_conf"]
        if not databricks_job["new_cluster"].get("docker_image"):
            databricks_job["new_cluster"]["docker_image"] = {"url": container.image}
        if not databricks_job["new_cluster"].get("spark_conf"):
            databricks_job["new_cluster"]["spark_conf"] = custom["spark_conf"]
        databricks_job["spark_python_task"] = {
            "python_file": custom["applications_path"],
            "parameters": container.args,
        }

        secrets = task_template.security_context.secrets[0]
        ctx = flytekit.current_context()
        token = ctx.secrets.get(group=secrets.group, key=secrets.key, group_version=secrets.group_version)

        response = await send_request(
            method="POST",
            databricks_job=databricks_job,
            databricks_endpoint=custom["databricks_endpoint"],
            databricks_instance=custom["databricks_instance"],
            token=token,
            run_id=None,
            is_cancel=False,
        )

        metadata = Metadata(
            databricks_endpoint=custom["databricks_endpoint"],
            databricks_instance=custom["databricks_instance"],
            token=token,
            run_id=str(response["run_id"]),
        )

        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        response = await send_request(
            method="GET",
            databricks_job=None,
            databricks_endpoint=metadata.databricks_endpoint,
            databricks_instance=metadata.databricks_instance,
            token=metadata.token,
            run_id=metadata.run_id,
            is_cancel=False,
        )

        cur_state = PENDING
        if response["state"].get("result_state"):
            cur_state = convert_to_flyte_state(response["state"]["result_state"])

        return GetTaskResponse(resource=Resource(state=cur_state))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))

        await send_request(
            method="POST",
            databricks_job=None,
            databricks_endpoint=metadata.databricks_endpoint,
            databricks_instance=metadata.databricks_instance,
            token=metadata.token,
            run_id=metadata.run_id,
            is_cancel=True,
        )

        return DeleteTaskResponse()


async def send_request(
    method: str,
    databricks_job: dict,
    databricks_endpoint: Optional[str],
    databricks_instance: Optional[str],
    token: str,
    run_id: Optional[str],
    is_cancel: bool,
) -> dict:
    databricksAPI = "/api/2.0/jobs/runs"
    post = "POST"
    get = "GET"
    data = None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if not databricks_endpoint:
        databricks_url = f"https://{databricks_instance}{databricksAPI}"
    else:
        databricks_url = f"{databricks_endpoint}{databricksAPI}"

    if is_cancel:
        databricks_url += "/cancel"
        data = json.dumps({"run_id": run_id})
    elif method == post:
        databricks_url += "/submit"
        try:
            data = json.dumps(databricks_job)
        except json.JSONDecodeError:
            raise ValueError("Failed to marshal databricksJob to JSON")
    elif method == get:
        databricks_url += f"/get?run_id={run_id}"

    async with aiohttp.ClientSession() as session:
        if method == post:
            async with session.post(databricks_url, headers=headers, data=data) as resp:
                return await resp.json()
        elif method == get:
            async with session.get(databricks_url, headers=headers) as resp:
                return await resp.json()


AgentRegistry.register(DatabricksAgent())
