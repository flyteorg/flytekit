import json
from dataclasses import asdict, dataclass
from typing import Optional

import aiohttp
import grpc
from flyteidl.admin.agent_pb2 import (
    PENDING,
    PERMANENT_FAILURE,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.models.types import LiteralType, StructuredDatasetType


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
        print("@@@ custom")
        for attr_name, attr_value in custom.items():
            print(f"{type(attr_value)} {attr_name}: {attr_value}")
        """
        add 1.docker image 2.spark config 3.spark python task
        all of them into databricks_job
        """

        databricks_job = custom["databricks_conf"]
        # note: current docker image does not support basic auth
        # todo image and arguments
        # if not databricks_job["new_cluster"].get("docker_image"):
        #     databricks_job["new_cluster"]["docker_image"] = {}
        #     databricks_job["new_cluster"]["docker_image"]["url"] = container.image
        if not databricks_job["new_cluster"].get("spark_conf"):
            databricks_job["new_cluster"]["spark_conf"] = custom["spark_conf"]
        databricks_job["spark_python_task"] = {
            "python_file": custom["applications_path"],
            "parameters": container.args,
        }

        print("@@@ databricks_job")
        for attr_name, attr_value in databricks_job.items():
            print(f"{attr_name}: {attr_value}")

        # with open("/mnt/c/code/dev/example/plugins/databricks.json", "w") as f:
        #     f.write(str(databricks_job))
        # json.dump(databricks_job, json_file)
        response = await build_request(
            method="POST",
            databricks_job=databricks_job,
            databricks_endpoint=custom["databricks_endpoint"],
            databricks_instance=custom["databricks_instance"],
            token=custom["token"],
            run_id="",
            is_cancel=False,
        )

        print("response:", response)
        print(type(response["run_id"]))
        metadata = Metadata(
            databricks_endpoint=custom["databricks_endpoint"],
            databricks_instance=custom["databricks_instance"],
            token=custom["token"],
            run_id=str(response["run_id"]),
        )
        return CreateTaskResponse(resource_meta=json.dumps(asdict(metadata)).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        response = await build_request(
            method="GET",
            databricks_job=None,
            databricks_endpoint=metadata.databricks_endpoint,
            databricks_instance=metadata.databricks_instance,
            token=metadata.token,
            run_id=metadata.run_id,
            is_cancel=False,
        )
        print("response:", response)
        print("@@@ response")
        for attr_name, attr_value in response.items():
            print(f"{type(attr_value)} {attr_name}: {attr_value}")

        """
        jobState := data["state"].(map[string]interface{})
        message := fmt.Sprintf("%s", jobState["state_message"])
        jobID := fmt.Sprintf("%.0f", data["job_id"])
        lifeCycleState := fmt.Sprintf("%s", jobState["life_cycle_state"])
        resultState := fmt.Sprintf("%s", jobState["result_state"])


        response->state->state_message
                       ->life_cycle_state
                       ->result_state
                ->job_id
        """

        """
        cur_state = response["state"]["result_state"]

        SUCCESS
        FAILED
        TIMEDOUT
        CANCELED
        """

        cur_state = PENDING
        if response["state"].get("result_state"):
            if response["state"]["result_state"] == "SUCCESS":
                cur_state = SUCCEEDED
            else:
                context.set_code(grpc.StatusCode.INTERNAL)
                return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))

        res = None
        if cur_state == SUCCEEDED:
            ctx = FlyteContextManager.current_context()
            # output page_url and task_output
            res = literals.LiteralMap({}).to_flyte_idl()
        return GetTaskResponse(resource=Resource(state=cur_state, outputs=res))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        metadata = Metadata(**json.loads(resource_meta.decode("utf-8")))
        await build_request(
            method="POST",
            databricks_job=None,
            databricks_endpoint=metadata.databricks_endpoint,
            databricks_instance=metadata.databricks_instance,
            token=metadata.token,
            run_id=metadata.run_id,
            is_cancel=True,
        )
        return DeleteTaskResponse()


async def build_request(
    method: str,
    databricks_job: dict,
    databricks_endpoint: str,
    databricks_instance: str,
    token: str,
    run_id: str,
    is_cancel: bool,
) -> dict:
    databricksAPI = "/api/2.0/jobs/runs"
    post = "POST"

    # Build the databricks URL
    if not databricks_endpoint:
        databricks_url = f"https://{databricks_instance}{databricksAPI}"
    else:
        databricks_url = f"{databricks_endpoint}{databricksAPI}"

    data = None
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    if is_cancel:
        databricks_url += "/cancel"
        data = json.dumps({"run_id": run_id})
    elif method == post:
        databricks_url += "/submit"
        try:
            data = json.dumps(databricks_job)
        except json.JSONDecodeError:
            raise ValueError("Failed to marshal databricksJob to JSON")
    else:
        databricks_url += f"/get?run_id={run_id}"

    print(databricks_url)
    async with aiohttp.ClientSession() as session:
        if method == post:
            async with session.post(databricks_url, headers=headers, data=data) as resp:
                return await resp.json()
        else:
            async with session.get(databricks_url, headers=headers) as resp:
                return await resp.json()


AgentRegistry.register(DatabricksAgent())
