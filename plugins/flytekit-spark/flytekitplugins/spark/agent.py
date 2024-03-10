import http
import json
import typing
from dataclasses import dataclass
from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import lazy_module
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_agent_secret
from flytekit.models.core.execution import TaskLog
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

aiohttp = lazy_module("aiohttp")

DATABRICKS_API_ENDPOINT = "/api/2.1/jobs"


@dataclass
class DatabricksJobMetadata(ResourceMeta):
    databricks_instance: str
    run_id: str


class DatabricksAgent(AsyncAgentBase):
    name = "Databricks Agent"

    def __init__(self):
        super().__init__(task_type_name="spark", metadata_type=DatabricksJobMetadata)

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> DatabricksJobMetadata:
        custom = task_template.custom
        container = task_template.container
        databricks_job = custom["databricksConf"]
        if databricks_job.get("existing_cluster_id") is None:
            new_cluster = databricks_job.get("new_cluster")
            if new_cluster is None:
                raise Exception("Either existing_cluster_id or new_cluster must be specified")
            if not new_cluster.get("docker_image"):
                new_cluster["docker_image"] = {"url": container.image}
            if not new_cluster.get("spark_conf"):
                new_cluster["spark_conf"] = custom["sparkConf"]
        # https://docs.databricks.com/api/workspace/jobs/submit
        databricks_job["spark_python_task"] = {
            "python_file": "flytekitplugins/spark/entrypoint.py",
            "source": "GIT",
            "parameters": container.args,
        }
        databricks_job["git_source"] = {
            "git_url": "https://github.com/flyteorg/flytetools",
            "git_provider": "gitHub",
            # https://github.com/flyteorg/flytetools/commit/aff8a9f2adbf5deda81d36d59a0b8fa3b1fc3679
            "git_commit": "aff8a9f2adbf5deda81d36d59a0b8fa3b1fc3679",
        }

        databricks_instance = custom["databricksInstance"]
        databricks_url = f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/submit"
        data = json.dumps(databricks_job)

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=get_header(), data=data) as resp:
                response = await resp.json()
                if resp.status != http.HTTPStatus.OK:
                    raise Exception(f"Failed to create databricks job with error: {response}")

        return DatabricksJobMetadata(databricks_instance=databricks_instance, run_id=str(response["run_id"]))

    async def get(self, resource_meta: DatabricksJobMetadata, **kwargs) -> Resource:
        databricks_instance = resource_meta.databricks_instance
        databricks_url = (
            f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/get?run_id={resource_meta.run_id}"
        )

        async with aiohttp.ClientSession() as session:
            async with session.get(databricks_url, headers=get_header()) as resp:
                if resp.status != http.HTTPStatus.OK:
                    raise Exception(f"Failed to get databricks job {resource_meta.run_id} with error: {resp.reason}")
                response = await resp.json()

        cur_phase = TaskExecution.UNDEFINED
        message = ""
        state = response.get("state")

        # The databricks job's state is determined by life_cycle_state and result_state. https://docs.databricks.com/en/workflows/jobs/jobs-2.0-api.html#runresultstate
        if state:
            life_cycle_state = state.get("life_cycle_state")
            if result_state_is_available(life_cycle_state):
                result_state = state.get("result_state")
                cur_phase = convert_to_flyte_phase(result_state)
            else:
                cur_phase = convert_to_flyte_phase(life_cycle_state)

            message = state.get("state_message")

        job_id = response.get("job_id")
        databricks_console_url = f"https://{databricks_instance}/#job/{job_id}/run/{resource_meta.run_id}"
        log_links = [TaskLog(uri=databricks_console_url, name="Databricks Console").to_flyte_idl()]

        return Resource(phase=cur_phase, message=message, log_links=log_links)

    async def delete(self, resource_meta: DatabricksJobMetadata, **kwargs):
        databricks_url = f"https://{resource_meta.databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/cancel"
        data = json.dumps({"run_id": resource_meta.run_id})

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=get_header(), data=data) as resp:
                if resp.status != http.HTTPStatus.OK:
                    raise Exception(f"Failed to cancel databricks job {resource_meta.run_id} with error: {resp.reason}")
                await resp.json()


def get_header() -> typing.Dict[str, str]:
    token = get_agent_secret("FLYTE_DATABRICKS_ACCESS_TOKEN")
    return {"Authorization": f"Bearer {token}", "content-type": "application/json"}


def result_state_is_available(life_cycle_state: str) -> bool:
    return life_cycle_state == "TERMINATED"


AgentRegistry.register(DatabricksAgent())
