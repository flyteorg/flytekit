import asyncio
import http
import json
import os
import typing
from dataclasses import dataclass
from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import lazy_module
from flytekit.core.constants import FLYTE_FAIL_ON_ERROR
from flytekit.extend.backend.base_connector import AsyncConnectorBase, ConnectorRegistry, Resource, ResourceMeta
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_connector_secret
from flytekit.models.core.execution import TaskLog
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

aiohttp = lazy_module("aiohttp")

DATABRICKS_API_ENDPOINT = "/api/2.1/jobs"
DEFAULT_DATABRICKS_INSTANCE_ENV_KEY = "FLYTE_DATABRICKS_INSTANCE"


@dataclass
class DatabricksJobMetadata(ResourceMeta):
    databricks_instance: str
    run_id: str


def _get_databricks_job_spec(task_template: TaskTemplate) -> dict:
    custom = task_template.custom
    container = task_template.container
    envs = task_template.container.env
    envs[FLYTE_FAIL_ON_ERROR] = "true"
    databricks_job = custom["databricksConf"]
    if databricks_job.get("existing_cluster_id") is None:
        new_cluster = databricks_job.get("new_cluster")
        if new_cluster is None:
            raise ValueError("Either existing_cluster_id or new_cluster must be specified")
        if not new_cluster.get("docker_image"):
            new_cluster["docker_image"] = {"url": container.image}
        if not new_cluster.get("spark_conf"):
            new_cluster["spark_conf"] = custom.get("sparkConf", {})
        if not new_cluster.get("spark_env_vars"):
            new_cluster["spark_env_vars"] = {k: v for k, v in envs.items()}
        else:
            new_cluster["spark_env_vars"].update({k: v for k, v in envs.items()})
    # https://docs.databricks.com/api/workspace/jobs/submit
    databricks_job["spark_python_task"] = {
        "python_file": "flytekitplugins/databricks/entrypoint.py",
        "source": "GIT",
        "parameters": container.args,
    }
    databricks_job["git_source"] = {
        "git_url": "https://github.com/flyteorg/flytetools",
        "git_provider": "gitHub",
        # https://github.com/flyteorg/flytetools/commit/572298df1f971fb58c258398bd70a6372f811c96
        "git_commit": "572298df1f971fb58c258398bd70a6372f811c96",
    }

    return databricks_job


class DatabricksConnector(AsyncConnectorBase):
    name = "Databricks Connector"

    def __init__(self):
        super().__init__(task_type_name="spark", metadata_type=DatabricksJobMetadata)

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> DatabricksJobMetadata:
        data = json.dumps(_get_databricks_job_spec(task_template))
        # databricks_instance = task_template.custom.get(
        #     "databricksInstance", os.getenv(DEFAULT_DATABRICKS_INSTANCE_ENV_KEY)
        # )
        #
        # if not databricks_instance:
        #     raise ValueError(
        #         f"Missing databricks instance. Please set the value through the task config or set the {DEFAULT_DATABRICKS_INSTANCE_ENV_KEY} environment variable in the connector."
        #     )
        #
        # databricks_url = f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/submit"
        #
        # async with aiohttp.ClientSession() as session:
        #     async with session.post(databricks_url, headers=get_header(), data=data) as resp:
        #         response = await resp.json()
        #         if resp.status != http.HTTPStatus.OK:
        #             raise RuntimeError(f"Failed to create databricks job with error: {response}")

        return DatabricksJobMetadata(databricks_instance="databricks_instance", run_id="run_id")

    async def get(self, resource_meta: DatabricksJobMetadata, **kwargs) -> Resource:
        print("get")
        databricks_instance = resource_meta.databricks_instance

        return Resource(phase=TaskExecution.Phase.SUCCEEDED)

    async def delete(self, resource_meta: DatabricksJobMetadata, **kwargs):

        print("delete")
        return


class DatabricksConnectorV2(DatabricksConnector):
    """
    Add DatabricksConnectorV2 to support running the k8s spark and databricks spark together in the same workflow.
    This is necessary because one task type can only be handled by a single backend plugin.

    spark -> k8s spark plugin
    databricks -> databricks connector
    """

    def __init__(self):
        super(DatabricksConnector, self).__init__(task_type_name="databricks", metadata_type=DatabricksJobMetadata)


def get_header() -> typing.Dict[str, str]:
    token = get_connector_secret("FLYTE_DATABRICKS_ACCESS_TOKEN")
    return {"Authorization": f"Bearer {token}", "content-type": "application/json"}


def result_state_is_available(life_cycle_state: str) -> bool:
    return life_cycle_state == "TERMINATED"


ConnectorRegistry.register(DatabricksConnector())
ConnectorRegistry.register(DatabricksConnectorV2())
