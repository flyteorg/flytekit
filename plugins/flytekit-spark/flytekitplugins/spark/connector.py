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
DEFAULT_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER_ENV_KEY = "FLYTE_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER"


@dataclass
class DatabricksJobMetadata(ResourceMeta):
    databricks_instance: str
    run_id: str


def _is_serverless_config(databricks_job: dict) -> bool:
    """
    Detect if the configuration is for serverless compute.
    Serverless is indicated by having environment_key or environments without cluster config.
    """
    # Check if cluster config keys exist (even empty dict counts as cluster config)
    has_cluster_config = "existing_cluster_id" in databricks_job or "new_cluster" in databricks_job
    has_serverless_config = bool(databricks_job.get("environment_key") or databricks_job.get("environments"))
    return not has_cluster_config and has_serverless_config


def _configure_serverless(databricks_job: dict, envs: dict) -> str:
    """
    Configure serverless compute settings and return the environment_key to use.

    Databricks serverless requires the 'environments' array to be defined in the job
    submission. This function ensures the environments array exists and injects
    Flyte environment variables.

    Args:
        databricks_job: The databricks job configuration dict
        envs: Environment variables to inject

    Returns:
        The environment_key to use for the task
    """
    environment_key = databricks_job.get("environment_key", "default")
    environments = databricks_job.get("environments", [])

    # Check if environment already exists in the array
    env_exists = any(env.get("environment_key") == environment_key for env in environments)

    if not env_exists:
        # Create the environment entry - Databricks serverless requires environments
        # to be defined in the job submission (not externally pre-configured)
        new_env = {
            "environment_key": environment_key,
            "spec": {
                "client": "1",  # Required: Databricks serverless client version
            }
        }
        environments.append(new_env)
        databricks_job["environments"] = environments

    # Inject Flyte environment variables into the environment spec
    for env in environments:
        if env.get("environment_key") == environment_key:
            spec = env.setdefault("spec", {})
            existing_env_vars = spec.get("environment_vars", {})
            # Merge Flyte env vars with any existing ones (Flyte vars take precedence)
            merged_env_vars = {**existing_env_vars, **{k: v for k, v in envs.items()}}
            spec["environment_vars"] = merged_env_vars
            break

    # Remove environment_key from top level (it's now in the task definition)
    databricks_job.pop("environment_key", None)

    return environment_key


def _configure_classic_cluster(databricks_job: dict, custom: dict, container, envs: dict) -> None:
    """
    Configure classic compute (existing cluster or new cluster).

    Args:
        databricks_job: The databricks job configuration dict
        custom: The custom config from task template
        container: The container config from task template
        envs: Environment variables to inject
    """
    if databricks_job.get("existing_cluster_id") is not None:
        # Using an existing cluster, no additional configuration needed
        return

    new_cluster = databricks_job.get("new_cluster")
    if new_cluster is None:
        raise ValueError(
            "Either existing_cluster_id, new_cluster, environment_key, or environments must be specified"
        )

    if not new_cluster.get("docker_image"):
        new_cluster["docker_image"] = {"url": container.image}
    if not new_cluster.get("spark_conf"):
        new_cluster["spark_conf"] = custom.get("sparkConf", {})
    if not new_cluster.get("spark_env_vars"):
        new_cluster["spark_env_vars"] = {k: v for k, v in envs.items()}
    else:
        new_cluster["spark_env_vars"].update({k: v for k, v in envs.items()})


def _get_databricks_job_spec(task_template: TaskTemplate) -> dict:
    custom = task_template.custom
    container = task_template.container
    envs = task_template.container.env
    envs[FLYTE_FAIL_ON_ERROR] = "true"
    databricks_job = custom["databricksConf"]

    # Check if this is a notebook task
    notebook_path = custom.get("notebookPath")
    notebook_base_parameters = custom.get("notebookBaseParameters", {})

    # Determine compute mode and configure accordingly
    is_serverless = _is_serverless_config(databricks_job)

    if notebook_path:
        # Notebook task - runs a Databricks notebook
        notebook_task = {
            "notebook_path": notebook_path,
        }
        if notebook_base_parameters:
            notebook_task["base_parameters"] = notebook_base_parameters
        
        # Check if notebook should be sourced from git
        user_git_source = databricks_job.get("git_source")
        if user_git_source:
            notebook_task["source"] = "GIT"
            # Set git_source at job level
            databricks_job["git_source"] = user_git_source

        if is_serverless:
            # Serverless notebook task
            environment_key = _configure_serverless(databricks_job, envs)
            
            task_def = {
                "task_key": "flyte_notebook_task",
                "notebook_task": notebook_task,
                "environment_key": environment_key,
            }
            databricks_job["tasks"] = [task_def]
            databricks_job.pop("environment_key", None)
        else:
            # Classic compute notebook task
            _configure_classic_cluster(databricks_job, custom, container, envs)
            databricks_job["notebook_task"] = notebook_task
        
        # Clean up git_source from databricks_job if it was there (already set at job level)
        databricks_job.pop("git_source", None)
        if user_git_source:
            databricks_job["git_source"] = user_git_source

        return databricks_job

    # Python file task (original behavior)
    # Allow custom git_source and python_file override from user config
    user_git_source = databricks_job.get("git_source")
    user_python_file = databricks_job.get("python_file")

    # Default entrypoints from the flytetools repo.
    # Both classic and serverless use the same repo; only the python_file differs.
    default_git_source = {
        "git_url": "https://github.com/flyteorg/flytetools",
        "git_provider": "gitHub",
        "git_commit": "572298df1f971fb58c258398bd70a6372f811c96",
    }
    default_classic_python_file = "flytekitplugins/databricks/entrypoint.py"
    default_serverless_python_file = "flytekitplugins/databricks/entrypoint_serverless.py"

    if is_serverless:
        # Serverless compute - use flytetools serverless entrypoint by default
        git_source = user_git_source if user_git_source else default_git_source
        python_file = user_python_file if user_python_file else default_serverless_python_file

        # Serverless requires multi-task format with tasks array
        environment_key = _configure_serverless(databricks_job, envs)

        # Build parameters list - append credential provider if specified
        # This allows the entrypoint to receive the credential provider via command line
        # We append at the END to avoid breaking pyflyte-fast-execute which must be first
        parameters = list(container.args) if container.args else []
        
        # Resolve service credential provider: task config > env var
        service_credential_provider = custom.get(
            "databricksServiceCredentialProvider",
            os.getenv(DEFAULT_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER_ENV_KEY)
        )
        if service_credential_provider:
            # Append as a special argument that the entrypoint will parse and remove
            parameters.append(f"--flyte-credential-provider={service_credential_provider}")

        spark_python_task = {
            "python_file": python_file,
            "source": "GIT",
            "parameters": parameters,
        }

        # Build the task definition for serverless
        task_def = {
            "task_key": "flyte_task",
            "spark_python_task": spark_python_task,
            "environment_key": environment_key,
        }

        # Add tasks array for serverless (required by Databricks API)
        databricks_job["tasks"] = [task_def]

        # Remove environment_key from top level (it's now in the task)
        databricks_job.pop("environment_key", None)
    else:
        # Classic compute - use flytetools entrypoint by default
        git_source = user_git_source if user_git_source else default_git_source
        python_file = user_python_file if user_python_file else default_classic_python_file

        spark_python_task = {
            "python_file": python_file,
            "source": "GIT",
            "parameters": container.args,
        }

        _configure_classic_cluster(databricks_job, custom, container, envs)
        databricks_job["spark_python_task"] = spark_python_task

    # Set git_source (remove from user config if it was there to avoid duplication)
    databricks_job.pop("git_source", None)
    databricks_job.pop("python_file", None)
    databricks_job["git_source"] = git_source

    return databricks_job


class DatabricksConnector(AsyncConnectorBase):
    name = "Databricks Connector"

    def __init__(self):
        super().__init__(task_type_name="spark", metadata_type=DatabricksJobMetadata)

    async def create(
        self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> DatabricksJobMetadata:
        data = json.dumps(_get_databricks_job_spec(task_template))
        databricks_instance = task_template.custom.get(
            "databricksInstance", os.getenv(DEFAULT_DATABRICKS_INSTANCE_ENV_KEY)
        )

        if not databricks_instance:
            raise ValueError(
                f"Missing databricks instance. Please set the value through the task config or set the {DEFAULT_DATABRICKS_INSTANCE_ENV_KEY} environment variable in the connector."
            )

        databricks_url = f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/submit"

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=get_header(), data=data) as resp:
                response = await resp.json()
                if resp.status != http.HTTPStatus.OK:
                    raise RuntimeError(f"Failed to create databricks job with error: {response}")

        return DatabricksJobMetadata(databricks_instance=databricks_instance, run_id=str(response["run_id"]))

    async def get(self, resource_meta: DatabricksJobMetadata, **kwargs) -> Resource:
        databricks_instance = resource_meta.databricks_instance
        databricks_url = (
            f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/get?run_id={resource_meta.run_id}"
        )

        async with aiohttp.ClientSession() as session:
            async with session.get(databricks_url, headers=get_header()) as resp:
                if resp.status != http.HTTPStatus.OK:
                    raise RuntimeError(f"Failed to get databricks job {resource_meta.run_id} with error: {resp.reason}")
                response = await resp.json()

        cur_phase = TaskExecution.UNDEFINED
        message = ""
        state = response.get("state")

        # The databricks job's state is determined by life_cycle_state and result_state.
        # https://docs.databricks.com/en/workflows/jobs/jobs-2.0-api.html#runresultstate
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
                    raise RuntimeError(
                        f"Failed to cancel databricks job {resource_meta.run_id} with error: {resp.reason}"
                    )
                await resp.json()


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
