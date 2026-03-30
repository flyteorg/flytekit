import http
import json
import logging
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
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate

from .utils import is_serverless_config as _is_serverless_config

aiohttp = lazy_module("aiohttp")

logger = logging.getLogger(__name__)

DATABRICKS_API_ENDPOINT = "/api/2.1/jobs"
DEFAULT_DATABRICKS_INSTANCE_ENV_KEY = "FLYTE_DATABRICKS_INSTANCE"
DEFAULT_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER_ENV_KEY = "FLYTE_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER"


@dataclass
class DatabricksJobMetadata(ResourceMeta):
    databricks_instance: str
    run_id: str
    auth_token: Optional[str] = None  # Store auth token for get/delete operations


def _configure_serverless(databricks_job: dict, envs: dict) -> str:
    """
    Configure serverless compute settings and return the environment_key to use.

    Databricks serverless requires the ``environments`` array in the job submission.
    This function ensures the array exists and injects Flyte environment variables
    into the matching environment's ``spec.environment_vars``.

    Reference: https://docs.databricks.com/api/workspace/jobs/submit

    Expected ``environments`` format::

        "environments": [
            {
                "environment_key": "<key>",
                "spec": {
                    "client": "1",
                    "dependencies": ["pandas==2.0.0"],
                    "environment_vars": {"KEY": "VALUE"}
                }
            }
        ]

    Tasks reference an environment via their own ``environment_key`` field,
    analogous to how ``job_cluster_key`` links a task to a shared cluster.

    Args:
        databricks_job (dict): The databricks job configuration dict.
        envs (dict): Environment variables to inject into the environment spec.

    Returns:
        str: The environment_key to use in the task definition.
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
            },
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


def _configure_classic_cluster(databricks_job: dict, custom: dict, container: typing.Any, envs: dict) -> None:
    """
    Configure classic compute (existing cluster or new cluster).

    Args:
        databricks_job (dict): The databricks job configuration dict.
        custom (dict): The custom config from task template.
        container (typing.Any): The container config from task template.
        envs (dict): Environment variables to inject.
    """
    if databricks_job.get("existing_cluster_id") is not None:
        # Using an existing cluster, no additional configuration needed
        return

    new_cluster = databricks_job.get("new_cluster")
    if new_cluster is None:
        return

    if not new_cluster.get("docker_image"):
        new_cluster["docker_image"] = {"url": container.image}
    if not new_cluster.get("spark_conf"):
        new_cluster["spark_conf"] = custom.get("sparkConf", {})
    if not new_cluster.get("spark_env_vars"):
        new_cluster["spark_env_vars"] = {k: v for k, v in envs.items()}
    else:
        new_cluster["spark_env_vars"].update({k: v for k, v in envs.items()})


def _build_notebook_job_spec(
    databricks_job: dict, custom: dict, container: typing.Any, envs: dict, is_serverless: bool
) -> dict:
    """Build the Databricks job spec for a notebook task."""
    notebook_path = custom["notebookPath"]
    notebook_base_parameters = custom.get("notebookBaseParameters", {})

    notebook_task = {"notebook_path": notebook_path}
    if notebook_base_parameters:
        notebook_task["base_parameters"] = notebook_base_parameters

    user_git_source = databricks_job.get("git_source")
    if user_git_source:
        notebook_task["source"] = "GIT"

    if is_serverless:
        environment_key = _configure_serverless(databricks_job, envs)
        task_def = {
            "task_key": "flyte_notebook_task",
            "notebook_task": notebook_task,
            "environment_key": environment_key,
        }
        databricks_job["tasks"] = [task_def]
    else:
        _configure_classic_cluster(databricks_job, custom, container, envs)
        databricks_job["notebook_task"] = notebook_task

    databricks_job.pop("git_source", None)
    if user_git_source:
        databricks_job["git_source"] = user_git_source

    return databricks_job


def _build_python_file_job_spec(
    databricks_job: dict, custom: dict, container: typing.Any, envs: dict, is_serverless: bool
) -> dict:
    """Build the Databricks job spec for a python file (spark_python_task)."""
    user_git_source = databricks_job.get("git_source")
    user_python_file = databricks_job.get("python_file")

    default_git_source = {
        "git_url": "https://github.com/flyteorg/flytetools",
        "git_provider": "gitHub",
        "git_commit": "572298df1f971fb58c258398bd70a6372f811c96",
    }
    default_classic_python_file = "flytekitplugins/databricks/entrypoint.py"
    default_serverless_python_file = "flytekitplugins/databricks/entrypoint_serverless.py"

    if is_serverless:
        git_source = user_git_source or default_git_source
        python_file = user_python_file or default_serverless_python_file

        environment_key = _configure_serverless(databricks_job, envs)

        parameters = list(container.args) if container.args else []

        service_credential_provider = custom.get(
            "databricksServiceCredentialProvider", os.getenv(DEFAULT_DATABRICKS_SERVICE_CREDENTIAL_PROVIDER_ENV_KEY)
        )
        if service_credential_provider:
            parameters.append(f"--flyte-credential-provider={service_credential_provider}")

        spark_python_task = {
            "python_file": python_file,
            "source": "GIT",
            "parameters": parameters,
        }

        task_def = {
            "task_key": "flyte_task",
            "spark_python_task": spark_python_task,
            "environment_key": environment_key,
        }

        databricks_job["tasks"] = [task_def]
    else:
        git_source = user_git_source or default_git_source
        python_file = user_python_file or default_classic_python_file

        spark_python_task = {
            "python_file": python_file,
            "source": "GIT",
            "parameters": container.args,
        }

        _configure_classic_cluster(databricks_job, custom, container, envs)
        databricks_job["spark_python_task"] = spark_python_task

    databricks_job.pop("git_source", None)
    databricks_job.pop("python_file", None)
    databricks_job["git_source"] = git_source

    return databricks_job


def _get_databricks_job_spec(task_template: TaskTemplate) -> dict:
    custom = task_template.custom
    container = task_template.container
    envs = task_template.container.env
    envs[FLYTE_FAIL_ON_ERROR] = "true"
    databricks_job = custom["databricksConf"]

    has_cluster = databricks_job.get("existing_cluster_id") is not None or databricks_job.get("new_cluster") is not None
    has_serverless = bool(databricks_job.get("environment_key") or databricks_job.get("environments"))
    if not has_cluster and not has_serverless:
        raise ValueError(
            "No compute configuration found in databricks_conf. "
            "Provide one of: 'existing_cluster_id' (classic), 'new_cluster' (classic), "
            "'environment_key' (serverless), or 'environments' (serverless)."
        )

    is_serverless = _is_serverless_config(databricks_job)

    if custom.get("notebookPath"):
        return _build_notebook_job_spec(databricks_job, custom, container, envs, is_serverless)

    return _build_python_file_job_spec(databricks_job, custom, container, envs, is_serverless)


class DatabricksConnector(AsyncConnectorBase):
    name = "Databricks Connector"

    def __init__(self):
        super().__init__(task_type_name="spark", metadata_type=DatabricksJobMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        task_execution_metadata: Optional[TaskExecutionMetadata] = None,
        **kwargs,
    ) -> DatabricksJobMetadata:
        data = json.dumps(_get_databricks_job_spec(task_template))
        databricks_instance = task_template.custom.get(
            "databricksInstance", os.getenv(DEFAULT_DATABRICKS_INSTANCE_ENV_KEY)
        )

        if not databricks_instance:
            raise ValueError(
                f"Missing databricks instance. Please set the value through the task config or set the {DEFAULT_DATABRICKS_INSTANCE_ENV_KEY} environment variable in the connector."
            )

        # Get workflow-specific token or fall back to default
        namespace = task_execution_metadata.namespace if task_execution_metadata else None

        # Extract custom secret name from task template (if provided)
        custom_secret_name = task_template.custom.get("databricksTokenSecret")

        logger.info(f"Creating Databricks job for namespace: {namespace or 'unknown'}")
        if custom_secret_name:
            logger.info(f"Using custom secret name: {custom_secret_name}")

        auth_token = get_databricks_token(
            namespace=namespace, task_template=task_template, secret_name=custom_secret_name
        )
        databricks_url = f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/submit"

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=get_header(auth_token=auth_token), data=data) as resp:
                response = await resp.json()
                if resp.status != http.HTTPStatus.OK:
                    raise RuntimeError(f"Failed to create databricks job with error: {response}")

        logger.info(f"Successfully created Databricks job with run_id: {response['run_id']}")
        return DatabricksJobMetadata(
            databricks_instance=databricks_instance, run_id=str(response["run_id"]), auth_token=auth_token
        )

    async def get(self, resource_meta: DatabricksJobMetadata, **kwargs) -> Resource:
        databricks_instance = resource_meta.databricks_instance
        databricks_url = (
            f"https://{databricks_instance}{DATABRICKS_API_ENDPOINT}/runs/get?run_id={resource_meta.run_id}"
        )

        # Use the stored auth token if available, otherwise fall back to default
        headers = get_header(auth_token=resource_meta.auth_token)

        async with aiohttp.ClientSession() as session:
            async with session.get(databricks_url, headers=headers) as resp:
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

        # Use the stored auth token if available, otherwise fall back to default
        headers = get_header(auth_token=resource_meta.auth_token)

        async with aiohttp.ClientSession() as session:
            async with session.post(databricks_url, headers=headers, data=data) as resp:
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


def get_secret_from_k8s(secret_name: str, secret_key: str, namespace: str) -> Optional[str]:
    """Read a secret from Kubernetes using the Kubernetes Python client.

    Args:
        secret_name (str): Name of the Kubernetes secret (e.g., "databricks-token").
        secret_key (str): Key within the secret (e.g., "token").
        namespace (str): Kubernetes namespace where the secret is stored.

    Returns:
        Optional[str]: The secret value as a string, or None if not found.
    """
    try:
        import base64

        from kubernetes import client, config

        # Try to load in-cluster config first (when running in K8s)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            # Fall back to kubeconfig (for local testing)
            try:
                config.load_kube_config()
            except Exception as e:
                logger.warning(f"Failed to load Kubernetes config: {e}")
                return None

        v1 = client.CoreV1Api()

        try:
            secret = v1.read_namespaced_secret(name=secret_name, namespace=namespace)
            if secret.data and secret_key in secret.data:
                # Kubernetes secrets are base64 encoded
                secret_value = base64.b64decode(secret.data[secret_key]).decode("utf-8")
                return secret_value
            else:
                logger.debug(
                    f"Secret '{secret_name}' exists but key '{secret_key}' not found in namespace '{namespace}'"
                )
                return None
        except client.exceptions.ApiException as e:
            if e.status == 404:
                logger.debug(f"Secret '{secret_name}' not found in namespace '{namespace}'")
            else:
                logger.warning(f"Error reading secret '{secret_name}' from namespace '{namespace}': {e}")
            return None

    except ImportError:
        logger.warning("kubernetes Python package not installed - cannot read namespace secrets")
        return None
    except Exception as e:
        logger.warning(f"Unexpected error reading K8s secret: {e}")
        return None


def get_databricks_token(
    namespace: Optional[str] = None, task_template: Optional[TaskTemplate] = None, secret_name: Optional[str] = None
) -> str:
    """Get the Databricks access token with multi-tenant support.

    Token resolution: namespace K8s secret -> FLYTE_DATABRICKS_ACCESS_TOKEN env var.

    Args:
        namespace (Optional[str]): Kubernetes namespace for workflow-specific token lookup.
        task_template (Optional[TaskTemplate]): Optional TaskTemplate (kept for API compatibility).
        secret_name (Optional[str]): Custom secret name. Defaults to 'databricks-token'.

    Returns:
        str: The Databricks access token.

    Raises:
        ValueError: If no token is found from any source.
    """
    token = None
    token_source = "unknown"

    # Use custom secret name or default to 'databricks-token'
    k8s_secret_name = secret_name or "databricks-token"

    # Step 1: Try namespace-specific K8s secret (cross-namespace lookup)
    if namespace:
        logger.info(f"Looking for Databricks token in workflow namespace: {namespace} (secret: {k8s_secret_name})")
        token = get_secret_from_k8s(secret_name=k8s_secret_name, secret_key="token", namespace=namespace)

        if token:
            logger.info(f"Found Databricks token in namespace '{namespace}' from secret '{k8s_secret_name}'")
            token_source = f"k8s_namespace:{namespace}/secret:{k8s_secret_name}"
        else:
            logger.info(
                f"Databricks token not found in secret '{k8s_secret_name}' in namespace '{namespace}' - trying fallback"
            )
    else:
        logger.info("No namespace provided for cross-namespace lookup")

    # Step 2: Fall back to environment variable (backward compatibility)
    if token is None:
        logger.info("Falling back to default Databricks token (FLYTE_DATABRICKS_ACCESS_TOKEN)")
        try:
            token = get_connector_secret("FLYTE_DATABRICKS_ACCESS_TOKEN")
            token_source = "env_variable"
        except Exception as e:
            logger.error(f"Failed to get default Databricks token: {e}")
            raise ValueError(
                "No Databricks token found from any source:\n"
                f"1. Namespace-specific K8s secret '{k8s_secret_name}'\n"
                "2. FLYTE_DATABRICKS_ACCESS_TOKEN environment variable\n"
                f"Workflow namespace: {namespace or 'N/A'}"
            )

    if not token:
        raise ValueError("Databricks token is empty")

    # Log token info without exposing the actual token value
    token_preview = f"{token[:8]}..." if len(token) > 8 else "***"
    logger.info(f"Using Databricks token from: {token_source} (preview: {token_preview})")

    return token


def get_header(task_template: Optional[TaskTemplate] = None, auth_token: Optional[str] = None) -> typing.Dict[str, str]:
    """Get the authorization header for Databricks API calls.

    Args:
        task_template (Optional[TaskTemplate]): TaskTemplate with workflow-specific secret requests.
        auth_token (Optional[str]): Pre-fetched auth token to use directly.

    Returns:
        typing.Dict[str, str]: Authorization and content-type headers.
    """
    if auth_token is None:
        auth_token = get_databricks_token(task_template)

    return {"Authorization": f"Bearer {auth_token}", "content-type": "application/json"}


def result_state_is_available(life_cycle_state: str) -> bool:
    return life_cycle_state == "TERMINATED"


ConnectorRegistry.register(DatabricksConnector())
ConnectorRegistry.register(DatabricksConnectorV2())
