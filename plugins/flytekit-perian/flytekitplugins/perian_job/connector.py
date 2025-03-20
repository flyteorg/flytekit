import base64
import shlex
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from flyteidl.core.execution_pb2 import TaskExecution
from perian import (
    AcceleratorQueryInput,
    ApiClient,
    Configuration,
    CpuQueryInput,
    CreateJobRequest,
    DockerRegistryCredentials,
    DockerRunParameters,
    InstanceTypeQueryInput,
    JobApi,
    JobStatus,
    MemoryQueryInput,
    Name,
    OSStorageConfig,
    ProviderQueryInput,
    RegionQueryInput,
    Size,
)

from flytekit import current_context
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.base_connector import AsyncConnectorBase, ConnectorRegistry, Resource, ResourceMeta
from flytekit.loggers import logger
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

PERIAN_API_URL = "https://api.perian.cloud"


@dataclass
class PerianMetadata(ResourceMeta):
    """Metadata for PERIAN jobs"""

    job_id: str


class PerianConnector(AsyncConnectorBase):
    """Flyte Connector for executing tasks on PERIAN Job Platform"""

    name = "Perian Connector"

    def __init__(self):
        logger.info("Initializing Perian connector")
        super().__init__(task_type_name="perian_task", metadata_type=PerianMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap],
        output_prefix: Optional[str],
        **kwargs,
    ) -> PerianMetadata:
        logger.info("Creating new Perian job")
        ctx = current_context()
        literal_types = task_template.interface.inputs
        input_kwargs = (
            TypeEngine.literal_map_to_kwargs(ctx, inputs, literal_types=literal_types) if inputs.literals else None
        )
        config = Configuration(host=PERIAN_API_URL)
        job_request = self._build_create_job_request(task_template, input_kwargs)
        with ApiClient(config) as api_client:
            api_instance = JobApi(api_client)
            response = api_instance.create_job(
                create_job_request=job_request,
                _headers=self._build_headers(),
            )
        if response.status_code != 200:
            raise FlyteException(f"Failed to create Perian job: {response.text}")

        return PerianMetadata(job_id=response.id)

    def get(self, resource_meta: PerianMetadata, **kwargs) -> Resource:
        job_id = resource_meta.job_id
        logger.info("Getting Perian job status: %s", job_id)
        config = Configuration(host=PERIAN_API_URL)
        with ApiClient(config) as api_client:
            api_instance = JobApi(api_client)
            response = api_instance.get_job_by_id(
                job_id=str(job_id),
                _headers=self._build_headers(),
            )
        if response.status_code != 200:
            raise FlyteException(f"Failed to get Perian job status: {response.text}")
        if not response.jobs:
            raise FlyteException(f"Perian job not found: {job_id}")
        job = response.jobs[0]

        return Resource(
            phase=self._perian_job_status_to_flyte_phase(job.status),
            message=job.logs,
        )

    def delete(self, resource_meta: PerianMetadata, **kwargs):
        job_id = resource_meta.job_id
        logger.info("Cancelling Perian job: %s", job_id)
        config = Configuration(host=PERIAN_API_URL)
        with ApiClient(config) as api_client:
            api_instance = JobApi(api_client)
            response = api_instance.cancel_job(
                job_id=str(job_id),
                _headers=self._build_headers(),
            )
        if response.status_code != 200:
            raise FlyteException(f"Failed to cancel Perian job: {response.text}")

    def _build_create_job_request(
        self, task_template: TaskTemplate, inputs: Optional[Dict[str, Any]]
    ) -> CreateJobRequest:
        params = task_template.custom
        secrets = current_context().secrets

        # Build instance type requirements
        reqs = InstanceTypeQueryInput()
        if params.get("cores"):
            reqs.cpu = CpuQueryInput(cores=int(params["cores"]))
        if params.get("memory"):
            reqs.ram = MemoryQueryInput(size=Size(params["memory"]))
        if any([params.get("accelerators"), params.get("accelerator_type")]):
            reqs.accelerator = AcceleratorQueryInput()
            if params.get("accelerators"):
                reqs.accelerator.no = int(params["accelerators"])
            if params.get("accelerator_type"):
                reqs.accelerator.name = Name(params["accelerator_type"])
        if params.get("country_code"):
            reqs.region = RegionQueryInput(location=params["country_code"])
        if params.get("provider"):
            reqs.provider = ProviderQueryInput(name_short=params["provider"])

        docker_run = self._read_storage_credentials()

        docker_registry = None
        try:
            dr_url = secrets.get(key="docker_registry_url")
            dr_username = secrets.get(key="docker_registry_username")
            dr_password = secrets.get(key="docker_registry_password")
            if any([dr_url, dr_username, dr_password]):
                docker_registry = DockerRegistryCredentials(
                    url=dr_url,
                    username=dr_username,
                    password=dr_password,
                )
        except ValueError:
            pass

        container = task_template.container
        if container:
            image = container.image
        else:
            image = params["image"]
        if ":" in image:
            docker_run.image_name, docker_run.image_tag = image.rsplit(":", 1)
        else:
            docker_run.image_name = image

        if container:
            command = container.args
        else:
            command = self._render_command_template(params["command"], inputs)
        if command:
            docker_run.command = shlex.join(command)

        if params.get("environment"):
            if docker_run.env_variables:
                docker_run.env_variables.update(params["environment"])
            else:
                docker_run.env_variables = params["environment"]

        storage_config = None
        if params.get("os_storage_size"):
            storage_config = OSStorageConfig(size=int(params["os_storage_size"]))

        return CreateJobRequest(
            auto_failover_instance_type=True,
            requirements=reqs,
            docker_run_parameters=docker_run,
            docker_registry_credentials=docker_registry,
            os_storage_config=storage_config,
        )

    def _render_command_template(self, command: List[str], inputs: Optional[Dict[str, Any]]) -> List[str]:
        if not inputs:
            return command
        rendered_command = []
        for c in command:
            for key, val in inputs.items():
                c = c.replace("{{.inputs." + key + "}}", str(val))
            rendered_command.append(c)
        return rendered_command

    def _read_storage_credentials(self) -> DockerRunParameters:
        secrets = current_context().secrets
        docker_run = DockerRunParameters()
        # AWS
        try:
            aws_access_key_id = secrets.get(key="aws_access_key_id")
            aws_secret_access_key = secrets.get(key="aws_secret_access_key")
            docker_run.secrets = {
                "AWS_ACCESS_KEY_ID": aws_access_key_id,
                "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
            }
            return docker_run
        except ValueError:
            pass
        # GCP
        try:
            creds_file = "/data/gcp-credentials.json"  # to be mounted in the container
            google_application_credentials = secrets.get(key="google_application_credentials")
            docker_run.secrets = {
                "GOOGLE_APPLICATION_CREDENTIALS": creds_file,
            }
            docker_run.container_files = [
                {
                    "path": creds_file,
                    "base64_content": base64.b64encode(google_application_credentials.encode()).decode(),
                }
            ]
            return docker_run
        except ValueError:
            pass

        raise FlyteUserException(
            "To access the Flyte storage bucket, `aws_access_key_id` and `aws_secret_access_key` for AWS "
            "or `google_application_credentials` for GCP must be provided in the secrets"
        )

    def _build_headers(self) -> dict:
        secrets = current_context().secrets
        org = secrets.get(key="perian_organization")
        token = secrets.get(key="perian_token")
        if not org or not token:
            raise FlyteUserException("perian_organization and perian_token must be provided in the secrets")
        return {
            "X-PERIAN-AUTH-ORG": org,
            "Authorization": "Bearer " + token,
        }

    def _perian_job_status_to_flyte_phase(self, status: JobStatus) -> TaskExecution.Phase:
        status_map = {
            JobStatus.QUEUED: TaskExecution.QUEUED,
            JobStatus.INITIALIZING: TaskExecution.INITIALIZING,
            JobStatus.RUNNING: TaskExecution.RUNNING,
            JobStatus.DONE: TaskExecution.SUCCEEDED,
            JobStatus.SERVERERROR: TaskExecution.FAILED,
            JobStatus.USERERROR: TaskExecution.FAILED,
            JobStatus.CANCELLED: TaskExecution.ABORTED,
        }
        if status == JobStatus.UNDEFINED:
            raise FlyteException("Undefined Perian job status")
        return status_map[status]


# To register the Perian connector
ConnectorRegistry.register(PerianConnector())
