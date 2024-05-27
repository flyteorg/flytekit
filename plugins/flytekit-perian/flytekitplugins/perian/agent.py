from dataclasses import dataclass
import shlex
from typing import Optional

from perian import (
    AcceleratorQueryInput,
    ApiClient,
    CpuQueryInput,
    CreateJobRequest,
    Configuration,
    DockerRegistryCredentials,
    DockerRunParameters,
    InstanceTypeQueryInput,
    JobApi,
    JobStatus,
    MemoryQueryInput,
    Name,
    RegionQueryInput,
)

from flyteidl.core.execution_pb2 import TaskExecution
from flytekit import current_context
from flytekit.exceptions.base import FlyteException
from flytekit.exceptions.user import FlyteUserException
from flytekit.loggers import logger
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta

PERIAN_API_URL = "https://api.perian.cloud"


@dataclass
class PerianMetadata(ResourceMeta):
    """Metadata for Perian jobs"""
    job_id: str


class PerianAgent(AsyncAgentBase):
    """Flyte Agent for executing tasks on Perian"""
    name = "Perian Agent"

    def __init__(self):
        logger.info("Initializing Perian agent")
        super().__init__(task_type_name="perian_task", metadata_type=PerianMetadata)

    def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap],
        output_prefix: Optional[str],
        task_execution_metadata: Optional[TaskExecutionMetadata],
        **kwargs,
    ) -> PerianMetadata:
        logger.info("Creating new Perian job")
        logger.debug("Task template: %s", task_template.__dict__)

        config = Configuration(host=PERIAN_API_URL)
        job_request = self._build_create_job_request(task_template)
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

    def _build_create_job_request(self, task_template: TaskTemplate) -> CreateJobRequest:
        params = task_template.custom
        secrets = current_context().secrets

        # Build instance type requirements
        reqs = InstanceTypeQueryInput()
        if params.get("cores"):
            reqs.cpu = CpuQueryInput(cores=int(params['cores']))
        if params.get("memory"):
            reqs.ram = MemoryQueryInput(size=int(params['memory']))
        if any([params.get("accelerators"), params.get("accelerator_type")]):
            reqs.accelerator = AcceleratorQueryInput()
            if params.get("accelerators"):
                reqs.accelerator.no = int(params["accelerators"])
            if params.get("accelerator_type"):
                reqs.accelerator.name = Name(params['accelerator_type'])
        if params.get("country_code"):
            reqs.region = RegionQueryInput(location=params['country_code'])

        docker_run = DockerRunParameters()
        # Credentials to the Flyte storage S3 bucket need to be passed to the Perian job.
        aws_access_key_id = secrets.get("aws_access_key_id")
        aws_secret_access_key = secrets.get("aws_secret_access_key")
        if not aws_access_key_id or not aws_secret_access_key:
            raise FlyteUserException(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for the Flyte storage bucket "
                "must be provided in the secrets"
            )
        docker_run.env_variables = {
            "AWS_ACCESS_KEY_ID": aws_access_key_id,
            "AWS_SECRET_ACCESS_KEY": aws_secret_access_key,
        }

        docker_registry = None
        try:
            dr_url = secrets.get("docker_registry_url")
            dr_username = secrets.get("docker_registry_username")
            dr_password = secrets.get("docker_registry_password")
            if any([dr_url, dr_username, dr_password]):
                docker_registry = DockerRegistryCredentials(
                    url=dr_url, username=dr_username, password=dr_password,
                )
        except ValueError:
            pass

        container = task_template.container
        if ':' in container.image:
            docker_run.image_name, docker_run.image_tag = container.image.rsplit(':', 1)
        else:
            docker_run.image_name = container.image
        if container.args:
            docker_run.command = shlex.join(container.args)

        #

        return CreateJobRequest(
            auto_failover_instance_type=True,
            requirements=reqs,
            docker_run_parameters=docker_run,
            docker_registry_credentials=docker_registry,
        )

    def _build_headers(self) -> dict:
        secrets = current_context().secrets
        org = secrets.get("perian_organization")
        token = secrets.get("perian_token")
        if not org or not token:
            raise FlyteUserException(
                "perian_organization and perian_token must be provided in the secrets"
            )
        return {
            "X-PERIAN-AUTH-ORG": org,
            "Authorization": "Bearer " + token,
        }

    def _perian_job_status_to_flyte_phase(self, status: JobStatus) -> TaskExecution.Phase:
        status_map = {
            JobStatus.QUEUED: TaskExecution.QUEUED,
            JobStatus.INITIALIZING: TaskExecution.QUEUED,
            JobStatus.RUNNING: TaskExecution.RUNNING,
            JobStatus.DONE: TaskExecution.SUCCEEDED,
            JobStatus.SERVERERROR: TaskExecution.FAILED,
            JobStatus.USERERROR: TaskExecution.FAILED,
            JobStatus.CANCELLED: TaskExecution.ABORTED,
        }
        if status == JobStatus.UNDEFINED:
            raise FlyteException("Undefined Perian job status")
        return status_map[status]


# To register the Perian agent
AgentRegistry.register(PerianAgent())
