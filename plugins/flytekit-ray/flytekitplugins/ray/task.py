from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from dataclasses_json import dataclass_json
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins


@dataclass_json
@dataclass
class RayClientConfig(object):
    """
    Client for submitting and interacting with jobs on a remote cluster.
    Submits requests over HTTP to the job server on the cluster using the REST API.
    More detail, see https://docs.ray.io/en/latest/cluster/jobs-package-ref.html#jobsubmissionclient

    :param address: The IP address and port of the head node.
    :param create_cluster_if_needed: Indicates whether the cluster at the specified
     address needs to already be running. Ray doesn't start a cluster
      before interacting with jobs, but external job managers may do so
    :param cookies: Cookies to use when sending requests to the HTTP job server.
    :param metadata: Arbitrary metadata to store along with all jobs.  New metadata
     specified per job will be merged with the global metadata provided here
      via a simple dict update..
    :param headers: Headers to use when sending requests to the HTTP job server, used
     for cases like authentication to a remote cluster..

    """

    address: str
    create_cluster_if_needed: bool = False
    cookies: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, Any]] = None


@dataclass_json
@dataclass
class RayJobSubmissionConfig(object):
    """
    This object contains the settings to talk to a  Ray job submitter.
    More detail, see https://docs.ray.io/en/latest/_modules/ray/dashboard/modules/job/sdk.html#JobSubmissionClient.submit_job

    :param entrypoint: The shell command to run for this job.
    :param job_id: The job ID for the job to be stopped.
    :param runtime_env: The runtime environment to install and run this job in.
    :param metadata: Arbitrary data to store along with this job.

    """

    entrypoint: str
    job_id: Optional[str] = None
    runtime_env: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, str]] = None


@dataclass_json
@dataclass
class RayConfig(object):
    """
    Use this to configure SubmitJobInput for a Ray job. Task's marked with this will automatically execute
    natively onto Ray service.
    """

    ray_client_config: RayClientConfig
    ray_job_submission_config: Optional[RayJobSubmissionConfig]

    def to_dict(self):
        s = Struct()
        s.update(self.to_dict())
        return json_format.MessageToDict(s)


class RayFunctionTask(PythonFunctionTask):
    """
    Actual Plugin that transforms the local python code for execution within Ray job
    """

    _RAY_TASK_TYPE = "ray"

    def __init__(self, task_config: RayConfig, task_function: Callable, **kwargs):
        if task_config is None:
            task_config = RayConfig()
        super().__init__(task_config=task_config, task_type=self._RAY_TASK_TYPE, task_function=task_function, **kwargs)
        self._task_config = task_config

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return self._task_config.to_dict()

    def get_command(self, settings: SerializationSettings) -> List[str]:
        container_args = [
            "pyflyte-execute",
            "--inputs",
            "{{.input}}",
            "--output-prefix",
            "{{.outputPrefix}}",
            "--raw-output-data-prefix",
            "{{.rawOutputDataPrefix}}",
            "--resolver",
            self.task_resolver.location,
            "--",
            *self.task_resolver.loader_args(settings, self),
        ]

        return container_args


# Inject the Ray plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(RayConfig, RayFunctionTask)
