import typing
from abc import abstractmethod

from pydantic import BaseModel


class CreateRequest(BaseModel):
    inputs_path: str
    output_prefix: str
    task_template_path: str
    bq_token_name: str  # token should be saved in the k8s secret


class CreateResponse(BaseModel):
    job_id: str


class PollResponse(BaseModel):
    job_id: str
    state: str


class BackendPluginBase:
    def __init__(self, task_type: str, version: str = "v1"):
        self._task_type = task_type
        self._version = version

    @property
    def task_type(self) -> str:
        return self._task_type

    @property
    def version(self) -> str:
        return self._version

    @abstractmethod
    async def initialize(self):
        pass

    @abstractmethod
    async def create(self, create_request: CreateRequest) -> CreateResponse:
        pass

    @abstractmethod
    async def poll(self, job_id: str) -> PollResponse:
        pass

    @abstractmethod
    async def terminate(self, job_id: str):
        pass


class BackendPluginRegistry(object):
    _REGISTRY = []

    @staticmethod
    def register(plugin: BackendPluginBase):
        BackendPluginRegistry._REGISTRY.append(plugin)

    @staticmethod
    def list_registered_plugins() -> typing.List[BackendPluginBase]:
        return BackendPluginRegistry._REGISTRY


def convert_to_flyte_state(state: str):
    if state.lower() in ["pending"]:
        return "pending"
    if state.lower() in ["done", "succeeded"]:
        return "succeeded"
    if state.lower() in ["running"]:
        return "running"
    raise ValueError("Unrecognize state")
