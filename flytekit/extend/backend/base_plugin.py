from abc import abstractmethod
from typing import List, Optional

from pydantic import BaseModel

PENDING = "pending"
SUCCEEDED = "succeeded"
RUNNING = "running"


class CreateRequest(BaseModel):
    inputs_path: str
    task_template_path: str


class CreateResponse(BaseModel):
    job_id: str
    message: Optional[str]


class PollRequest(BaseModel):
    job_id: str
    output_prefix: str
    prev_state: str


class PollResponse(BaseModel):
    state: str
    message: Optional[str]


class BackendPluginBase:
    def __init__(self, task_type: str):
        self._task_type = task_type

    @property
    def task_type(self) -> str:
        return self._task_type

    @abstractmethod
    async def initialize(self):
        pass

    @abstractmethod
    async def create(self, create_request: CreateRequest) -> CreateResponse:
        pass

    @abstractmethod
    async def poll(self, poll_request: PollRequest) -> PollResponse:
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
    def list_registered_plugins() -> List[BackendPluginBase]:
        return BackendPluginRegistry._REGISTRY


def convert_to_flyte_state(state: str):
    if state.lower() in [PENDING]:
        return PENDING
    if state.lower() in ["done", SUCCEEDED]:
        return SUCCEEDED
    if state.lower() in [RUNNING]:
        return RUNNING
    raise ValueError("Unrecognize state")
