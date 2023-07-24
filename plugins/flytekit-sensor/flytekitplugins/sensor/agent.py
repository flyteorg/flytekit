from typing import Optional

import fsspec
import grpc
import msgpack
from flyteidl.admin.agent_pb2 import (
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    GetTaskResponse,
    Resource,
)
from fsspec.utils import get_protocol

from flytekit.configuration import DataConfig
from flytekit.core.data_persistence import s3_setup_args
from flytekitplugins.sensor.task import FileSensorConfig

from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class FileSensorAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="file_sensor")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=msgpack.packb(task_template.custom))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        meta = msgpack.unpackb(resource_meta)
        path = FileSensorConfig(**meta).path

        protocol = get_protocol(path)
        kwargs = {}
        if get_protocol(path):
            kwargs = s3_setup_args(DataConfig.auto().s3, anonymous=False)
        file_system = fsspec.filesystem(protocol, **kwargs)
        if file_system.exists(path):
            cur_state = SUCCEEDED
        else:
            cur_state = RUNNING

        return GetTaskResponse(resource=Resource(state=cur_state, outputs=None))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        # Do Nothing
        return DeleteTaskResponse()


AgentRegistry.register(FileSensorAgent())
