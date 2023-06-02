import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTaskRequest,
    DeleteTaskResponse,
    GetTaskRequest,
    GetTaskResponse,
    Resource,
)
from flyteidl.service.agent_pb2_grpc import AsyncAgentServiceServicer

from flytekit import logger
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class AgentService(AsyncAgentServiceServicer):
    def CreateTask(self, request: CreateTaskRequest, context: grpc.ServicerContext) -> CreateTaskResponse:
        try:
            tmp = TaskTemplate.from_flyte_idl(request.template)
            inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
            agent = AgentRegistry.get_agent(context, tmp.type)
            if agent is None:
                return CreateTaskResponse()
            return agent.create(context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp)
        except Exception as e:
            logger.error(f"failed to create task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")

    def GetTask(self, request: GetTaskRequest, context: grpc.ServicerContext) -> GetTaskResponse:
        try:
            agent = AgentRegistry.get_agent(context, request.task_type)
            if agent is None:
                return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
            return agent.get(context=context, resource_meta=request.resource_meta)
        except Exception as e:
            logger.error(f"failed to get task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")

    def DeleteTask(self, request: DeleteTaskRequest, context: grpc.ServicerContext) -> DeleteTaskResponse:
        try:
            agent = AgentRegistry.get_agent(context, request.task_type)
            if agent is None:
                return DeleteTaskResponse()
            return agent.delete(context=context, resource_meta=request.resource_meta)
        except Exception as e:
            logger.error(f"failed to delete task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
