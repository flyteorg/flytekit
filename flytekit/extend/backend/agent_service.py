import grpc
from flyteidl.service.agent_service_pb2 import (
    PERMANENT_FAILURE,
    TaskCreateRequest,
    TaskCreateResponse,
    TaskDeleteRequest,
    TaskDeleteResponse,
    TaskGetRequest,
    TaskGetResponse,
)
from flyteidl.service.agent_service_pb2_grpc import AgentServiceServicer

from flytekit import logger
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class AgentService(AgentServiceServicer):
    def CreateTask(self, request: TaskCreateRequest, context: grpc.ServicerContext) -> TaskCreateResponse:
        try:
            tmp = TaskTemplate.from_flyte_idl(request.template)
            inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
            agent = AgentRegistry.get_agent(context, tmp.type)
            if agent is None:
                return TaskCreateResponse()
            return agent.create(context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp)
        except Exception as e:
            logger.error(f"failed to create task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")

    def GetTask(self, request: TaskGetRequest, context: grpc.ServicerContext) -> TaskGetResponse:
        try:
            agent = AgentRegistry.get_agent(context, request.task_type)
            if agent is None:
                return TaskGetResponse(state=PERMANENT_FAILURE)
            return agent.get(context=context, job_id=request.job_id)
        except Exception as e:
            logger.error(f"failed to get task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")

    def DeleteTask(self, request: TaskDeleteRequest, context: grpc.ServicerContext) -> TaskDeleteResponse:
        try:
            agent = AgentRegistry.get_agent(context, request.task_type)
            if agent is None:
                return TaskDeleteResponse()
            return agent.delete(context=context, job_id=request.job_id)
        except Exception as e:
            logger.error(f"failed to delete task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
