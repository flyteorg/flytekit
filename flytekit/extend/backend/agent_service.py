import asyncio

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


class AsyncAgentService(AsyncAgentServiceServicer):
    async def CreateTask(self, request: CreateTaskRequest, context: grpc.ServicerContext) -> CreateTaskResponse:
        try:
            tmp = TaskTemplate.from_flyte_idl(request.template)
            inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
            agent = AgentRegistry.get_agent(context, tmp.type)
            logger.info(f"{tmp.type} agent start creating the job")
            if agent is None:
                return CreateTaskResponse()
            if agent.asynchronous:
                try:
                    return await agent.async_create(
                        context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp
                    )
                except Exception as e:
                    logger.error(f"failed to run async create with error {e}")
                    raise e
            try:
                return await asyncio.to_thread(
                    agent.create, context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp
                )
            except Exception as e:
                logger.error(f"failed to run sync create with error {e}")
                raise
        except Exception as e:
            logger.error(f"failed to create task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")

    async def GetTask(self, request: GetTaskRequest, context: grpc.ServicerContext) -> GetTaskResponse:
        try:
            agent = AgentRegistry.get_agent(context, request.task_type)
            logger.info(f"{agent.task_type} agent start checking the status of the job")
            if agent is None:
                return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
            if agent.asynchronous:
                try:
                    return await agent.async_get(context=context, resource_meta=request.resource_meta)
                except Exception as e:
                    logger.error(f"failed to run async get with error {e}")
                    raise
            try:
                return await asyncio.to_thread(agent.get, context=context, resource_meta=request.resource_meta)
            except Exception as e:
                logger.error(f"failed to run sync get with error {e}")
                raise
        except Exception as e:
            logger.error(f"failed to get task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")

    async def DeleteTask(self, request: DeleteTaskRequest, context: grpc.ServicerContext) -> DeleteTaskResponse:
        try:
            agent = AgentRegistry.get_agent(context, request.task_type)
            logger.info(f"{agent.task_type} agent start deleting the job")
            if agent is None:
                return DeleteTaskResponse()
            if agent.asynchronous:
                try:
                    return await agent.async_delete(context=context, resource_meta=request.resource_meta)
                except Exception as e:
                    logger.error(f"failed to run async delete with error {e}")
                    raise
            try:
                return asyncio.to_thread(agent.delete, context=context, resource_meta=request.resource_meta)
            except Exception as e:
                logger.error(f"failed to run sync delete with error {e}")
                raise
        except Exception as e:
            logger.error(f"failed to delete task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
