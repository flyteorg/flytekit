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
from prometheus_async.aio import time
from prometheus_client import Counter, Summary

from flytekit import logger
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

request_count = Counter("request_count", "Total number of requests")
request_success_count = Counter("request_success_count", "Total number of successful requests")
request_failure_count = Counter("request_failure_count", "Total number of failed requests")
create_req_process_time = Summary("create_request_processing_seconds", "Time spent processing agent create request")
get_req_process_time = Summary("get_request_processing_seconds", "Time spent processing agent get request")
delete_req_process_time = Summary("delete_request_processing_seconds", "Time spent processing agent delete request")


class AsyncAgentService(AsyncAgentServiceServicer):
    @time(create_req_process_time)
    async def CreateTask(self, request: CreateTaskRequest, context: grpc.ServicerContext) -> CreateTaskResponse:
        try:
            request_count.inc()
            tmp = TaskTemplate.from_flyte_idl(request.template)
            inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
            agent = AgentRegistry.get_agent(context, tmp.type)
            logger.info(f"{tmp.type} agent start creating the job")
            if agent is None:
                return CreateTaskResponse()
            if agent.asynchronous:
                try:
                    res = await agent.async_create(
                        context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp
                    )
                    request_success_count.inc()
                    return res
                except Exception as e:
                    logger.error(f"failed to run async create with error {e}")
                    raise e
            try:
                res = await asyncio.to_thread(
                    agent.create, context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp
                )
                request_success_count.inc()
                return res
            except Exception as e:
                logger.error(f"failed to run sync create with error {e}")
                raise
        except Exception as e:
            logger.error(f"failed to create task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")
            request_failure_count.inc()

    @time(get_req_process_time)
    async def GetTask(self, request: GetTaskRequest, context: grpc.ServicerContext) -> GetTaskResponse:
        try:
            request_count.inc()
            agent = AgentRegistry.get_agent(context, request.task_type)
            logger.info(f"{agent.task_type} agent start checking the status of the job")
            if agent is None:
                return GetTaskResponse(resource=Resource(state=PERMANENT_FAILURE))
            if agent.asynchronous:
                try:
                    res = await agent.async_get(context=context, resource_meta=request.resource_meta)
                    request_success_count.inc()
                    return res
                except Exception as e:
                    logger.error(f"failed to run async get with error {e}")
                    raise
            try:
                res = await asyncio.to_thread(agent.get, context=context, resource_meta=request.resource_meta)
                request_success_count.inc()
                return res
            except Exception as e:
                logger.error(f"failed to run sync get with error {e}")
                raise
        except Exception as e:
            logger.error(f"failed to get task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")
            request_failure_count.inc()

    @time(delete_req_process_time)
    async def DeleteTask(self, request: DeleteTaskRequest, context: grpc.ServicerContext) -> DeleteTaskResponse:
        try:
            request_count.inc()
            agent = AgentRegistry.get_agent(context, request.task_type)
            logger.info(f"{agent.task_type} agent start deleting the job")
            if agent is None:
                return DeleteTaskResponse()
            if agent.asynchronous:
                try:
                    res = await agent.async_delete(context=context, resource_meta=request.resource_meta)
                    request_success_count.inc()
                    return res
                except Exception as e:
                    logger.error(f"failed to run async delete with error {e}")
                    raise
            try:
                res = asyncio.to_thread(agent.delete, context=context, resource_meta=request.resource_meta)
                request_success_count.inc()
                return res
            except Exception as e:
                logger.error(f"failed to run sync delete with error {e}")
                raise
        except Exception as e:
            logger.error(f"failed to delete task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
            request_failure_count.inc()
