import asyncio

import grpc
from flyteidl.admin.agent_pb2 import (
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTaskRequest,
    DeleteTaskResponse,
    GetTaskRequest,
    GetTaskResponse,
)
from flyteidl.service.agent_pb2_grpc import AsyncAgentServiceServicer
from prometheus_client import Counter, Summary

from flytekit import logger
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

metric_prefix = "flyte_agent_"
create_operation = "create"
get_operation = "get"
delete_operation = "delete"

# Follow the naming convention. https://prometheus.io/docs/practices/naming/
request_success_count = Counter(
    f"{metric_prefix}requests_success_total", "Total number of successful requests", ["task_type", "operation"]
)
request_failure_count = Counter(
    f"{metric_prefix}requests_failure_total", "Total number of failed requests", ["task_type", "operation"]
)

request_latency = Summary(
    f"{metric_prefix}request_latency_seconds", "Time spent processing agent request", ["task_type", "operation"]
)
input_literal_size = Summary(f"{metric_prefix}input_literal_bytes", "Size of input literal", ["task_type"])


class AsyncAgentService(AsyncAgentServiceServicer):
    async def CreateTask(self, request: CreateTaskRequest, context: grpc.ServicerContext) -> CreateTaskResponse:
        try:
            with request_latency.labels(task_type=request.template.type, operation=create_operation).time():
                tmp = TaskTemplate.from_flyte_idl(request.template)
                inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None

                input_literal_size.labels(task_type=tmp.type).observe(request.inputs.ByteSize())

                agent = AgentRegistry.get_agent(tmp.type)
                logger.info(f"{tmp.type} agent start creating the job")
                if agent.asynchronous:
                    try:
                        res = await agent.async_create(
                            context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp
                        )
                        request_success_count.labels(task_type=tmp.type, operation=create_operation).inc()
                        return res
                    except Exception as e:
                        logger.error(f"failed to run async create with error {e}")
                        raise e
                try:
                    res = await asyncio.to_thread(
                        agent.create,
                        context=context,
                        inputs=inputs,
                        output_prefix=request.output_prefix,
                        task_template=tmp,
                    )
                    request_success_count.labels(task_type=tmp.type, operation=create_operation).inc()
                    return res
                except Exception as e:
                    logger.error(f"failed to run sync create with error {e}")
                    raise
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")
            request_failure_count.labels(task_type=tmp.type, operation=create_operation).inc()

    async def GetTask(self, request: GetTaskRequest, context: grpc.ServicerContext) -> GetTaskResponse:
        try:
            with request_latency.labels(task_type=request.task_type, operation="get").time():
                agent = AgentRegistry.get_agent(request.task_type)
                logger.info(f"{agent.task_type} agent start checking the status of the job")
                if agent.asynchronous:
                    try:
                        res = await agent.async_get(context=context, resource_meta=request.resource_meta)
                        request_success_count.labels(task_type=request.task_type, operation=get_operation).inc()
                        return res
                    except Exception as e:
                        logger.error(f"failed to run async get with error {e}")
                        raise
                try:
                    res = await asyncio.to_thread(agent.get, context=context, resource_meta=request.resource_meta)
                    request_success_count.labels(task_type=request.task_type, operation=get_operation).inc()
                    return res
                except Exception as e:
                    logger.error(f"failed to run sync get with error {e}")
                    raise
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")
            request_failure_count.labels(task_type=request.task_type, operation=get_operation).inc()

    async def DeleteTask(self, request: DeleteTaskRequest, context: grpc.ServicerContext) -> DeleteTaskResponse:
        try:
            with request_latency.labels(task_type=request.task_type, operation="delete").time():
                agent = AgentRegistry.get_agent(request.task_type)
                logger.info(f"{agent.task_type} agent start deleting the job")
                if agent.asynchronous:
                    try:
                        res = await agent.async_delete(context=context, resource_meta=request.resource_meta)
                        request_success_count.labels(task_type=request.task_type, operation=delete_operation).inc()
                        return res
                    except Exception as e:
                        logger.error(f"failed to run async delete with error {e}")
                        raise
                try:
                    res = asyncio.to_thread(agent.delete, context=context, resource_meta=request.resource_meta)
                    request_success_count.labels(task_type=request.task_type, operation=delete_operation).inc()
                    return res
                except Exception as e:
                    logger.error(f"failed to run sync delete with error {e}")
                    raise
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
            request_failure_count.labels(task_type=request.task_type, operation=delete_operation).inc()
