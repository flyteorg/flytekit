import typing
from http import HTTPStatus

import grpc
from flyteidl.admin.agent_pb2 import (
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTaskRequest,
    DeleteTaskResponse,
    ExecuteTaskSyncRequest,
    ExecuteTaskSyncResponse,
    ExecuteTaskSyncResponseHeader,
    GetAgentRequest,
    GetAgentResponse,
    GetTaskRequest,
    GetTaskResponse,
    ListAgentsRequest,
    ListAgentsResponse,
    Resource,
)
from flyteidl.service.agent_pb2_grpc import (
    AgentMetadataServiceServicer,
    AsyncAgentServiceServicer,
    SyncAgentServiceServicer,
)
from prometheus_client import Counter, Summary

from flytekit import FlyteContext, logger
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.system import FlyteAgentNotFound
from flytekit.extend.backend.base_agent import AgentRegistry, SyncAgentBase, mirror_async_methods
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate

metric_prefix = "flyte_agent_"
create_operation = "create"
get_operation = "get"
delete_operation = "delete"
do_operation = "do"

# Follow the naming convention. https://prometheus.io/docs/practices/naming/
request_success_count = Counter(
    f"{metric_prefix}requests_success_total",
    "Total number of successful requests",
    ["task_type", "operation"],
)
request_failure_count = Counter(
    f"{metric_prefix}requests_failure_total",
    "Total number of failed requests",
    ["task_type", "operation", "error_code"],
)
request_latency = Summary(
    f"{metric_prefix}request_latency_seconds",
    "Time spent processing agent request",
    ["task_type", "operation"],
)
input_literal_size = Summary(f"{metric_prefix}input_literal_bytes", "Size of input literal", ["task_type"])


def _handle_exception(e: Exception, context: grpc.ServicerContext, task_type: str, operation: str):
    if isinstance(e, FlyteAgentNotFound):
        error_message = f"Cannot find agent for task type: {task_type}."
        logger.error(error_message)
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(error_message)
        request_failure_count.labels(task_type=task_type, operation=operation, error_code=HTTPStatus.NOT_FOUND).inc()
    else:
        error_message = f"failed to {operation} {task_type} task with error: {e}."
        logger.error(error_message)
        context.set_code(grpc.StatusCode.INTERNAL)
        context.set_details(error_message)
        request_failure_count.labels(
            task_type=task_type, operation=operation, error_code=HTTPStatus.INTERNAL_SERVER_ERROR
        ).inc()


def record_agent_metrics(func: typing.Callable):
    async def wrapper(
        self,
        request: typing.Union[CreateTaskRequest, GetTaskRequest, DeleteTaskRequest],
        context: grpc.ServicerContext,
        *args,
        **kwargs,
    ):
        if isinstance(request, CreateTaskRequest):
            task_type = request.template.type
            operation = create_operation
            if request.inputs:
                input_literal_size.labels(task_type=task_type).observe(request.inputs.ByteSize())
        elif isinstance(request, GetTaskRequest):
            task_type = request.task_type or request.task_category.name
            operation = get_operation
        elif isinstance(request, DeleteTaskRequest):
            task_type = request.task_type or request.task_category.name
            operation = delete_operation
        else:
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details("Method not implemented!")
            return

        try:
            with request_latency.labels(task_type=task_type, operation=operation).time():
                res = await func(self, request, context, *args, **kwargs)
            request_success_count.labels(task_type=task_type, operation=operation).inc()
            return res
        except Exception as e:
            _handle_exception(e, context, task_type, operation)

    return wrapper


class AsyncAgentService(AsyncAgentServiceServicer):
    @record_agent_metrics
    async def CreateTask(self, request: CreateTaskRequest, context: grpc.ServicerContext) -> CreateTaskResponse:
        template = TaskTemplate.from_flyte_idl(request.template)
        inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
        agent = AgentRegistry.get_agent(template.type, template.task_type_version)
        task_execution_metadata = TaskExecutionMetadata.from_flyte_idl(request.task_execution_metadata)

        logger.info(f"{agent.name} start creating the job")
        resource_mata = await mirror_async_methods(
            agent.create,
            task_template=template,
            inputs=inputs,
            output_prefix=request.output_prefix,
            task_execution_metadata=task_execution_metadata,
        )
        return CreateTaskResponse(resource_meta=resource_mata.encode())

    @record_agent_metrics
    async def GetTask(self, request: GetTaskRequest, context: grpc.ServicerContext) -> GetTaskResponse:
        if request.task_category and request.task_category.name:
            agent = AgentRegistry.get_agent(request.task_category.name, request.task_category.version)
        else:
            agent = AgentRegistry.get_agent(request.task_type)
        logger.info(f"{agent.name} start checking the status of the job")
        res = await mirror_async_methods(agent.get, resource_meta=agent.metadata_type.decode(request.resource_meta))

        if res.outputs is None:
            outputs = None
        elif isinstance(res.outputs, LiteralMap):
            outputs = res.outputs.to_flyte_idl()
        else:
            ctx = FlyteContext.current_context()
            outputs = TypeEngine.dict_to_literal_map_pb(ctx, res.outputs)
        return GetTaskResponse(
            resource=Resource(phase=res.phase, log_links=res.log_links, message=res.message, outputs=outputs)
        )

    @record_agent_metrics
    async def DeleteTask(self, request: DeleteTaskRequest, context: grpc.ServicerContext) -> DeleteTaskResponse:
        if request.task_category and request.task_category.name:
            agent = AgentRegistry.get_agent(request.task_category.name, request.task_category.version)
        else:
            agent = AgentRegistry.get_agent(request.task_type)
        logger.info(f"{agent.name} start deleting the job")
        await mirror_async_methods(agent.delete, resource_meta=agent.metadata_type.decode(request.resource_meta))
        return DeleteTaskResponse()


class SyncAgentService(SyncAgentServiceServicer):
    async def ExecuteTaskSync(
        self, request_iterator: typing.AsyncIterator[ExecuteTaskSyncRequest], context: grpc.ServicerContext
    ) -> typing.AsyncIterator[ExecuteTaskSyncResponse]:
        request = await request_iterator.__anext__()
        template = TaskTemplate.from_flyte_idl(request.header.template)
        output_prefix = request.header.output_prefix
        task_type = template.type
        try:
            with request_latency.labels(task_type=task_type, operation=do_operation).time():
                agent = AgentRegistry.get_agent(task_type, template.task_type_version)
                if not isinstance(agent, SyncAgentBase):
                    raise ValueError(f"[{agent.name}] does not support sync execution")

                request = await request_iterator.__anext__()
                literal_map = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
                res = await mirror_async_methods(
                    agent.do, task_template=template, inputs=literal_map, output_prefix=output_prefix
                )

                if res.outputs is None:
                    outputs = None
                elif isinstance(res.outputs, LiteralMap):
                    outputs = res.outputs.to_flyte_idl()
                else:
                    ctx = FlyteContext.current_context()
                    outputs = TypeEngine.dict_to_literal_map_pb(ctx, res.outputs)

                header = ExecuteTaskSyncResponseHeader(
                    resource=Resource(phase=res.phase, log_links=res.log_links, message=res.message, outputs=outputs)
                )
                yield ExecuteTaskSyncResponse(header=header)
            request_success_count.labels(task_type=task_type, operation=do_operation).inc()
        except Exception as e:
            _handle_exception(e, context, template.type, do_operation)


class AgentMetadataService(AgentMetadataServiceServicer):
    async def GetAgent(self, request: GetAgentRequest, context: grpc.ServicerContext) -> GetAgentResponse:
        return GetAgentResponse(agent=AgentRegistry.get_agent_metadata(request.name))

    async def ListAgents(self, request: ListAgentsRequest, context: grpc.ServicerContext) -> ListAgentsResponse:
        return ListAgentsResponse(agents=AgentRegistry.list_agents())
