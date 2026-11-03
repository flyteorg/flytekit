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
    GetTaskLogsRequest,
    GetTaskLogsResponse,
    GetTaskMetricsRequest,
    GetTaskMetricsResponse,
    GetTaskRequest,
    GetTaskResponse,
    ListAgentsRequest,
    ListAgentsResponse,
)
from flyteidl.service.agent_pb2_grpc import (
    AgentMetadataServiceServicer,
    AsyncAgentServiceServicer,
    SyncAgentServiceServicer,
)
from prometheus_client import Counter, Summary

from flytekit import logger
from flytekit.bin.entrypoint import get_traceback_str
from flytekit.exceptions.system import FlyteConnectorNotFound
from flytekit.extend.backend.base_connector import ConnectorRegistry, SyncConnectorBase, mirror_async_methods
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate

metric_prefix = "flyte_connector_"
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
    "Time spent processing connector request",
    ["task_type", "operation"],
)
input_literal_size = Summary(f"{metric_prefix}input_literal_bytes", "Size of input literal", ["task_type"])


def _handle_exception(e: Exception, context: grpc.ServicerContext, task_type: str, operation: str):
    if isinstance(e, FlyteConnectorNotFound):
        error_message = f"Cannot find connector for task type: {task_type}."
        logger.error(error_message)
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details(error_message)
        request_failure_count.labels(task_type=task_type, operation=operation, error_code=HTTPStatus.NOT_FOUND).inc()
    else:
        error_message = f"failed to {operation} {task_type} task with error:\n {get_traceback_str(e)}."
        logger.error(error_message)
        context.set_code(grpc.StatusCode.INTERNAL)
        context.set_details(error_message)
        request_failure_count.labels(
            task_type=task_type, operation=operation, error_code=HTTPStatus.INTERNAL_SERVER_ERROR
        ).inc()


def record_connector_metrics(func: typing.Callable):
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


class AsyncConnectorService(AsyncAgentServiceServicer):
    @record_connector_metrics
    async def CreateTask(self, request: CreateTaskRequest, context: grpc.ServicerContext) -> CreateTaskResponse:
        template = TaskTemplate.from_flyte_idl(request.template)
        inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
        connector = ConnectorRegistry.get_connector(template.type, template.task_type_version)
        task_execution_metadata = TaskExecutionMetadata.from_flyte_idl(request.task_execution_metadata)

        logger.info(f"{connector.name} start creating the job")
        resource_meta = await mirror_async_methods(
            connector.create,
            task_template=template,
            inputs=inputs,
            output_prefix=request.output_prefix,
            task_execution_metadata=task_execution_metadata,
        )
        return CreateTaskResponse(resource_meta=resource_meta.encode())

    @record_connector_metrics
    async def GetTask(self, request: GetTaskRequest, context: grpc.ServicerContext) -> GetTaskResponse:
        if request.task_category and request.task_category.name:
            connector = ConnectorRegistry.get_connector(request.task_category.name, request.task_category.version)
        else:
            connector = ConnectorRegistry.get_connector(request.task_type)
        logger.info(f"{connector.name} start checking the status of the job")
        res = await mirror_async_methods(
            connector.get, resource_meta=connector.metadata_type.decode(request.resource_meta)
        )
        resource = await res.to_flyte_idl()
        return GetTaskResponse(resource=resource)

    @record_connector_metrics
    async def DeleteTask(self, request: DeleteTaskRequest, context: grpc.ServicerContext) -> DeleteTaskResponse:
        if request.task_category and request.task_category.name:
            connector = ConnectorRegistry.get_connector(request.task_category.name, request.task_category.version)
        else:
            connector = ConnectorRegistry.get_connector(request.task_type)
        logger.info(f"{connector.name} start deleting the job")
        await mirror_async_methods(
            connector.delete, resource_meta=connector.metadata_type.decode(request.resource_meta)
        )
        return DeleteTaskResponse()

    async def GetTaskMetrics(
        self, request: GetTaskMetricsRequest, context: grpc.ServicerContext
    ) -> GetTaskMetricsResponse:
        if request.task_category and request.task_category.name:
            connector = ConnectorRegistry.get_connector(request.task_category.name, request.task_category.version)
        else:
            connector = ConnectorRegistry.get_connector(request.task_type)
        logger.info(f"{connector.name} start getting metrics of the job")
        return await mirror_async_methods(
            connector.get_metrics, resource_meta=connector.metadata_type.decode(request.resource_meta)
        )

    async def GetTaskLogs(self, request: GetTaskLogsRequest, context: grpc.ServicerContext) -> GetTaskLogsResponse:
        if request.task_category and request.task_category.name:
            connector = ConnectorRegistry.get_connector(request.task_category.name, request.task_category.version)
        else:
            connector = ConnectorRegistry.get_connector(request.task_type)
        logger.info(f"{connector.name} start getting logs of the job")
        return await mirror_async_methods(
            connector.get_logs, resource_meta=connector.metadata_type.decode(request.resource_meta)
        )


class SyncConnectorService(SyncAgentServiceServicer):
    async def ExecuteTaskSync(
        self, request_iterator: typing.AsyncIterator[ExecuteTaskSyncRequest], context: grpc.ServicerContext
    ) -> typing.AsyncIterator[ExecuteTaskSyncResponse]:
        request = await request_iterator.__anext__()
        template = TaskTemplate.from_flyte_idl(request.header.template)
        output_prefix = request.header.output_prefix
        task_type = template.type
        try:
            with request_latency.labels(task_type=task_type, operation=do_operation).time():
                connector = ConnectorRegistry.get_connector(task_type, template.task_type_version)
                if not isinstance(connector, SyncConnectorBase):
                    raise ValueError(f"[{connector.name}] does not support sync execution")

                request = await request_iterator.__anext__()
                literal_map = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
                res = await mirror_async_methods(
                    connector.do, task_template=template, inputs=literal_map, output_prefix=output_prefix
                )

                resource = await res.to_flyte_idl()
                header = ExecuteTaskSyncResponseHeader(resource=resource)
                yield ExecuteTaskSyncResponse(header=header)
            request_success_count.labels(task_type=task_type, operation=do_operation).inc()
        except Exception as e:
            _handle_exception(e, context, template.type, do_operation)


class ConnectorMetadataService(AgentMetadataServiceServicer):
    async def GetAgent(self, request: GetAgentRequest, context: grpc.ServicerContext) -> GetAgentResponse:
        return GetAgentResponse(agent=ConnectorRegistry.get_connector_metadata(request.name))

    async def ListAgents(self, request: ListAgentsRequest, context: grpc.ServicerContext) -> ListAgentsResponse:
        return ListAgentsResponse(agents=ConnectorRegistry.list_connectors())
