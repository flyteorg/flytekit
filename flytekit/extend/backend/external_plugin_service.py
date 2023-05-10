import grpc
from flyteidl.service.external_plugin_service_pb2 import (
    PERMANENT_FAILURE,
    TaskCreateRequest,
    TaskCreateResponse,
    TaskDeleteRequest,
    TaskDeleteResponse,
    TaskGetRequest,
    TaskGetResponse,
)
from flyteidl.service.external_plugin_service_pb2_grpc import ExternalPluginServiceServicer

from flytekit import logger
from flytekit.extend.backend.base_plugin import BackendPluginRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class BackendPluginServer(ExternalPluginServiceServicer):
    def CreateTask(self, request: TaskCreateRequest, context: grpc.ServicerContext) -> TaskCreateResponse:
        try:
            tmp = TaskTemplate.from_flyte_idl(request.template)
            inputs = LiteralMap.from_flyte_idl(request.inputs) if request.inputs else None
            plugin = BackendPluginRegistry.get_plugin(context, tmp.type)
            if plugin is None:
                return TaskCreateResponse()
            return plugin.create(context=context, inputs=inputs, output_prefix=request.output_prefix, task_template=tmp)
        except Exception as e:
            logger.error(f"failed to create task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")

    def GetTask(self, request: TaskGetRequest, context: grpc.ServicerContext) -> TaskGetResponse:
        try:
            plugin = BackendPluginRegistry.get_plugin(context, request.task_type)
            if plugin is None:
                return TaskGetResponse(state=PERMANENT_FAILURE)
            return plugin.get(context=context, job_id=request.job_id)
        except Exception as e:
            logger.error(f"failed to get task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")

    def DeleteTask(self, request: TaskDeleteRequest, context: grpc.ServicerContext) -> TaskDeleteResponse:
        try:
            plugin = BackendPluginRegistry.get_plugin(context, request.task_type)
            if plugin is None:
                return TaskDeleteResponse()
            return plugin.delete(context=context, job_id=request.job_id)
        except Exception as e:
            logger.error(f"failed to delete task with error {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
