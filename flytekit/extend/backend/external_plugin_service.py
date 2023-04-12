import grpc
from flyteidl.service.external_plugin_service_pb2 import (
    TaskCreateRequest,
    TaskCreateResponse,
    TaskDeleteRequest,
    TaskDeleteResponse,
    TaskGetRequest,
    TaskGetResponse,
)
from flyteidl.service.external_plugin_service_pb2_grpc import ExternalPluginServiceServicer

from flytekit.extend.backend import model
from flytekit.extend.backend.base_plugin import BackendPluginRegistry


class BackendPluginServer(ExternalPluginServiceServicer):
    def CreateTask(self, request: TaskCreateRequest, context: grpc.ServicerContext) -> TaskCreateResponse:
        try:
            req = model.TaskCreateRequest.from_flyte_idl(request)
            plugin = BackendPluginRegistry.get_plugin(req.template.type)
            return plugin.create(
                context=context, inputs=req.inputs, output_prefix=req.output_prefix, task_template=req.template
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")

    def GetTask(self, request: TaskGetRequest, context: grpc.ServicerContext) -> TaskGetResponse:
        try:
            plugin = BackendPluginRegistry.get_plugin(request.task_type)
            return plugin.get(context=context, job_id=request.job_id)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")

    def DeleteTask(self, request: TaskDeleteRequest, context: grpc.ServicerContext) -> TaskDeleteResponse:
        try:
            plugin = BackendPluginRegistry.get_plugin(request.task_type)
            return plugin.delete(context=context, job_id=request.job_id)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
