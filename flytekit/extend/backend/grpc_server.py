import grpc
from flyteidl.service import plugin_system_pb2
from flyteidl.service.plugin_system_pb2_grpc import BackendPluginServiceServicer

from flytekit.extend.backend.base_plugin import BackendPluginRegistry
from flytekit.extend.backend.model import TaskCreateRequest


class BackendPluginServer(BackendPluginServiceServicer):
    def CreateTask(self, request: plugin_system_pb2.TaskCreateRequest, context) -> plugin_system_pb2.TaskCreateResponse:
        try:
            req = TaskCreateRequest.from_flyte_idl(request)
            plugin = BackendPluginRegistry.get_plugin(req.template.type)
            return plugin.create(
                context=context, inputs=req.inputs, output_prefix=req.output_prefix, task_template=req.template
            )
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to create task with error {e}")

    def GetTask(self, request: plugin_system_pb2.TaskGetRequest, context) -> plugin_system_pb2.TaskGetResponse:
        try:
            plugin = BackendPluginRegistry.get_plugin(request.task_type)
            return plugin.get(context=context, job_id=request.job_id)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to get task with error {e}")

    def DeleteTask(self, request: plugin_system_pb2.TaskDeleteRequest, context) -> plugin_system_pb2.TaskDeleteResponse:
        try:
            plugin = BackendPluginRegistry.get_plugin(request.task_type)
            return plugin.delete(context=context, job_id=request.job_id)
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"failed to delete task with error {e}")
