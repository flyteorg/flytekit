from flyteidl.service import plugin_system_pb2
from flyteidl.service.plugin_system_pb2_grpc import BackendPluginServiceServicer

from flytekit.extend.backend.base_plugin import BackendPluginRegistry
from flytekit.extend.backend.model import TaskCreateRequest


class BackendPluginServer(BackendPluginServiceServicer):
    def CreateTask(self, request: plugin_system_pb2.TaskCreateRequest, context) -> plugin_system_pb2.TaskCreateResponse:
        req = TaskCreateRequest.from_flyte_idl(request)
        plugin = BackendPluginRegistry.get_plugin(req.template.type)
        return plugin.create(req.inputs, req.output_prefix, req.template)

    def GetTask(self, request: plugin_system_pb2.TaskGetRequest, context) -> plugin_system_pb2.TaskGetResponse:
        plugin = BackendPluginRegistry.get_plugin(request.task_type)
        return plugin.get(job_id=request.job_id, prev_state=request.prev_state)

    def DeleteTask(self, request: plugin_system_pb2.TaskDeleteRequest, context) -> plugin_system_pb2.TaskDeleteResponse:
        plugin = BackendPluginRegistry.get_plugin(request.task_type)
        return plugin.delete(request.job_id)
