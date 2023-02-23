from flyteidl.service import plugin_system_pb2
from flyteidl.service.plugin_system_pb2_grpc import BackendPluginServiceServicer

from flytekit.extend.backend.base_plugin import BackendPluginRegistry, CreateRequest
from flytekit.extend.backend.model import TaskCreateRequest


class BackendPluginServer(BackendPluginServiceServicer):
    def CreateTask(self, request: plugin_system_pb2.TaskCreateRequest, context):
        req = TaskCreateRequest.from_flyte_idl(request)
        plugin = BackendPluginRegistry.get_plugin(req.task_type)
        plugin.create(CreateRequest(req.inputs, req.template))
        return plugin_system_pb2.TaskCreateResponse()
