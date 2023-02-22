from flyteidl.service.plugin_system_pb2_grpc import BackendPluginServiceServicer
from flyteidl.service import plugin_system_pb2

from flytekit.extend.backend.base_plugin import BackendPluginRegistry, CreateRequest


class BackendPluginServer(BackendPluginServiceServicer):
    def CreateTask(self, request: plugin_system_pb2.TaskCreateRequest, context):
        plugin = BackendPluginRegistry.get_plugin(request.task_type)
        plugin.create(CreateRequest())
        return plugin_system_pb2.TaskCreateResponse()
