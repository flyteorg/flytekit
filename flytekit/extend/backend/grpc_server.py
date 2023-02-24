from concurrent import futures

import grpc
from flyteidl.service import plugin_system_pb2
from flyteidl.service.plugin_system_pb2_grpc import BackendPluginServiceServicer, add_BackendPluginServiceServicer_to_server

from flytekit.extend.backend.base_plugin import BackendPluginRegistry, CreateRequest, SUCCEEDED
from flytekit.extend.backend.model import TaskCreateRequest


class BackendPluginServer(BackendPluginServiceServicer):
    def CreateTask(self, request: plugin_system_pb2.TaskCreateRequest, context):
        req = TaskCreateRequest.from_flyte_idl(request)
        plugin = BackendPluginRegistry.get_plugin(req.template.type)
        res = plugin.create(CreateRequest(req.inputs, req.template))
        return plugin_system_pb2.TaskCreateResponse(res.job_id, res.message)

    def GetTask(self, request, context):
        return plugin_system_pb2.TaskGetResponse(state=SUCCEEDED)

    def DeleteTask(self, request, context):
        print("deleting")
