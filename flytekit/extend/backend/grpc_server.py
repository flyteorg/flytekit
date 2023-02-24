from flyteidl.service import plugin_system_pb2
from flyteidl.service.plugin_system_pb2_grpc import BackendPluginServiceServicer, add_BackendPluginServiceServicer_to_server

from flytekit.extend.backend.base_plugin import BackendPluginRegistry, CreateRequest, SUCCEEDED, PollRequest
from flytekit.extend.backend.model import TaskCreateRequest


class BackendPluginServer(BackendPluginServiceServicer):
    def CreateTask(self, request: plugin_system_pb2.TaskCreateRequest, context):
        print("creating")
        req = TaskCreateRequest.from_flyte_idl(request)
        plugin = BackendPluginRegistry.get_plugin(req.template.type)
        res = plugin.create(CreateRequest(req.inputs, req.template))
        return plugin_system_pb2.TaskCreateResponse(job_id=res.job_id, message=res.message)

    def GetTask(self, request: plugin_system_pb2.TaskGetRequest, context):
        print("getting")
        plugin = BackendPluginRegistry.get_plugin(request.task_type)
        res = plugin.poll(PollRequest(job_id=request.job_id, output_prefix=request.output_prefix, prev_state=request.prev_state))
        return plugin_system_pb2.TaskGetResponse(state=res.state, message=res.message)

    def DeleteTask(self, request: plugin_system_pb2.TaskDeleteRequest, context):
        plugin = BackendPluginRegistry.get_plugin(request.task_type)
        res = plugin.terminate(request.job_id)
        print("deleting", res)
