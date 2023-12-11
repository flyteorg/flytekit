import typing

import grpc
from flyteidl.admin.agent_pb2 import DeleteTaskResponse, GetTaskResponse, CreateTaskResponse
from flyteidl.core.tasks_pb2 import TaskTemplate
from google.protobuf.json_format import MessageToDict

from flytekit.extend.backend.base_agent import AgentBase
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap
from .boto3_mixin import Boto3AgentMixin
from .sync_agent_base import SyncAgentBase


class SagemakerEndpointAgent(Boto3AgentMixin, SyncAgentBase):

    def __init__(self, task_type: str):
        super().__init__(service="sagemaker", region="us-east-2", task_type=task_type, asynchronous=False, )

    def create(self, context: grpc.ServicerContext, output_prefix: str, task_template: TaskTemplate,
               inputs: typing.Optional[LiteralMap] = None) -> CreateTaskResponse:
        # We probably want to make 2 parts in endpoint config, one for the model and one for the endpoint
        res = self._call("create_endpoint_config", task_template.custom, task_template, inputs)
        res = self._call("create_endpoint", task_template.custom, task_template, inputs)

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        # Wait for endpoint to be created
        pass

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        # delete endpoint and endpoint config
        pass


class SagemakerModelAgent(Boto3AgentMixin, AgentBase):
    boto3_method = "create_model"
    boto3_service = "sagemaker"

    def __init__(self, task_type: str):
        super().__init__(service=self.boto3_service, region="us-east-2", task_type=task_type, asynchronous=False, )

    def create(self, context: grpc.ServicerContext, output_prefix: str, task_template: TaskTemplate,
               inputs: typing.Optional[LiteralMap] = None) -> CreateTaskResponse:
        custom_config = {}
        if task_template.custom:
            custom_config = MessageToDict(task_template.custom)
        res = self._call(self.boto3_method, custom_config, task_template, inputs, region="us-east-2")
        # Should return immediately

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        pass

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        pass


class SagemakerInvokeEndpointAgent(Boto3AgentMixin, AgentBase):

    def __init__(self, task_type: str):
        super().__init__(service="sagemaker-runtime", region="us-east-2", task_type=task_type, asynchronous=False, )

    def create(self, context: grpc.ServicerContext, output_prefix: str, task_template: TaskTemplate,
               inputs: typing.Optional[LiteralMap] = None) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})
        str_inputs = literal_map_string_repr(inputs)
        res = self._call("us-east-2", "invoke_endpoint", task_template.custom, str_inputs)

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        pass

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        pass



# create_model = SagemakerCreateModelTask(
#     inputs=kwtypes(model_name=str, image=str, model_path=str),
#     config={
#         "ModelName": "{inputs.model_name}",
#         "Image": "{container.image}",
#         "PrimaryContainer": {
#             "Image": "{image}",
#             "ModelDataUrl": "{model_path}",
#             "region": "us-east-2",
#         },
#     },
# )

