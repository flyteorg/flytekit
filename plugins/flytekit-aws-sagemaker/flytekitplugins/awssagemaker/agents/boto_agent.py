import typing

import grpc
from flyteidl.admin.agent_pb2 import CreateTaskResponse
from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.models.literals import LiteralMap
from .boto3_mixin import Boto3AgentMixin
from .external_api_task import ExternalApiTask


class GenericSyncBotoAgent(Boto3AgentMixin, ExternalApiTask):
    """
    This provides a general purpose Boto3 agent that can be used to call any boto3 method, synchronously.
    The method has to be provided as part of the task template custom field.

    TODO this needs a common base.
    """

    def __init__(self, task_type: str):
        super().__init__(task_type=task_type, asynchronous=False)

    def do(self, context: grpc.ServicerContext, output_prefix: str, task_template: TaskTemplate,
               inputs: typing.Optional[LiteralMap] = None) -> CreateTaskResponse:
        inputs = inputs or LiteralMap(literals={})
        str_inputs = literal_map_string_repr(inputs)
        res = self._call("us-east-2", "create_model", task_template.custom, str_inputs)
        return CreateTaskResponse()
