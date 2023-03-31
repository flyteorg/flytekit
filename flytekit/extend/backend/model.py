from typing import Optional

from flyteidl.service import external_plugin_service_pb2

from flytekit.models import common, task
from flytekit.models.literals import LiteralMap


class TaskCreateRequest(common.FlyteIdlEntity):
    def __init__(self, output_prefix: str, template: task.TaskTemplate, inputs: Optional[LiteralMap] = None):
        self._output_prefix = output_prefix
        self._template = template
        self._inputs = inputs

    @property
    def output_prefix(self) -> str:
        return self._output_prefix

    @property
    def template(self) -> task.TaskTemplate:
        return self._template

    @property
    def inputs(self) -> Optional[LiteralMap]:
        return self._inputs

    def to_flyte_idl(self) -> external_plugin_service_pb2.TaskCreateRequest:
        return external_plugin_service_pb2.TaskCreateRequest(
            output_prefix=self.output_prefix,
            template=self.template.to_flyte_idl(),
            inputs=self.inputs.to_flyte_idl(),
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        return cls(
            output_prefix=proto.output_prefix,
            template=task.TaskTemplate.from_flyte_idl(proto.template),
            inputs=LiteralMap.from_flyte_idl(proto.inputs) if proto.inputs is not None else None,
        )
