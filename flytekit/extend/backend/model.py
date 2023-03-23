from flyteidl.service import plugin_system_pb2

from flytekit.models import common, task
from flytekit.models.literals import LiteralMap


class TaskCreateRequest(common.FlyteIdlEntity):
    def __init__(self, inputs: LiteralMap, template: task.TaskTemplate, output_prefix: str):
        self._inputs = inputs
        self._template = template
        self._output_prefix = output_prefix

    @property
    def inputs(self):
        return self._inputs

    @property
    def template(self):
        return self._template

    @property
    def output_prefix(self):
        return self._output_prefix

    def to_flyte_idl(self):
        return plugin_system_pb2.TaskCreateRequest(
            inputs=self.inputs.to_flyte_idl(), template=self.template.to_flyte_idl(), output_prefix=self.output_prefix
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        return cls(
            inputs=LiteralMap.from_flyte_idl(proto.inputs) if proto.inputs is not None else None,
            template=task.TaskTemplate.from_flyte_idl(proto.template),
            output_prefix=proto.output_prefix,
        )
