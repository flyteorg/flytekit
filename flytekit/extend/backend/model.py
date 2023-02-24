from flyteidl.service import plugin_system_pb2

from flytekit.models import common, task
from flytekit.models.literals import LiteralMap


class TaskCreateRequest(common.FlyteIdlEntity):
    def __init__(self, inputs: LiteralMap, template: task.TaskTemplate):
        self._inputs = inputs
        self._template = template

    @property
    def inputs(self):
        return self._inputs

    @property
    def template(self):
        return self._template

    def to_flyte_idl(self):
        return plugin_system_pb2.TaskCreateRequest(
            inputs=self.inputs.to_flyte_idl(), template=self.template.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        return cls(
            inputs=LiteralMap.from_flyte_idl(proto.inputs) if proto.inputs is not None else None,
            template=task.TaskTemplate.from_flyte_idl(proto.template),
        )
