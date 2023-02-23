from flyteidl.service import plugin_system_pb2

from flytekit.models import common, interface, task


class TaskCreateRequest(common.FlyteIdlEntity):
    def __init__(self, task_type: str, inputs: interface.VariableMap, template: task.TaskTemplate):
        self._task_type = task_type
        self._inputs = inputs
        self._template = template

    @property
    def task_type(self):
        return self._task_type

    @property
    def inputs(self):
        return self._inputs

    @property
    def template(self):
        return self._template

    def to_flyte_idl(self):
        return plugin_system_pb2.TaskCreateRequest(
            task_type=self.task_type, inputs=self.inputs.to_flyte_idl(), template=self.template.to_flyte_idl()
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        return cls(
            task_type=proto.proto,
            inputs=interface.VariableMap.from_flyte_idl(proto.inputs) if proto.inputs is not None else None,
            template=task.TaskTemplate.from_flyte_idl(proto.template),
        )
