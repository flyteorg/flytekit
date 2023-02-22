from flytekit.models import common as _common
from flyteidl.service import plugin_system_pb2


class TaskCreateRequest(_common.FlyteIdlEntity):
    def __init__(self, task_type: str,  input, template):
        self._task_type = task_type
        self._input = input
        self._template = template

    @property
    def task_type(self):
        return self._task_type

    @property
    def input(self):
        return self._input

    @property
    def template(self):
        return self._template

    def to_flyte_idl(self):
        return plugin_system_pb2.TaskCreateRequest(task_type=self.task_type, input=self.input, template=self.template)

    @classmethod
    def from_flyte_idl(cls, proto):
        return cls(task_type=proto.proto, input=proto.input, template=proto.template)