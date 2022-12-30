from flyteidl.core import identifier_pb2
from google.protobuf.message import Message

from flytekit.exceptions import user as _user_exceptions


class Identifier(Message):

    _STRING_TO_TYPE_MAP = {
        "lp": identifier_pb2.LAUNCH_PLAN,
        "wf": identifier_pb2.WORKFLOW,
        "tsk": identifier_pb2.TASK,
    }
    _TYPE_TO_STRING_MAP = {v: k for k, v in _STRING_TO_TYPE_MAP.items()}

    @classmethod
    def from_python_std(cls, string) -> identifier_pb2.Identifier:
        """
        Parses a string in the correct format into an identifier
        :param Text string:
        :rtype: Identifier
        """
        segments = string.split(":")
        if len(segments) != 5:
            raise _user_exceptions.FlyteValueException(
                "The provided string was not in a parseable format. The string for an identifier must be in the format"
                " entity_type:project:domain:name:version.  Received: {}".format(string)
            )

        resource_type, project, domain, name, version = segments

        if resource_type not in cls._STRING_TO_TYPE_MAP:
            raise _user_exceptions.FlyteValueException(
                resource_type,
                "The provided string could not be parsed. The first element of an identifier must be one of: {}. "
                "Received: {}".format(list(cls._STRING_TO_TYPE_MAP.keys()), resource_type),
            )
        resource_type = cls._STRING_TO_TYPE_MAP[resource_type]

        return identifier_pb2.Identifier(resource_type, project, domain, name, version)

    # TODO: can I rely on the protobuf-generated __str__?
    # def __str__(self):
    #     return "{}:{}:{}:{}:{}".format(
    #         type(self)._TYPE_TO_STRING_MAP.get(self.resource_type, "<unknown>"),
    #         self.project,
    #         self.domain,
    #         self.name,
    #         self.version,
    #     )


class TaskExecutionIdentifier(Message):
    @classmethod
    def from_python_std(cls, string) -> identifier_pb2.TaskExecutionIdentifier:
        """
        Parses a string in the correct format into an identifier
        :param Text string:
        :rtype: TaskExecutionIdentifier
        """
        segments = string.split(":")
        if len(segments) != 10:
            raise _user_exceptions.FlyteValueException(
                string,
                "The provided string was not in a parseable format. The string for an identifier must be in the format"
                " te:exec_project:exec_domain:exec_name:node_id:task_project:task_domain:task_name:task_version:retry.",
            )

        resource_type, ep, ed, en, node_id, tp, td, tn, tv, retry = segments

        if resource_type != "te":
            raise _user_exceptions.FlyteValueException(
                resource_type,
                "The provided string could not be parsed. The first element of an execution identifier must be 'ex'.",
            )

        return identifier_pb2.TaskExecutionIdentifier(
            task_id=identifier_pb2.Identifier(identifier_pb2.TASK, tp, td, tn, tv),
            node_execution_id=identifier_pb2.NodeExecutionIdentifier(
                node_id=node_id,
                execution_id=identifier_pb2.WorkflowExecutionIdentifier(ep, ed, en),
            ),
            retry_attempt=int(retry),
        )

    # TODO: auto-generated protobuf __str__ enough?
    # def __str__(self):
    #     return "te:{ep}:{ed}:{en}:{node_id}:{tp}:{td}:{tn}:{tv}:{retry}".format(
    #         ep=self.node_execution_id.execution_id.project,
    #         ed=self.node_execution_id.execution_id.domain,
    #         en=self.node_execution_id.execution_id.name,
    #         node_id=self.node_execution_id.node_id,
    #         tp=self.task_id.project,
    #         td=self.task_id.domain,
    #         tn=self.task_id.name,
    #         tv=self.task_id.version,
    #         retry=self.retry_attempt,
    #     )


class WorkflowExecutionIdentifier(Message):
    @classmethod
    def from_python_std(cls, string) -> identifier_pb2.WorkflowExecutionIdentifier:
        """
        Parses a string in the correct format into an identifier
        :param Text string:
        :rtype: WorkflowExecutionIdentifier
        """
        segments = string.split(":")
        if len(segments) != 4:
            raise _user_exceptions.FlyteValueException(
                string,
                "The provided string was not in a parseable format. The string for an identifier must be in the format"
                " ex:project:domain:name.",
            )

        resource_type, project, domain, name = segments

        if resource_type != "ex":
            raise _user_exceptions.FlyteValueException(
                resource_type,
                "The provided string could not be parsed. The first element of an execution identifier must be 'ex'.",
            )

        return identifier_pb2.WorkflowExecutionIdentifier(
            project,
            domain,
            name,
        )

    # TODO: auto-generated protobuf __str__ enough?
    # def __str__(self):
    #     return "ex:{}:{}:{}".format(self.project, self.domain, self.name)
