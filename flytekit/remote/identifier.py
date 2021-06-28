from flytekit.common.exceptions import user as _user_exceptions
from flytekit.models.core import identifier as _core_identifier


class Identifier(_core_identifier.Identifier):

    _STRING_TO_TYPE_MAP = {
        "lp": _core_identifier.ResourceType.LAUNCH_PLAN,
        "wf": _core_identifier.ResourceType.WORKFLOW,
        "tsk": _core_identifier.ResourceType.TASK,
    }
    _TYPE_TO_STRING_MAP = {v: k for k, v in _STRING_TO_TYPE_MAP.items()}

    @classmethod
    def promote_from_model(cls, base_model: _core_identifier.Identifier) -> "Identifier":
        return cls(base_model.resource_type, base_model.project, base_model.domain, base_model.name, base_model.version)

    @classmethod
    def from_urn(cls, urn: str) -> "Identifier":
        """
        Parses a string urn in the correct format into an identifier
        """
        segments = urn.split(":")
        if len(segments) != 5:
            raise _user_exceptions.FlyteValueException(
                urn,
                "The provided string was not in a parseable format. The string for an identifier must be in the "
                "format entity_type:project:domain:name:version.",
            )

        resource_type, project, domain, name, version = segments

        if resource_type not in cls._STRING_TO_TYPE_MAP:
            raise _user_exceptions.FlyteValueException(
                resource_type,
                "The provided string could not be parsed. The first element of an identifier must be one of: "
                f"{list(cls._STRING_TO_TYPE_MAP.keys())}. ",
            )

        return cls(cls._STRING_TO_TYPE_MAP[resource_type], project, domain, name, version)

    def __str__(self):
        return (
            f"{type(self)._TYPE_TO_STRING_MAP.get(self.resource_type, '<unknown>')}:"
            f"{self.project}:"
            f"{self.domain}:"
            f"{self.name}:"
            f"{self.version}"
        )


class WorkflowExecutionIdentifier(_core_identifier.WorkflowExecutionIdentifier):
    @classmethod
    def promote_from_model(
        cls, base_model: _core_identifier.WorkflowExecutionIdentifier
    ) -> "WorkflowExecutionIdentifier":
        return cls(base_model.project, base_model.domain, base_model.name)

    @classmethod
    def from_urn(cls, string: str) -> "WorkflowExecutionIdentifier":
        """
        Parses a string in the correct format into an identifier
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

        return cls(project, domain, name)

    def __str__(self):
        return f"ex:{self.project}:{self.domain}:{self.name}"


class TaskExecutionIdentifier(_core_identifier.TaskExecutionIdentifier):
    @classmethod
    def promote_from_model(cls, base_model: _core_identifier.TaskExecutionIdentifier) -> "TaskExecutionIdentifier":
        return cls(
            task_id=base_model.task_id,
            node_execution_id=base_model.node_execution_id,
            retry_attempt=base_model.retry_attempt,
        )

    @classmethod
    def from_urn(cls, string: str) -> "TaskExecutionIdentifier":
        """
        Parses a string in the correct format into an identifier
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

        return cls(
            task_id=Identifier(_core_identifier.ResourceType.TASK, tp, td, tn, tv),
            node_execution_id=_core_identifier.NodeExecutionIdentifier(
                node_id=node_id,
                execution_id=_core_identifier.WorkflowExecutionIdentifier(ep, ed, en),
            ),
            retry_attempt=int(retry),
        )

    def __str__(self):
        return (
            "te:"
            f"{self.node_execution_id.execution_id.project}:"
            f"{self.node_execution_id.execution_id.domain}:"
            f"{self.node_execution_id.execution_id.name}:"
            f"{self.node_execution_id.node_id}:"
            f"{self.task_id.project}:"
            f"{self.task_id.domain}:"
            f"{self.task_id.name}:"
            f"{self.task_id.version}:"
            f"{self.retry_attempt}"
        )
