from flytekit.exceptions import user as _user_exceptions
from flytekit.models.core import identifier as _core_identifier


class WorkflowExecutionIdentifier(_core_identifier.WorkflowExecutionIdentifier):
    @classmethod
    def promote_from_model(cls, base_model):
        """
        :param flytekit.models.core.identifier.WorkflowExecutionIdentifier base_model:
        :rtype: WorkflowExecutionIdentifier
        """
        return cls(
            base_model.project,
            base_model.domain,
            base_model.name,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        base_model = super(WorkflowExecutionIdentifier, cls).from_flyte_idl(pb2_object)
        return cls.promote_from_model(base_model)

    @classmethod
    def from_python_std(cls, string):
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

        return cls(
            project,
            domain,
            name,
        )

    def __str__(self):
        return "ex:{}:{}:{}".format(self.project, self.domain, self.name)
