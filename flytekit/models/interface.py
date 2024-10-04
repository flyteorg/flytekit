import typing

import flyteidl_rust as flyteidl

from flytekit.models import common as _common
from flytekit.models import literals as _literals
from flytekit.models import types as _types


class Variable(_common.FlyteIdlEntity):
    def __init__(
        self,
        type,
        description,
        artifact_partial_id: typing.Optional[flyteidl.core.ArtifactId] = None,
        artifact_tag: typing.Optional[flyteidl.core.ArtifactTag] = None,
    ):
        """
        :param flytekit.models.types.LiteralType type: This describes the type of value that must be provided to
            satisfy this variable.
        :param Text description: This is a help string that can provide context for what this variable means in relation
            to a task or workflow.
        :param artifact_partial_id: Optional Artifact object to control how the artifact is created when the task runs.
        :param artifact_tag: Optional ArtifactTag object to automatically tag things.
        """
        self._type = type
        self._description = description
        self._artifact_partial_id = artifact_partial_id
        self._artifact_tag = artifact_tag

    @property
    def type(self):
        """
        This describes the type of value that must be provided to satisfy this variable.
        :rtype: flytekit.models.types.LiteralType
        """
        return self._type

    @property
    def description(self):
        """
        This is a help string that can provide context for what this variable means in relation to a task or workflow.
        :rtype: Text
        """
        return self._description

    @property
    def artifact_partial_id(self) -> typing.Optional[flyteidl.core.ArtifactId]:
        return self._artifact_partial_id

    @property
    def artifact_tag(self) -> typing.Optional[flyteidl.core.ArtifactTag]:
        return self._artifact_tag

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.interface_pb2.Variable
        """
        return flyteidl.core.Variable(
            type=self.type.to_flyte_idl(),
            description=self.description,
            artifact_partial_id=self.artifact_partial_id,
            artifact_tag=self.artifact_tag,
        )

    @classmethod
    def from_flyte_idl(cls, variable_proto) -> flyteidl.core.Variable:
        """
        :param flyteidl.core.interface_pb2.Variable variable_proto:
        """
        return cls(
            type=_types.LiteralType.from_flyte_idl(variable_proto.type),
            description=variable_proto.description,
            artifact_partial_id=variable_proto.artifact_partial_id or None,
            artifact_tag=variable_proto.artifact_tag or None,
        )


class VariableMap(_common.FlyteIdlEntity):
    def __init__(self, variables):
        """
        A map of Variables

        :param dict[Text, Variable] variables:
        """
        self._variables = variables

    @property
    def variables(self):
        """
        :rtype: dict[Text, Variable]
        """
        return self._variables

    def to_flyte_idl(self):
        """
        :rtype: dict[Text, Variable]
        """
        return flyteidl.core.VariableMap(variables={k: v.to_flyte_idl() for k, v in self.variables.items()})

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param dict[Text, Variable] pb2_object:
        :rtype: VariableMap
        """
        return cls({k: Variable.from_flyte_idl(v) for k, v in pb2_object.variables.items()})


class TypedInterface(_common.FlyteIdlEntity):
    def __init__(self, inputs, outputs):
        """
        Please note that this model is slightly incorrect, but is more user-friendly. The underlying inputs and
        outputs are represented directly as Python dicts, rather than going through the additional VariableMap layer.

        :param dict[Text, Variable] inputs: This defines the names and types for the interface's inputs.
        :param dict[Text, Variable] outputs: This defines the names and types for the interface's outputs.
        """
        self._inputs = inputs
        self._outputs = outputs

    @property
    def inputs(self) -> typing.Dict[str, Variable]:
        return self._inputs

    @property
    def outputs(self) -> typing.Dict[str, Variable]:
        return self._outputs

    def to_flyte_idl(self) -> flyteidl.core.TypedInterface:
        return flyteidl.core.TypedInterface(
            inputs=flyteidl.core.VariableMap(variables={k: v.to_flyte_idl() for k, v in self.inputs.items()}),
            outputs=flyteidl.core.VariableMap(variables={k: v.to_flyte_idl() for k, v in self.outputs.items()}),
        )

    @classmethod
    def from_flyte_idl(cls, proto: flyteidl.core.TypedInterface) -> "TypedInterface":
        """
        :param proto:
        """
        return cls(
            inputs={k: Variable.from_flyte_idl(v) for k, v in proto.inputs.variables.items()},
            outputs={k: Variable.from_flyte_idl(v) for k, v in proto.outputs.variables.items()},
        )


class Parameter(_common.FlyteIdlEntity):
    def __init__(
        self,
        var,
        default=None,
        required=None,
        artifact_query: typing.Optional[flyteidl.core.ArtifactQuery] = None,
        artifact_id: typing.Optional[flyteidl.core.ArtifactId] = None,
    ):
        """
        Declares an input parameter.  A parameter is used as input to a launch plan and has
            the special ability to have a default value or mark itself as required.
        :param Variable var: Defines a name and a type to reference/compare through out the system.
        :param flytekit.models.literals.Literal default: [Optional] Defines a default value that has to match the
            variable type defined.
        :param bool required: [Optional] is this value required to be filled in?
        :param artifact_query: Specify this to bind to a query instead of a constant.
        :param artifact_id: When you want to bind to a known artifact pointer.
        """
        self._var = var
        self._default = default
        self._required = required
        self._artifact_query = artifact_query
        self._artifact_id = artifact_id

    @property
    def var(self):
        """
        The variable definition for this input parameter.
        :rtype: Variable
        """
        return self._var

    @property
    def default(self):
        """
        This is the default literal value that will be applied for this parameter if not user specified.
        :rtype: flytekit.models.literals.Literal
        """
        return self._default

    @property
    def required(self) -> bool:
        """
        If True, this parameter must be specified.  There cannot be a default value.
        :rtype: bool
        """
        return self._required

    @property
    def behavior(self):
        """
        :rtype: T
        """
        return self._default or self._required or self._artifact_query

    @property
    def artifact_query(self) -> typing.Optional[flyteidl.core.ArtifactQuery]:
        return self._artifact_query

    @property
    def artifact_id(self) -> typing.Optional[flyteidl.core.ArtifactId]:
        return self._artifact_id

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.interface_pb2.Parameter
        """
        behavior = None
        if self.default:
            behavior = flyteidl.parameter.Behavior.Default(self.default.to_flyte_idl())
        if self.required:
            behavior = flyteidl.parameter.Behavior.Required(self.required)
        if self.artifact_query:
            behavior = flyteidl.parameter.Behavior.ArtifactQuery(self.artifact_query)
        if self.artifact_id:
            behavior = flyteidl.parameter.Behavior.ArtifactId(self.artifact_id)
        return flyteidl.core.Parameter(
            var=self.var.to_flyte_idl(),
            behavior=behavior,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.interface_pb2.Parameter pb2_object:
        :rtype: Parameter
        """
        return cls(
            Variable.from_flyte_idl(pb2_object.var),
            _literals.Literal.from_flyte_idl(pb2_object.behavior[0])
            if isinstance(pb2_object.behavior, flyteidl.parameter.Behavior.Default)
            else None,
            pb2_object.behavior if isinstance(pb2_object.behavior, flyteidl.parameter.Behavior.Required) else None,
            artifact_query=pb2_object.behavior
            if isinstance(pb2_object.behavior, flyteidl.parameter.Behavior.ArtifactQuery)
            else None,
            artifact_id=pb2_object.behavior
            if isinstance(pb2_object.behavior, flyteidl.parameter.Behavior.ArtifactId)
            else None,
        )


class ParameterMap(_common.FlyteIdlEntity):
    def __init__(self, parameters):
        """
        A map of Parameters
        :param dict[Text, Parameter]: parameters
        """
        self._parameters = parameters

    @property
    def parameters(self):
        """
        :rtype: dict[Text, Parameter]
        """
        return self._parameters

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.interface_pb2.ParameterMap
        """
        return flyteidl.core.ParameterMap(
            parameters={k: v.to_flyte_idl() for k, v in self.parameters.items()},
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.interface_pb2.ParameterMap pb2_object:
        :rtype: ParameterMap
        """
        return cls(parameters={k: Parameter.from_flyte_idl(v) for k, v in pb2_object.parameters.items()})
