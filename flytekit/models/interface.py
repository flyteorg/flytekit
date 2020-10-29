import six as _six
from flyteidl.core import interface_pb2 as _interface_pb2

from flytekit.models import common as _common
from flytekit.models import literals as _literals
from flytekit.models import types as _types


class Variable(_common.FlyteIdlEntity):
    def __init__(self, type, description):
        """
        :param flytekit.models.types.LiteralType type: This describes the type of value that must be provided to
            satisfy this variable.
        :param Text description: This is a help string that can provide context for what this variable means in relation
            to a task or workflow.
        """
        self._type = type
        self._description = description

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

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.interface_pb2.Variable
        """
        return _interface_pb2.Variable(type=self.type.to_flyte_idl(), description=self.description)

    @classmethod
    def from_flyte_idl(cls, variable_proto):
        """
        :param flyteidl.core.interface_pb2.Variable variable_proto:
        :rtype: Variable
        """
        return cls(type=_types.LiteralType.from_flyte_idl(variable_proto.type), description=variable_proto.description,)


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
        return _interface_pb2.VariableMap(variables={k: v.to_flyte_idl() for k, v in _six.iteritems(self.variables)})

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param dict[Text, Variable] pb2_object:
        :rtype: VariableMap
        """
        return cls({k: Variable.from_flyte_idl(v) for k, v in _six.iteritems(pb2_object.variables)})


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
    def inputs(self):
        """
        :rtype: dict[Text, Variable]
        """
        return self._inputs

    @property
    def outputs(self):
        """
        :rtype: dict[Text, Variable]
        """
        return self._outputs

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.interface_pb2.TypedInterface
        """
        return _interface_pb2.TypedInterface(
            inputs=_interface_pb2.VariableMap(variables={k: v.to_flyte_idl() for k, v in _six.iteritems(self.inputs)}),
            outputs=_interface_pb2.VariableMap(
                variables={k: v.to_flyte_idl() for k, v in _six.iteritems(self.outputs)}
            ),
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.interface_pb2.TypedInterface proto:
        :rtype: TypedInterface
        """
        return cls(
            inputs={k: Variable.from_flyte_idl(v) for k, v in _six.iteritems(proto.inputs.variables)},
            outputs={k: Variable.from_flyte_idl(v) for k, v in _six.iteritems(proto.outputs.variables)},
        )


class Parameter(_common.FlyteIdlEntity):
    def __init__(self, var, default=None, required=None):
        """
        Declares an input parameter.  A parameter is used as input to a launch plan and has
            the special ability to have a default value or mark itself as required.
        :param Variable var: Defines a name and a type to reference/compare through out the system.
        :param flytekit.models.literals.Literal default: [Optional] Defines a default value that has to match the
            variable type defined.
        :param bool required: [Optional] is this value required to be filled in?
        """
        self._var = var
        self._default = default
        self._required = required

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
        return self._default or self._required

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.interface_pb2.Parameter
        """
        return _interface_pb2.Parameter(
            var=self.var.to_flyte_idl(),
            default=self.default.to_flyte_idl() if self.default is not None else None,
            required=self.required if self.default is None else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.interface_pb2.Parameter pb2_object:
        :rtype: Parameter
        """
        return cls(
            Variable.from_flyte_idl(pb2_object.var),
            _literals.Literal.from_flyte_idl(pb2_object.default) if pb2_object.HasField("default") else None,
            pb2_object.required if pb2_object.HasField("required") else None,
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
        return _interface_pb2.ParameterMap(
            parameters={k: v.to_flyte_idl() for k, v in _six.iteritems(self.parameters)},
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.interface_pb2.ParameterMap pb2_object:
        :rtype: ParameterMap
        """
        return cls(parameters={k: Parameter.from_flyte_idl(v) for k, v in _six.iteritems(pb2_object.parameters)})
