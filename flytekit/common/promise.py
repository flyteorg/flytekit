from flytekit.common import constants as _constants
from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models


class Input(_interface_models.Parameter, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, name, sdk_type, help=None, **kwargs):
        """
        :param Text name:
        :param flytekit.common.types.base_sdk_types.FlyteSdkType sdk_type: This is the SDK type necessary to create an
            input to this workflow.
        :param Text help: An optional help string to describe the input to users.
        :param bool required: If set to True, default must be None
        :param T default:  If this is not a required input, the value will default to this value.
        """
        param_default = None
        if "required" not in kwargs and "default" not in kwargs:
            # Neither required or default is set so assume required
            required = True
            default = None
        elif kwargs.get("required", False) and "default" in kwargs:
            # Required cannot be set to True and have a default specified
            raise _user_exceptions.FlyteAssertion("Default cannot be set when required is True")
        elif "default" in kwargs:
            # If default is specified, then required must be false and the value is whatever is specified
            required = None
            default = kwargs["default"]
            param_default = sdk_type.from_python_std(default)
        else:
            # If no default is set, but required is set, then the behavior is determined by required == True or False
            default = None
            required = kwargs["required"]
            if not required:
                # If required == False, we assume default to be None
                param_default = sdk_type.from_python_std(default)
                required = None

        self._sdk_required = required or False
        self._sdk_default = default
        self._help = help
        self._sdk_type = sdk_type
        self._promise = _type_models.OutputReference(_constants.GLOBAL_INPUT_NODE_ID, name)
        self._name = name
        super(Input, self).__init__(
            _interface_models.Variable(type=sdk_type.to_flyte_literal_type(), description=help or ""),
            required=required,
            default=param_default,
        )

    def rename_and_return_reference(self, new_name):
        self._promise._var = new_name
        return self

    @property
    def name(self):
        """
        :rtype: Text
        """
        return self._promise.var

    @property
    def promise(self):
        """
        :rtype: flytekit.models.types.OutputReference
        """
        return self._promise

    @property
    def sdk_required(self):
        """
        :rtype: bool
        """
        return self._sdk_required

    @property
    def sdk_default(self):
        """
        :rtype: T
        """
        return self._sdk_default

    @property
    def help(self):
        """
        :rtype: Text
        """
        return self._help

    @property
    def sdk_type(self):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        return self._sdk_type

    def __repr__(self):
        return "Input({}, {}, required={}, help={})".format(self.name, self.sdk_type, self.required, self.help)

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.interface.Parameter model:
        :rtype: Input
        """
        sdk_type = _type_helpers.get_sdk_type_from_literal_type(model.var.type)

        if model.default is not None:
            default_value = sdk_type.from_flyte_idl(model.default.to_flyte_idl()).to_python_std()
            return cls("", sdk_type, help=model.var.description, required=False, default=default_value,)
        else:
            return cls("", sdk_type, help=model.var.description, required=True)


class NodeOutput(_type_models.OutputReference, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, sdk_node, sdk_type, var):
        """
        :param sdk_node:
        :param sdk_type: deprecated in mypy flytekit.
        :param var:
        """
        self._node = sdk_node
        self._type = sdk_type
        super(NodeOutput, self).__init__(self._node.id, var)

    @property
    def node_id(self):
        """
        Override the underlying node_id property to refer to SdkNode.
        :rtype: Text
        """
        return self.sdk_node.id

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.types.OutputReference model:
        :rtype: NodeOutput
        """
        raise _user_exceptions.FlyteAssertion(
            "A NodeOutput cannot be promoted from a protobuf because it must be "
            "contextualized by an existing SdkNode."
        )

    @property
    def sdk_node(self):
        """
        :rtype: flytekit.common.nodes.SdkNode
        """
        return self._node

    @property
    def sdk_type(self):
        """
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        return self._type

    def __repr__(self):
        s = f"NodeOutput({self.sdk_node if self.sdk_node.id is not None else None}:{self.var})"
        return s
