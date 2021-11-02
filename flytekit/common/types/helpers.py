import importlib as _importlib

import six as _six

from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.configuration import sdk as _sdk_config
from flytekit.models import literals as _literal_models


class _TypeEngineLoader(object):
    _LOADED_ENGINES = None
    _LAST_LOADED = None

    @classmethod
    def _load_engines(cls):
        config = _sdk_config.TYPE_ENGINES.get()
        if cls._LOADED_ENGINES is None or config != cls._LAST_LOADED:
            cls._LAST_LOADED = config
            cls._LOADED_ENGINES = []
            for fqdn in config:
                split = fqdn.split(".")
                module_path, attr = ".".join(split[:-1]), split[-1]
                module = _exception_scopes.user_entry_point(_importlib.import_module)(module_path)

                if not hasattr(module, attr):
                    raise _user_exceptions.FlyteValueException(
                        module,
                        "Failed to load the type engine because the attribute named '{}' could not be found"
                        "in the module '{}'.".format(attr, module_path),
                    )

                engine_impl = getattr(module, attr)()
                cls._LOADED_ENGINES.append(engine_impl)
            from flytekit.type_engines.default.flyte import FlyteDefaultTypeEngine as _DefaultEngine

            cls._LOADED_ENGINES.append(_DefaultEngine())

    @classmethod
    def iterate_engines_in_order(cls):
        """
        :rtype: Generator[flytekit.type_engines.common.TypeEngine]
        """
        cls._load_engines()
        return iter(cls._LOADED_ENGINES)


def python_std_to_sdk_type(t):
    """
    :param T t: User input.  Should be of the form: Types.Integer, [Types.Integer], {Types.String: Types.Integer}, etc.
    :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
    """
    for e in _TypeEngineLoader.iterate_engines_in_order():
        out = e.python_std_to_sdk_type(t)
        if out is not None:
            return out
    raise _user_exceptions.FlyteValueException(t, "Could not resolve to an SDK type for this value.")


def get_sdk_type_from_literal_type(literal_type):
    """
    :param flytekit.models.types.LiteralType literal_type:
    :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
    """
    for e in _TypeEngineLoader.iterate_engines_in_order():
        out = e.get_sdk_type_from_literal_type(literal_type)
        if out is not None:
            return out
    raise _user_exceptions.FlyteValueException(
        literal_type, "Could not resolve to a type implementation for this " "value."
    )


def infer_sdk_type_from_literal(literal):
    """
    :param flytekit.models.literals.Literal literal:
    :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
    """
    for e in _TypeEngineLoader.iterate_engines_in_order():
        out = e.infer_sdk_type_from_literal(literal)
        if out is not None:
            return out
    raise _user_exceptions.FlyteValueException(literal, "Could not resolve to a type implementation for this value.")


def get_sdk_value_from_literal(literal, sdk_type=None):
    """
    :param flytekit.models.literals.Literal literal:
    :param flytekit.models.types.LiteralType sdk_type:
    :rtype: flytekit.common.types.base_sdk_types.FlyteSdkValue
    """
    # The spec states everything must be nullable, so if we receive a null value, swap to the null type behavior.
    if sdk_type is None:
        sdk_type = infer_sdk_type_from_literal(literal)
    return sdk_type.from_flyte_idl(literal.to_flyte_idl())


def unpack_literal_map_to_sdk_object(literal_map, type_map=None):
    """
    :param lytekit.models.literals.LiteralMap literal_map:
    :param dict[Text, flytekit.common.types.base_sdk_types.FlyteSdkType] type_map: Type map directing unpacking.
    :rtype: dict[Text, T]
    """
    type_map = type_map or {}
    return {k: get_sdk_value_from_literal(v, sdk_type=type_map.get(k, None)) for k, v in literal_map.literals.items()}


def unpack_literal_map_to_sdk_python_std(literal_map, type_map=None):
    """
    :param flytekit.models.literals.LiteralMap literal_map: Literal map containing values for unpacking.
    :param dict[Text, flytekit.common.types.base_sdk_types.FlyteSdkType] type_map: Type map directing unpacking.
    :rtype: dict[Text, T]
    """
    return {k: v.to_python_std() for k, v in unpack_literal_map_to_sdk_object(literal_map, type_map=type_map).items()}


def pack_python_std_map_to_literal_map(std_map, type_map):
    """
    :param dict[Text, T] std_map:
    :param dict[Text, flytekit.common.types.base_sdk_types.FlyteSdkType] type_map:
    :rtype: flytekit.models.literals.LiteralMap
    :raises: flytekit.common.exceptions.user.FlyteTypeException
    """
    return _literal_models.LiteralMap(literals={k: v.from_python_std(std_map[k]) for k, v in _six.iteritems(type_map)})
