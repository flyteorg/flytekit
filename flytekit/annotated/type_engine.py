import datetime as _datetime
import functools
import json
import typing
from typing import Dict

from flytekit import typing as flyte_typing
from flytekit.common.exceptions import system as system_exceptions
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.types import primitives as _primitives
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Primitive, Scalar


def create_int_literal(x: int) -> Literal:
    return Literal(scalar=Scalar(primitive=Primitive(integer=x)))


def create_float_literal(x: float) -> Literal:
    return Literal(scalar=Scalar(primitive=Primitive(float_value=x)))


def create_bool_literal(x: bool) -> Literal:
    return Literal(scalar=Scalar(primitive=Primitive(boolean=x)))


def create_str_literal(x: str) -> Literal:
    return Literal(scalar=Scalar(primitive=Primitive(string_value=x)))


def create_datetime_literal(x: _datetime.datetime) -> Literal:
    return Literal(scalar=Scalar(primitive=Primitive(datetime=x)))


def create_timedelta(x: _datetime.timedelta) -> Literal:
    return Literal(scalar=Scalar(primitive=Primitive(duration=x)))


def create_generic(x: typing.Dict) -> Literal:
    """
    Is this right? Does this work, or should we do
    json.loads(json.dumps(x))
    """
    return Literal(scalar=Scalar(generic=x))


def create_blob(dim: _core_types.BlobType, path: str) -> Literal:
    return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(type=dim), uri=path)))


# This is now in three different places. This here, the one that the notebook task uses, and the main in that meshes
# with the engine loader. I think we should get rid of the loader (can add back when/if we ever have more than one).
# All three should be merged into the existing one.
# TODO: Change the values to reference the literal model type directly.
BASE_TYPES: Dict[type, typing.Tuple[_type_models.LiteralType, typing.Callable]] = {
    int: (_primitives.Integer.to_flyte_literal_type(), create_int_literal),
    float: (_primitives.Float.to_flyte_literal_type(), create_float_literal),
    bool: (_primitives.Boolean, create_bool_literal),
    _datetime.datetime: (_primitives.Datetime.to_flyte_literal_type(), create_datetime_literal),
    _datetime.timedelta: (_primitives.Timedelta.to_flyte_literal_type(), create_timedelta),
    str: (_primitives.String.to_flyte_literal_type(), create_str_literal),
    # TODO: Not sure what to do about this yet
    dict: (_primitives.Generic.to_flyte_literal_type(), create_generic),
    None: (_type_models.LiteralType(simple=_type_models.SimpleType.NONE,), None),
    typing.TextIO: (
        _type_models.LiteralType(
            blob=_core_types.BlobType(
                format=flyte_typing.FlyteFileFormats.TEXT_IO.value,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        ),
        functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE),
    ),
    typing.BinaryIO: (
        _type_models.LiteralType(
            blob=_core_types.BlobType(
                format=flyte_typing.FlyteFileFormats.BINARY_IO.value,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        ),
        functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE),
    ),
    flyte_typing.FlyteFilePath: (
        _type_models.LiteralType(
            blob=_core_types.BlobType(
                format=flyte_typing.FlyteFileFormats.BASE_FORMAT.value,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        ),
        functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE),
    ),
    flyte_typing.FlyteCSVFilePath: (
        _type_models.LiteralType(
            blob=_core_types.BlobType(
                format=flyte_typing.FlyteFileFormats.CSV.value,
                dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
            )
        ),
        functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE),
    ),
}

CONTAINER_TYPES = [typing.Dict, typing.List]

# These are not supported in the context of Flyte's type management, because there is no way to peek into what the
# inner type is. Also unsupported are typing's Tuples. Even though you can look inside them, Flyte's type system
# doesn't support these currently.
# Confusing note: typing.NamedTuple is in here even though task functions themselves can return them. We just mean
# that the return signature of a task can be a NamedTuple that contains another NamedTuple inside it.
# Also, it's not entirely true that Flyte IDL doesn't support tuples. We can always fake them as structs, but we'll
# hold off on doing that for now, as we may amend the IDL formally to support tuples.
UNSUPPORTED_CONTAINERS = [tuple, list, dict, typing.Tuple, typing.NamedTuple]


# This only goes one way for now, is there any need to go the other way?
class BaseEngine(object):
    def native_type_to_literal_type(self, native_type: type) -> _type_models.LiteralType:
        if native_type in BASE_TYPES:
            return BASE_TYPES[native_type][0]

        # Handle container types like typing.List and Dict. However, since these are implemented as Generics, and
        # you can't just do runtime object comparison. i.e. if:
        #   t = typing.List[int]
        #   t in [typing.List]  # False
        #   isinstance(t, typing.List)  # False
        # so we have to look inside the type's hidden attributes
        if hasattr(native_type, "__origin__") and native_type.__origin__ is list:
            return self._type_unpack_list(native_type)

        if hasattr(native_type, "__origin__") and native_type.__origin__ is dict:
            return self._type_unpack_dict(native_type)

        raise user_exceptions.FlyteUserException(f"Python type {native_type} is not supported")

    def _type_unpack_dict(self, t) -> _type_models.LiteralType:
        if t.__origin__ != dict:
            raise user_exceptions.FlyteUserException(
                f"Attempting to analyze dict but non-dict " f"type given {t.__origin__}"
            )
        if t.__args__[0] != str:
            raise user_exceptions.FlyteUserException(f"Key type for hash tables must be of type str, given: {t}")

        sub_type = self.native_type_to_literal_type(t.__args__[1])
        return _type_models.LiteralType(map_value_type=sub_type)

    def _type_unpack_list(self, t) -> _type_models.LiteralType:
        if t.__origin__ != list:
            raise user_exceptions.FlyteUserException(
                f"Attempting to analyze list but non-list " f"type given {t.__origin__}"
            )

        sub_type = self.native_type_to_literal_type(t.__args__[0])
        return _type_models.LiteralType(collection_type=sub_type)

    def named_tuple_to_variable_map(self, t: typing.NamedTuple) -> _interface_models.VariableMap:
        variables = {}
        for idx, (var_name, var_type) in enumerate(t._field_types.items()):
            literal_type = self.native_type_to_literal_type(var_type)
            variables[var_name] = _interface_models.Variable(type=literal_type, description=f"{idx}")
        return _interface_models.VariableMap(variables=variables)
