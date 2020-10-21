import datetime as _datetime
import functools
import typing
from abc import abstractmethod
from typing import Dict

from flytekit import typing as flyte_typing
from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.types import primitives as _primitives
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Primitive, Scalar
from flytekit.models.types import LiteralType

T = typing.TypeVar("T")


class TypeTransformer(typing.Generic[T]):
    """
    Base transformer type that should be implemented for every python native type that can be handled by flytekit
    """

    def __init__(self, name: str, t: type):
        self._t = t
        self._name = name

    @property
    def name(self):
        return self._name

    @property
    def python_type(self) -> type:
        """
        This returns the python type
        """
        return self._t

    @abstractmethod
    def get_literal_type(self, t: type) -> LiteralType:
        """
        Converts the python type to a Flyte LiteralType
        """
        raise NotImplementedError("Conversion to LiteralType should be implemented")

    @abstractmethod
    def get_literal(self, t: T) -> Literal:
        """
        Transforms the object in given type T to a Literal
        """
        raise NotImplementedError("Conversion to Literal should be implemented")

    def __repr__(self):
        return f"{self._name} Transforms ({self._t}) to Flyte native"

    def __str__(self):
        return str(self.__repr__())


class SimpleTransformer(TypeTransformer[T]):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(self, name: str, t: type, lt: LiteralType, transformer: typing.Callable[[T], Literal]):
        super().__init__(name, t)
        self._lt = lt
        self._transformer = transformer

    def get_literal_type(self, t=None) -> LiteralType:
        return self._lt

    def get_literal(self, t: T) -> Literal:
        return self._transformer(t)


class RestrictedTypeError(Exception):
    pass


class RestrictedType(TypeTransformer[T]):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(self, name: str, t: type):
        super().__init__(name, t)

    def get_literal_type(self, t=None) -> LiteralType:
        raise RestrictedTypeError(f"Transformer for type{self.python_type} is restricted currently")

    def get_literal(self, t: T) -> Literal:
        raise RestrictedTypeError(f"Transformer for type{self.python_type} is restricted currently")


class TypeEngine(object):
    _REGISTRY: typing.Dict[type, TypeTransformer[T]] = {}

    @classmethod
    def register(cls, transformer: TypeTransformer):
        """
        This should be used for all types that respond with the right type annotation when you use type(...) function
        """
        if transformer.python_type in cls._REGISTRY:
            existing = cls._REGISTRY[transformer.python_type]
            raise ValueError(
                f"Transformer f{existing.name} for type{transformer.python_type} is already registered."
                f" Cannot override with {transformer.name}")
        cls._REGISTRY[transformer.python_type] = transformer

    @classmethod
    def get_transformer(cls, python_type: type) -> TypeTransformer[T]:
        if python_type in cls._REGISTRY:
            return cls._REGISTRY[python_type]
        if hasattr(python_type, "__origin__"):
            if python_type.__origin__ in cls._REGISTRY:
                return cls._REGISTRY[python_type.__origin__]
            raise ValueError(f"Generic Type{python_type.__origin__} not supported currently in Flytekit.")
        raise ValueError(f"Type{python_type} not supported currently in Flytekit. Please register a new transformer")

    @classmethod
    def to_literal_type(cls, python_type: type) -> LiteralType:
        transformer = cls.get_transformer(python_type)
        return transformer.get_literal_type(python_type)

    def get_available_transformers(self) -> typing.KeysView[type]:
        """
        Returns all python types for which transformers are available
        """
        return self._REGISTRY.keys()


TypeEngine.register(SimpleTransformer(
    "int", int, _primitives.Integer.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(integer=x)))),
)

TypeEngine.register(SimpleTransformer(
    "float", float, _primitives.Float.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(float_value=x)))),
)

TypeEngine.register(SimpleTransformer(
    "bool", bool, _primitives.Boolean.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(boolean=x)))),
)

TypeEngine.register(SimpleTransformer(
    "str", str, _primitives.String.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(string_value=x)))),
)

TypeEngine.register(SimpleTransformer(
    "datetime", _datetime.datetime, _primitives.Datetime.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(datetime=x)))),
)

TypeEngine.register(SimpleTransformer(
    "timedelta", _datetime.timedelta, _primitives.Timedelta.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(duration=x)))),
)

TypeEngine.register(SimpleTransformer(
    "none", None, _type_models.LiteralType(simple=_type_models.SimpleType.NONE),
    lambda x: None),
)

"""
Is this right? Does this work, or should we do
json.loads(json.dumps(x))
"""
TypeEngine.register(SimpleTransformer(
    "dict", dict, _primitives.Generic.to_flyte_literal_type(),
    lambda x: Literal(scalar=Scalar(generic=x))),
)


def create_blob(dim: _core_types.BlobType, path: str) -> Literal:
    return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(type=dim), uri=path)))


TypeEngine.register(SimpleTransformer(
    "TextIO", typing.TextIO,
    _type_models.LiteralType(
        blob=_core_types.BlobType(
            format=flyte_typing.FlyteFileFormats.TEXT_IO.value,
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )
    ),
    functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE))
)

TypeEngine.register(SimpleTransformer(
    "BinaryIO", typing.BinaryIO,
    _type_models.LiteralType(
        blob=_core_types.BlobType(
            format=flyte_typing.FlyteFileFormats.BINARY_IO.value,
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )
    ),
    functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE)),
)

TypeEngine.register(SimpleTransformer(
    "FlyteFilePath", flyte_typing.FlyteFilePath,
    _type_models.LiteralType(
        blob=_core_types.BlobType(
            format=flyte_typing.FlyteFileFormats.BASE_FORMAT.value,
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )
    ),
    functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE)),
)

TypeEngine.register(SimpleTransformer(
    "FlyteFilePath", flyte_typing.FlyteCSVFilePath,
    _type_models.LiteralType(
        blob=_core_types.BlobType(
            format=flyte_typing.FlyteFileFormats.CSV.value,
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )
    ),
    functools.partial(create_blob, dim=_core_types.BlobType.BlobDimensionality.SINGLE)),
)


class ListTransformer(TypeTransformer[T]):
    def __init__(self):
        super().__init__("Typed List", list)

    def get_literal_type(self, t: type) -> LiteralType:
        if hasattr(t, "__origin__") and t.__origin__ is list:
            if hasattr(t, "__args__"):
                try:
                    sub_type = TypeEngine.to_literal_type(t.__args__[0])
                    return _type_models.LiteralType(collection_type=sub_type)
                except Exception as e:
                    raise ValueError(f"Type of Generic List type is not supported, {e}")
        raise ValueError("Only generic typing.List[T] type is supported.")

    def get_literal(self, t: T) -> Literal:
        raise NotImplementedError("We need to rationalize this with the type engine stuff")


TypeEngine.register(ListTransformer())


class DictTransformer(TypeTransformer[T]):
    def __init__(self):
        super().__init__("Typed Dict", dict)

    def get_literal_type(self, t: type) -> LiteralType:
        if hasattr(t, "__origin__") and t.__origin__ is dict:
            if hasattr(t, "__args__"):
                if t.__args__[0] != str:
                    raise user_exceptions.FlyteUserException(
                        f"Key type for hash tables must be of type str, given: {t}")
                try:
                    sub_type = TypeEngine.to_literal_type(t.__args__[1])
                    return _type_models.LiteralType(map_value_type=sub_type)
                except Exception as e:
                    raise ValueError(f"Type of Generic List type is not supported, {e}")
        raise ValueError("Only generic typing.List[T] type is supported.")

    def get_literal(self, t: T) -> Literal:
        raise NotImplementedError("We need to rationalize this with the type engine stuff")


# These are not supported in the context of Flyte's type management, because there is no way to peek into what the
# inner type is. Also unsupported are typing's Tuples. Even though you can look inside them, Flyte's type system
# doesn't support these currently.
# Confusing note: typing.NamedTuple is in here even though task functions themselves can return them. We just mean
# that the return signature of a task can be a NamedTuple that contains another NamedTuple inside it.
# Also, it's not entirely true that Flyte IDL doesn't support tuples. We can always fake them as structs, but we'll
# hold off on doing that for now, as we may amend the IDL formally to support tuples.
TypeEngine.register(RestrictedType("non typed tuple", tuple))
TypeEngine.register(RestrictedType("non typed tuple", typing.Tuple))
TypeEngine.register(RestrictedType("named tuple", typing.NamedTuple))


# This only goes one way for now, is there any need to go the other way?
# TODO more changes to the type engine to include types
class BaseEngine(object):

    def native_type_to_literal_type(self, native_type: type) -> _type_models.LiteralType:
        return TypeEngine.to_literal_type(native_type)

    def named_tuple_to_variable_map(self, t: typing.NamedTuple) -> _interface_models.VariableMap:
        variables = {}
        for idx, (var_name, var_type) in enumerate(t._field_types.items()):
            literal_type = self.native_type_to_literal_type(var_type)
            variables[var_name] = _interface_models.Variable(type=literal_type, description=f"{idx}")
        return _interface_models.VariableMap(variables=variables)
