from __future__ import annotations

import dataclasses
import datetime as _datetime
import json as _json
import mimetypes
import os
import typing
from abc import ABC, abstractmethod
from typing import Type

from dataclasses_json import DataClassJsonMixin
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit.annotated.context_manager import FlyteContext
from flytekit.common.types import primitives as _primitives
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralCollection, LiteralMap, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType

T = typing.TypeVar("T")


class TypeTransformer(typing.Generic[T]):
    """
    Base transformer type that should be implemented for every python native type that can be handled by flytekit
    """

    def __init__(self, name: str, t: Type[T], enable_type_assertions: bool = True):
        self._t = t
        self._name = name
        self._type_assertions_enabled = enable_type_assertions

    @property
    def name(self):
        return self._name

    @property
    def python_type(self) -> Type[T]:
        """
        This returns the python type
        """
        return self._t

    @property
    def type_assertions_enabled(self) -> bool:
        """
        Indicates if the transformer wants type assertions to be enabled at the core type engine layer
        """
        return self._type_assertions_enabled

    @abstractmethod
    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Converts the python type to a Flyte LiteralType
        """
        raise NotImplementedError("Conversion to LiteralType should be implemented")

    @abstractmethod
    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        """
        Converts a given python_val to a Flyte Literal, assuming the given python_val matches the declared python_type.
        Implementers should refrain from using type(python_val) instead rely on the passed in python_type. If these
        do not match (or are not allowed) the Transformer implementer should raise an AssertionError, clearly stating
        what was the mismatch
        :param ctx: A FlyteContext, useful in accessing the filesystem and other attributes
        :param python_val: The actual value to be transformed
        :param python_type: The assumed type of the value (this matches the declared type on the function)
        :param expected: Expected Literal Type
        """
        raise NotImplementedError(f"Conversion to Literal for python type {python_type} not implemented")

    @abstractmethod
    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        """
        Converts the given Literal to a Python Type. If the conversion cannot be done an AssertionError should be raised
        :param ctx: FlyteContext
        :param lv: The received literal Value
        :param expected_python_type: Expected native python type that should be returned
        """
        raise NotImplementedError(
            f"Conversion to python value expected type {expected_python_type} from literal not implemented"
        )

    def __repr__(self):
        return f"{self._name} Transforms ({self._t}) to Flyte native"

    def __str__(self):
        return str(self.__repr__())


class SimpleTransformer(TypeTransformer[T]):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(
        self,
        name: str,
        t: Type[T],
        lt: LiteralType,
        to_literal_transformer: typing.Callable[[T], Literal],
        from_literal_transformer: typing.Callable[[Literal], T],
    ):
        super().__init__(name, t)
        self._lt = lt
        self._to_literal_transformer = to_literal_transformer
        self._from_literal_transformer = from_literal_transformer

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        return self._lt

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        return self._to_literal_transformer(python_val)

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        return self._from_literal_transformer(lv)


class RestrictedTypeError(Exception):
    pass


class RestrictedType(TypeTransformer[T], ABC):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(self, name: str, t: Type[T]):
        super().__init__(name, t)

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        raise RestrictedTypeError(f"Transformer for type{self.python_type} is restricted currently")


class DataclassTransformer(TypeTransformer[object]):
    def __init__(self):
        super().__init__("Object-Dataclass-Transformer", object)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        if not issubclass(t, DataClassJsonMixin):
            raise AssertionError(
                f"Dataclass {t} should be decorated with @dataclass_json to be " f"serialized correctly"
            )
        return _primitives.Generic.to_flyte_literal_type()

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if not dataclasses.is_dataclass(python_val):
            raise AssertionError(
                f"{type(python_val)} is not of type @dataclass, only Dataclasses are supported for "
                f"user defined datatypes in Flytekit"
            )
        if not issubclass(type(python_val), DataClassJsonMixin):
            raise AssertionError(
                f"Dataclass {python_type} should be decorated with @dataclass_json to be " f"serialized correctly"
            )
        return Literal(scalar=Scalar(generic=_json_format.Parse(python_val.to_json(), _struct.Struct())))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if not dataclasses.is_dataclass(expected_python_type):
            raise AssertionError(
                f"{expected_python_type} is not of type @dataclass, only Dataclasses are supported for "
                f"user defined datatypes in Flytekit"
            )
        if not issubclass(expected_python_type, DataClassJsonMixin):
            raise AssertionError(
                f"Dataclass {expected_python_type} should be decorated with @dataclass_json to be "
                f"serialized correctly"
            )
        dc = expected_python_type.from_json(_json_format.MessageToJson(lv.scalar.generic))
        # NOTE: Protobuf Struct does not support explicit int types, int types are upconverted to a double value
        # https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Value
        # Thus we will have to walk the given dataclass and typecast values to int, where expected.
        for f in dataclasses.fields(expected_python_type):
            if f.type == int:
                dc.__setattr__(f.name, int(dc.__getattribute__(f.name)))
        return dc


class TypeEngine(typing.Generic[T]):
    """
    Core Extensible TypeEngine of Flytekit. This should be used to extend the capabilities of FlyteKits type system.
    Users can implement their own TypeTransformers and register them with the TypeEngine. This will allow special handling
    of user objects
    """

    _REGISTRY: typing.Dict[type, TypeTransformer[T]] = {}
    _DATACLASS_TRANSFORMER: TypeTransformer = DataclassTransformer()

    @classmethod
    def register(cls, transformer: TypeTransformer):
        """
        This should be used for all types that respond with the right type annotation when you use type(...) function
        """
        if transformer.python_type in cls._REGISTRY:
            existing = cls._REGISTRY[transformer.python_type]
            raise ValueError(
                f"Transformer {existing.name} for type {transformer.python_type} is already registered."
                f" Cannot override with {transformer.name}"
            )
        cls._REGISTRY[transformer.python_type] = transformer

    @classmethod
    def get_transformer(cls, python_type: Type) -> TypeTransformer[T]:
        if python_type in cls._REGISTRY:
            return cls._REGISTRY[python_type]
        if hasattr(python_type, "__origin__"):
            if python_type.__origin__ in cls._REGISTRY:
                return cls._REGISTRY[python_type.__origin__]
            raise ValueError(f"Generic Type{python_type.__origin__} not supported currently in Flytekit.")
        if dataclasses.is_dataclass(python_type):
            return cls._DATACLASS_TRANSFORMER
        raise ValueError(f"Type{python_type} not supported currently in Flytekit. Please register a new transformer")

    @classmethod
    def to_literal_type(cls, python_type: Type) -> LiteralType:
        transformer = cls.get_transformer(python_type)
        return transformer.get_literal_type(python_type)

    @classmethod
    def to_literal(cls, ctx: FlyteContext, python_val: typing.Any, python_type: Type, expected: LiteralType) -> Literal:
        transformer = cls.get_transformer(python_type)
        lv = transformer.to_literal(ctx, python_val, python_type, expected)
        # TODO Perform assertion here
        return lv

    @classmethod
    def to_python_value(cls, ctx: FlyteContext, lv: Literal, expected_python_type: Type) -> typing.Any:
        transformer = cls.get_transformer(expected_python_type)
        return transformer.to_python_value(ctx, lv, expected_python_type)

    @classmethod
    def named_tuple_to_variable_map(cls, t: typing.NamedTuple) -> _interface_models.VariableMap:
        variables = {}
        for idx, (var_name, var_type) in enumerate(t._field_types.items()):
            literal_type = cls.to_literal_type(var_type)
            variables[var_name] = _interface_models.Variable(type=literal_type, description=f"{idx}")
        return _interface_models.VariableMap(variables=variables)

    @classmethod
    def literal_map_to_kwargs(
        cls, ctx: FlyteContext, lm: LiteralMap, python_types: typing.Dict[str, type]
    ) -> typing.Dict[str, typing.Any]:
        """
        Given a literal Map (usually an input into a task - intermediate), convert to kwargs for the task
        """
        if len(lm.literals) != len(python_types):
            raise ValueError(
                f"Received more input values {len(lm.literals)}" f" than allowed by the input spec {len(python_types)}"
            )

        return {k: TypeEngine.to_python_value(ctx, lm.literals[k], v) for k, v in python_types.items()}

    @classmethod
    def get_available_transformers(cls) -> typing.KeysView[Type]:
        """
        Returns all python types for which transformers are available
        """
        return cls._REGISTRY.keys()


class ListTransformer(TypeTransformer[T]):
    """
    Transformer that handles a univariate typing.List[T]
    """

    def __init__(self):
        super().__init__("Typed List", list)

    @staticmethod
    def get_sub_type(t: Type[T]) -> Type[T]:
        """
        Return the generic Type T of the List
        """
        if hasattr(t, "__origin__") and t.__origin__ is list:
            if hasattr(t, "__args__"):
                return t.__args__[0]
        raise ValueError("Only generic univariate typing.List[T] type is supported.")

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Only univariate Lists are supported in Flyte
        """
        try:
            sub_type = TypeEngine.to_literal_type(self.get_sub_type(t))
            return _type_models.LiteralType(collection_type=sub_type)
        except Exception as e:
            raise ValueError(f"Type of Generic List type is not supported, {e}")

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        t = self.get_sub_type(python_type)
        lit_list = [TypeEngine.to_literal(ctx, x, t, expected.collection_type) for x in python_val]
        return Literal(collection=LiteralCollection(literals=lit_list))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        st = self.get_sub_type(expected_python_type)
        return [TypeEngine.to_python_value(ctx, x, st) for x in lv.collection.literals]


class DictTransformer(TypeTransformer[dict]):
    """
    Transformer that transforms a univariate dictionary Dict[str, T] to a Literal Map or
    transforms a untyped dictionary to a JSON (struct/Generic)
    """

    def __init__(self):
        super().__init__("Typed Dict", dict)

    @staticmethod
    def get_dict_types(t: Type[dict]) -> (type, type):
        """
        Return the generic Type T of the Dict
        """
        if hasattr(t, "__origin__") and t.__origin__ is dict:
            if hasattr(t, "__args__"):
                return t.__args__
        return None, None

    @staticmethod
    def dict_to_generic_literal(v: dict) -> Literal:
        return Literal(scalar=Scalar(generic=_json_format.Parse(_json.dumps(v), _struct.Struct())))

    def get_literal_type(self, t: Type[dict]) -> LiteralType:
        tp = self.get_dict_types(t)
        if tp:
            if tp[0] == str:
                try:
                    sub_type = TypeEngine.to_literal_type(tp[1])
                    return _type_models.LiteralType(map_value_type=sub_type)
                except Exception as e:
                    raise ValueError(f"Type of Generic List type is not supported, {e}")
        return _primitives.Generic.to_flyte_literal_type()

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[dict], expected: LiteralType
    ) -> Literal:
        if expected and expected.simple and expected.simple == SimpleType.STRUCT:
            return self.dict_to_generic_literal(python_val)

        lit_map = {}
        for k, v in python_val.items():
            if type(k) != str:
                raise ValueError("Flyte MapType expects all keys to be strings")
            k_type, v_type = self.get_dict_types(python_type)
            lit_map[k] = TypeEngine.to_literal(ctx, v, v_type, expected.map_value_type)
        return Literal(map=LiteralMap(literals=lit_map))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[dict]) -> dict:
        if lv and lv.map and lv.map.literals:
            tp = self.get_dict_types(expected_python_type)
            if tp is None or tp[0] is None:
                raise TypeError(
                    "TypeMismatch: Cannot convert to python dictionary from Flyte Literal Dictionary as the given "
                    "dictionary ddoes not have sub-type hints or they do not match with the originating dictionary "
                    "source. Flytekit does not currently support implicit conversions"
                )
            if tp[0] != str:
                raise TypeError("TypeMismatch. Destination dictionary does not accept 'str' key")
            py_map = {}
            for k, v in lv.map.literals.items():
                py_map[k] = TypeEngine.to_python_value(ctx, v, tp[1])
            return py_map

        # for empty generic we have to explicitly test for lv.scalar.generic is not None as empty dict
        # evaluates to false
        if lv and lv.scalar and lv.scalar.generic is not None:
            return _json.loads(_json_format.MessageToJson(lv.scalar.generic))
        raise TypeError(f"Cannot convert from {lv} to {expected_python_type}")


class TextIOTransformer(TypeTransformer[typing.TextIO]):
    """
    Handler for TextIO
    """

    def __init__(self):
        super().__init__(name="TextIO", t=typing.TextIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".txt"], dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: typing.TextIO) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(),)

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.TextIO, python_type: Type[typing.TextIO], expected: LiteralType
    ) -> Literal:
        raise NotImplementedError("Implement handle for TextIO")

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[typing.TextIO]
    ) -> typing.TextIO:
        # TODO rename to get_auto_local_path()
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probably the responsibility of the framework to close() this
        return open(local_path, "r")


class BinaryIOTransformer(TypeTransformer[typing.BinaryIO]):
    """
    Handler for BinaryIO
    """

    def __init__(self):
        super().__init__(name="BinaryIO", t=typing.BinaryIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".bin"], dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[typing.BinaryIO]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(),)

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.BinaryIO, python_type: Type[typing.BinaryIO], expected: LiteralType
    ) -> Literal:
        raise NotImplementedError("Implement handle for TextIO")

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[typing.BinaryIO]
    ) -> typing.BinaryIO:
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probability the responsibility of the framework to close this
        return open(local_path, "rb")


class PathLikeTransformer(TypeTransformer[os.PathLike]):
    """
    Handler for os.PathLike
    """

    def __init__(self):
        super().__init__(name="os.PathLike", t=os.PathLike)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".bin"], dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[os.PathLike]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(),)

    def to_literal(
        self, ctx: FlyteContext, python_val: os.PathLike, python_type: Type[os.PathLike], expected: LiteralType
    ) -> Literal:
        # TODO we could guess the mimetype and allow the format to be changed at runtime. thus a non existent format
        #      could be replaced with a guess format?

        rpath = ctx.file_access.get_random_remote_path()

        # For remote values, say https://raw.github.com/demo_data.csv, we will not upload to Flyte's store (S3/GCS)
        # and just return a literal with a uri equal to the path given
        if ctx.file_access.is_remote(python_val):
            return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(expected.blob), uri=python_val)))

        # For local files, we'll upload for the user.
        ctx.file_access.put_data(python_val, rpath, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(expected.blob), uri=rpath)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[os.PathLike]) -> os.PathLike:
        # TODO rename to get_auto_local_path()
        local_destination_path = ctx.file_access.get_random_local_path()
        uri = lv.scalar.blob.uri
        # If the uri is just a local path like /tmp/file_name, we just return
        if not ctx.file_access.is_remote(uri):
            return uri

        # Since no delayed downloading is possible with strings, always download immediately.
        ctx.file_access.get_data(lv.scalar.blob.uri, local_destination_path, is_multipart=False)
        return local_destination_path


def _check_and_covert_float(lv: Literal) -> float:
    if lv.scalar.primitive.float_value:
        return lv.scalar.primitive.float_value
    elif lv.scalar.primitive.integer:
        return float(lv.scalar.primitive.integer)
    raise RuntimeError(f"Cannot convert literal {lv} to float")


def _register_default_type_transformers():
    TypeEngine.register(
        SimpleTransformer(
            "int",
            int,
            _primitives.Integer.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(integer=x))),
            lambda x: x.scalar.primitive.integer,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "float",
            float,
            _primitives.Float.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(float_value=x))),
            _check_and_covert_float,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "bool",
            bool,
            _primitives.Boolean.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(boolean=x))),
            lambda x: x.scalar.primitive.boolean,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "str",
            str,
            _primitives.String.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(string_value=x))),
            lambda x: x.scalar.primitive.string_value,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "datetime",
            _datetime.datetime,
            _primitives.Datetime.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(datetime=x))),
            lambda x: x.scalar.primitive.datetime,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "timedelta",
            _datetime.timedelta,
            _primitives.Timedelta.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(duration=x))),
            lambda x: x.scalar.primitive.duration,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "none", None, _type_models.LiteralType(simple=_type_models.SimpleType.NONE), lambda x: None, lambda x: None,
        )
    )
    TypeEngine.register(ListTransformer())
    TypeEngine.register(DictTransformer())
    TypeEngine.register(TextIOTransformer())
    TypeEngine.register(PathLikeTransformer())
    TypeEngine.register(BinaryIOTransformer())

    # inner type is. Also unsupported are typing's Tuples. Even though you can look inside them, Flyte's type system
    # doesn't support these currently.
    # Confusing note: typing.NamedTuple is in here even though task functions themselves can return them. We just mean
    # that the return signature of a task can be a NamedTuple that contains another NamedTuple inside it.
    # Also, it's not entirely true that Flyte IDL doesn't support tuples. We can always fake them as structs, but we'll
    # hold off on doing that for now, as we may amend the IDL formally to support tuples.
    TypeEngine.register(RestrictedType("non typed tuple", tuple))
    TypeEngine.register(RestrictedType("non typed tuple", typing.Tuple))
    TypeEngine.register(RestrictedType("named tuple", typing.NamedTuple))


_register_default_type_transformers()
