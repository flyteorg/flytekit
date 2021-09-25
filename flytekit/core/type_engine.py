from __future__ import annotations

import copy
import dataclasses
import datetime as _datetime
import enum
import inspect
import json as _json
import mimetypes
import typing
from abc import ABC, abstractmethod
from typing import Optional, Type, cast

from dataclasses_json import DataClassJsonMixin, dataclass_json
from google.protobuf import json_format as _json_format
from google.protobuf import reflection as _proto_reflection
from google.protobuf import struct_pb2 as _struct
from google.protobuf.json_format import MessageToDict as _MessageToDict
from google.protobuf.json_format import ParseDict as _ParseDict
from google.protobuf.struct_pb2 import Struct
from marshmallow_jsonschema import JSONSchema

from flytekit.common.exceptions import user as user_exceptions
from flytekit.common.types import primitives as _primitives
from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_helpers import load_type_from_tag
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Literal, LiteralCollection, LiteralMap, Primitive, Scalar
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

    def assert_type(self, t: Type[T], v: T):
        if not hasattr(t, "__origin__") and not isinstance(v, t):
            raise TypeError(f"Type of Val '{v}' is not an instance of {t}")

    @abstractmethod
    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Converts the python type to a Flyte LiteralType
        """
        raise NotImplementedError("Conversion to LiteralType should be implemented")

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        """
        Converts the Flyte LiteralType to a python object type.
        """
        raise ValueError("By default, transformers do not translate from Flyte types back to Python types")

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

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if literal_type.simple is not None and literal_type.simple == self._lt.simple:
            return self.python_type
        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class RestrictedTypeError(Exception):
    pass


class RestrictedType(TypeTransformer[T], ABC):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(self, name: str, t: Type[T]):
        super().__init__(name, t)

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        raise RestrictedTypeError(f"Transformer for type {self.python_type} is restricted currently")


class DataclassTransformer(TypeTransformer[object]):
    """
    The Dataclass Transformer, provides a type transformer for arbitrary Python dataclasses, that have
    @dataclass and @dataclass_json decorators.

    The Dataclass is converted to and from json and is transported between tasks using the proto.Structpb representation
    Also the type declaration will try to extract the JSON Schema for the object if possible and pass it with the
    definition.

    For Json Schema, we use https://github.com/fuhrysteve/marshmallow-jsonschema library.

    Example

    .. code-block:: python

        @dataclass_json
        @dataclass
        class Test():
           a: int
           b: str

        from marshmallow_jsonschema import JSONSchema
        t = Test(a=10,b="e")
        JSONSchema().dump(t.schema())

    Output will look like

    .. code-block:: json

        {'$schema': 'http://json-schema.org/draft-07/schema#',
         'definitions': {'TestSchema': {'properties': {'a': {'title': 'a',
             'type': 'number',
             'format': 'integer'},
            'b': {'title': 'b', 'type': 'string'}},
           'type': 'object',
           'additionalProperties': False}},
         '$ref': '#/definitions/TestSchema'}

    .. note::

        The schema support is experimental and is useful for auto-completing in the UI/CLI

    """

    def __init__(self):
        super().__init__("Object-Dataclass-Transformer", object)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Extracts the Literal type definition for a Dataclass and returns a type Struct.
        If possible also extracts the JSONSchema for the dataclass.
        """
        if not issubclass(t, DataClassJsonMixin):
            raise AssertionError(
                f"Dataclass {t} should be decorated with @dataclass_json to be " f"serialized correctly"
            )
        schema = None
        try:
            schema = JSONSchema().dump(cast(DataClassJsonMixin, t).schema())
        except Exception as e:
            logger.warn("failed to extract schema for object %s, (will run schemaless) error: %s", str(t), e)

        return _primitives.Generic.to_flyte_literal_type(metadata=schema)

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
        return Literal(
            scalar=Scalar(generic=_json_format.Parse(cast(DataClassJsonMixin, python_val).to_json(), _struct.Struct()))
        )

    def _fix_val_int(self, t: typing.Type, val: typing.Any) -> typing.Any:
        if t == int:
            return int(val)

        if isinstance(val, list):
            # Handle nested List. e.g. [[1, 2], [3, 4]]
            return list(map(lambda x: self._fix_val_int(ListTransformer.get_sub_type(t), x), val))

        if isinstance(val, dict):
            ktype, vtype = DictTransformer.get_dict_types(t)
            # Handle nested Dict. e.g. {1: {2: 3}, 4: {5: 6}})
            return {self._fix_val_int(ktype, k): self._fix_val_int(vtype, v) for k, v in val.items()}

        if dataclasses.is_dataclass(t):
            return self._fix_dataclass_int(t, val)  # type: ignore

        return val

    def _fix_dataclass_int(self, dc_type: Type[DataClassJsonMixin], dc: DataClassJsonMixin) -> DataClassJsonMixin:
        """
        This is a performance penalty to convert to the right types, but this is expected by the user and hence
        needs to be done
        """
        # NOTE: Protobuf Struct does not support explicit int types, int types are upconverted to a double value
        # https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Value
        # Thus we will have to walk the given dataclass and typecast values to int, where expected.
        for f in dataclasses.fields(dc_type):
            val = dc.__getattribute__(f.name)
            dc.__setattr__(f.name, self._fix_val_int(f.type, val))
        return dc

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
        dc = cast(DataClassJsonMixin, expected_python_type).from_json(_json_format.MessageToJson(lv.scalar.generic))
        return self._fix_dataclass_int(expected_python_type, dc)


class ProtobufTransformer(TypeTransformer[_proto_reflection.GeneratedProtocolMessageType]):
    PB_FIELD_KEY = "pb_type"

    def __init__(self):
        super().__init__("Protobuf-Transformer", _proto_reflection.GeneratedProtocolMessageType)

    @staticmethod
    def tag(expected_python_type: Type[T]) -> str:
        return f"{expected_python_type.__module__}.{expected_python_type.__name__}"

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(simple=SimpleType.STRUCT, metadata={ProtobufTransformer.PB_FIELD_KEY: self.tag(t)})

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        struct = Struct()
        struct.update(_MessageToDict(python_val))
        return Literal(scalar=Scalar(generic=struct))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if not (lv and lv.scalar and lv.scalar.generic is not None):
            raise AssertionError("Can only convert a generic literal to a Protobuf")

        pb_obj = expected_python_type()
        dictionary = _MessageToDict(lv.scalar.generic)
        pb_obj = _ParseDict(dictionary, pb_obj)
        return pb_obj

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if (
            literal_type.simple == SimpleType.STRUCT
            and literal_type.metadata
            and literal_type.metadata.get(self.PB_FIELD_KEY, "")
        ):
            tag = literal_type.metadata[self.PB_FIELD_KEY]
            return load_type_from_tag(tag)
        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class TypeEngine(typing.Generic[T]):
    """
    Core Extensible TypeEngine of Flytekit. This should be used to extend the capabilities of FlyteKits type system.
    Users can implement their own TypeTransformers and register them with the TypeEngine. This will allow special handling
    of user objects
    """

    _REGISTRY: typing.Dict[type, TypeTransformer[T]] = {}
    _DATACLASS_TRANSFORMER: TypeTransformer = DataclassTransformer()

    @classmethod
    def register(cls, transformer: TypeTransformer, additional_types: Optional[typing.List[Type]] = None):
        """
        This should be used for all types that respond with the right type annotation when you use type(...) function
        """
        types = [transformer.python_type, *(additional_types or [])]
        for t in types:
            if t in cls._REGISTRY:
                existing = cls._REGISTRY[t]
                raise ValueError(
                    f"Transformer {existing.name} for type {t} is already registered."
                    f" Cannot override with {transformer.name}"
                )
            cls._REGISTRY[t] = transformer

    @classmethod
    def get_transformer(cls, python_type: Type) -> TypeTransformer[T]:
        """
        The TypeEngine hierarchy for flyteKit. This method looksup and selects the type transformer. The algorithm is
        as follows

          d = dictionary of registered transformers, where is a python `type`
          v = lookup type
        Step 1:
            find a transformer that matches v exactly

        Step 2:
            find a transformer that matches the generic type of v. e.g List[int], Dict[str, int] etc

        Step 3:
            if v is of type data class, use the dataclass transformer

        Step 4:
            Walk the inheritance hierarchy of v and find a transformer that matches the first base class.
            This is potentially non-deterministic - will depend on the registration pattern.

            TODO lets make this deterministic by using an ordered dict

        """
        # Step 1
        if python_type in cls._REGISTRY:
            return cls._REGISTRY[python_type]

        # Step 2
        if hasattr(python_type, "__origin__"):
            if python_type.__origin__ in cls._REGISTRY:
                return cls._REGISTRY[python_type.__origin__]
            raise ValueError(f"Generic Type {python_type.__origin__} not supported currently in Flytekit.")

        # Step 3
        if dataclasses.is_dataclass(python_type):
            return cls._DATACLASS_TRANSFORMER

        # To facilitate cases where users may specify one transformer for multiple types that all inherit from one
        # parent.
        for base_type in cls._REGISTRY.keys():
            if base_type is None:
                continue  # None is actually one of the keys, but isinstance/issubclass doesn't work on it
            if isinstance(python_type, base_type) or (
                inspect.isclass(python_type) and issubclass(python_type, base_type)
            ):
                return cls._REGISTRY[base_type]
        raise ValueError(f"Type {python_type} not supported currently in Flytekit. Please register a new transformer")

    @classmethod
    def to_literal_type(cls, python_type: Type) -> LiteralType:
        """
        Converts a python type into a flyte specific ``LiteralType``
        """
        transformer = cls.get_transformer(python_type)
        return transformer.get_literal_type(python_type)

    @classmethod
    def to_literal(cls, ctx: FlyteContext, python_val: typing.Any, python_type: Type, expected: LiteralType) -> Literal:
        """
        Converts a python value of a given type and expected ``LiteralType`` into a resolved ``Literal`` value.
        """
        if python_val is None:
            raise AssertionError(f"Python value cannot be None, expected {python_type}/{expected}")
        transformer = cls.get_transformer(python_type)
        if transformer.type_assertions_enabled:
            transformer.assert_type(python_type, python_val)
        lv = transformer.to_literal(ctx, python_val, python_type, expected)
        return lv

    @classmethod
    def to_python_value(cls, ctx: FlyteContext, lv: Literal, expected_python_type: Type) -> typing.Any:
        """
        Converts a Literal value with an expected python type into a python value.
        """
        transformer = cls.get_transformer(expected_python_type)
        return transformer.to_python_value(ctx, lv, expected_python_type)

    @classmethod
    def named_tuple_to_variable_map(cls, t: typing.NamedTuple) -> _interface_models.VariableMap:
        """
        Converts a python-native ``NamedTuple`` to a flyte-specific VariableMap of named literals.
        """
        variables = {}
        for idx, (var_name, var_type) in enumerate(t.__annotations__.items()):
            literal_type = cls.to_literal_type(var_type)
            variables[var_name] = _interface_models.Variable(type=literal_type, description=f"{idx}")
        return _interface_models.VariableMap(variables=variables)

    @classmethod
    def literal_map_to_kwargs(
        cls, ctx: FlyteContext, lm: LiteralMap, python_types: typing.Dict[str, type]
    ) -> typing.Dict[str, typing.Any]:
        """
        Given a ``LiteralMap`` (usually an input into a task - intermediate), convert to kwargs for the task
        """
        if len(lm.literals) != len(python_types):
            raise ValueError(
                f"Received more input values {len(lm.literals)}" f" than allowed by the input spec {len(python_types)}"
            )
        return {k: TypeEngine.to_python_value(ctx, lm.literals[k], v) for k, v in python_types.items()}

    @classmethod
    def dict_to_literal_map(
        cls,
        ctx: FlyteContext,
        d: typing.Dict[str, typing.Any],
        guessed_python_types: Optional[typing.Dict[str, type]] = None,
    ) -> LiteralMap:
        """
        Given a dictionary mapping string keys to python values and a dictionary containing guessed types for such string keys,
        convert to a LiteralMap.
        """
        guessed_python_types = guessed_python_types or {}
        literal_map = {}
        for k, v in d.items():
            # The guessed type takes precedence over the type returned by the python runtime. This is needed
            # to account for the type erasure that happens in the case of built-in collection containers, such as
            # `list` and `dict`.
            python_type = guessed_python_types.get(k, type(v))
            if (hasattr(python_type, "__origin__") and not isinstance(v, python_type.__origin__)) or (
                not hasattr(python_type, "__origin__") and not isinstance(v, python_type)
            ):
                raise user_exceptions.FlyteTypeException(type(v), python_type, received_value=v)
            literal_map[k] = TypeEngine.to_literal(
                ctx=ctx,
                python_val=v,
                python_type=python_type,
                expected=TypeEngine.to_literal_type(python_type),
            )
        return LiteralMap(literal_map)

    @classmethod
    def get_available_transformers(cls) -> typing.KeysView[Type]:
        """
        Returns all python types for which transformers are available
        """
        return cls._REGISTRY.keys()

    @classmethod
    def guess_python_types(
        cls, flyte_variable_dict: typing.Dict[str, _interface_models.Variable]
    ) -> typing.Dict[str, type]:
        """
        Transforms a dictionary of flyte-specific ``Variable`` objects to a dictionary of regular python values.
        """
        python_types = {}
        for k, v in flyte_variable_dict.items():
            python_types[k] = cls.guess_python_type(v.type)
        return python_types

    @classmethod
    def guess_python_type(cls, flyte_type: LiteralType) -> type:
        """
        Transforms a flyte-specific ``LiteralType`` to a regular python value.
        """
        for _, transformer in cls._REGISTRY.items():
            try:
                return transformer.guess_python_type(flyte_type)
            except ValueError:
                logger.debug(f"Skipping transformer {transformer.name} for {flyte_type}")
        raise ValueError(f"No transformers could reverse Flyte literal type {flyte_type}")


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
        if hasattr(t, "__origin__") and t.__origin__ is list:  # type: ignore
            if hasattr(t, "__args__"):
                return t.__args__[0]  # type: ignore
        raise ValueError("Only generic univariate typing.List[T] type is supported.")

    def get_literal_type(self, t: Type[T]) -> Optional[LiteralType]:
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
        lit_list = [TypeEngine.to_literal(ctx, x, t, expected.collection_type) for x in python_val]  # type: ignore
        return Literal(collection=LiteralCollection(literals=lit_list))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> typing.List[T]:
        st = self.get_sub_type(expected_python_type)
        return [TypeEngine.to_python_value(ctx, x, st) for x in lv.collection.literals]

    def guess_python_type(self, literal_type: LiteralType) -> Type[list]:
        if literal_type.collection_type:
            ct = TypeEngine.guess_python_type(literal_type.collection_type)
            return typing.List[ct]
        raise ValueError(f"List transformer cannot reverse {literal_type}")


class DictTransformer(TypeTransformer[dict]):
    """
    Transformer that transforms a univariate dictionary Dict[str, T] to a Literal Map or
    transforms a untyped dictionary to a JSON (struct/Generic)
    """

    def __init__(self):
        super().__init__("Typed Dict", dict)

    @staticmethod
    def get_dict_types(t: Optional[Type[dict]]) -> typing.Tuple[Optional[type], Optional[type]]:
        """
        Return the generic Type T of the Dict
        """
        if hasattr(t, "__origin__") and t.__origin__ is dict:  # type: ignore
            if hasattr(t, "__args__"):
                return t.__args__  # type: ignore
        return None, None

    @staticmethod
    def dict_to_generic_literal(v: dict) -> Literal:
        """
        Creates a flyte-specific ``Literal`` value from a native python dictionary.
        """
        return Literal(scalar=Scalar(generic=_json_format.Parse(_json.dumps(v), _struct.Struct())))

    def get_literal_type(self, t: Type[dict]) -> LiteralType:
        """
        Transforms a native python dictionary to a flyte-specific ``LiteralType``
        """
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
        if lv and lv.map and lv.map.literals is not None:
            tp = self.get_dict_types(expected_python_type)
            if tp is None or tp[0] is None:
                raise TypeError(
                    "TypeMismatch: Cannot convert to python dictionary from Flyte Literal Dictionary as the given "
                    "dictionary does not have sub-type hints or they do not match with the originating dictionary "
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

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if literal_type.map_value_type:
            mt = TypeEngine.guess_python_type(literal_type.map_value_type)
            return typing.Dict[str, mt]  # type: ignore

        if literal_type.simple == SimpleType.STRUCT:
            if literal_type.metadata is None:
                return dict
            if "definitions" in literal_type.metadata:
                return convert_json_schema_to_python_class(literal_type.metadata)

        raise ValueError(f"Dictionary transformer cannot reverse {literal_type}")


class TextIOTransformer(TypeTransformer[typing.TextIO]):
    """
    Handler for TextIO
    """

    def __init__(self):
        super().__init__(name="TextIO", t=typing.TextIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".txt"],
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: typing.TextIO) -> LiteralType:
        return _type_models.LiteralType(
            blob=self._blob_type(),
        )

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
            format=mimetypes.types_map[".bin"],
            dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[typing.BinaryIO]) -> LiteralType:
        return _type_models.LiteralType(
            blob=self._blob_type(),
        )

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


class EnumTransformer(TypeTransformer[enum.Enum]):
    """
    Enables converting a python type enum.Enum to LiteralType.EnumType
    """

    def __init__(self):
        super().__init__(name="DefaultEnumTransformer", t=enum.Enum)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        values = [v.value for v in t]  # type: ignore
        if not isinstance(values[0], str):
            raise AssertionError("Only EnumTypes with value of string are supported")
        return LiteralType(enum_type=_core_types.EnumType(values=values))

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        return Literal(scalar=Scalar(primitive=Primitive(string_value=python_val.value)))  # type: ignore

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        return expected_python_type(lv.scalar.primitive.string_value)


def convert_json_schema_to_python_class(schema):
    """Generate a model class based on the provided JSON Schema
    :param schema: dict representing valid JSON schema
    """
    schema = copy.deepcopy(schema)

    class Model(dict):
        def __init__(self, *args, **kwargs):
            self.__dict__["schema"] = schema
            d = dict(*args, **kwargs)
            dict.__init__(self, d)

        def __setitem__(self, key, value):
            dict.__setitem__(self, key, value)

        def __getattr__(self, key):
            try:
                return self.__getitem__(key)
            except KeyError:
                raise AttributeError(key)

    return dataclass_json(dataclasses.dataclass(Model))


def _check_and_covert_float(lv: Literal) -> float:
    if lv.scalar.primitive.float_value is not None:
        return lv.scalar.primitive.float_value
    elif lv.scalar.primitive.integer is not None:
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
            "none",
            None,
            _type_models.LiteralType(simple=_type_models.SimpleType.NONE),
            lambda x: None,
            lambda x: None,
        )
    )
    TypeEngine.register(ListTransformer())
    TypeEngine.register(DictTransformer())
    TypeEngine.register(TextIOTransformer())
    TypeEngine.register(BinaryIOTransformer())
    TypeEngine.register(EnumTransformer())
    TypeEngine.register(ProtobufTransformer())

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
