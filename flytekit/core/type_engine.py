from __future__ import annotations

import asyncio
import collections
import copy
import dataclasses
import datetime
import enum
import inspect
import json
import mimetypes
import os
import sys
import textwrap
import threading
import typing
from abc import ABC, abstractmethod
from collections import OrderedDict
from functools import lru_cache
from types import GenericAlias
from typing import Any, Dict, List, NamedTuple, Optional, Type, cast

import msgpack
from dataclasses_json import DataClassJsonMixin, dataclass_json
from flyteidl.core import literals_pb2
from fsspec.asyn import _run_coros_in_chunks  # pylint: disable=W0212
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from google.protobuf.json_format import MessageToDict as _MessageToDict
from google.protobuf.json_format import ParseDict as _ParseDict
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Struct
from mashumaro.codecs.json import JSONDecoder, JSONEncoder
from mashumaro.codecs.msgpack import MessagePackDecoder, MessagePackEncoder
from mashumaro.mixins.json import DataClassJSONMixin
from typing_extensions import Annotated, get_args, get_origin

from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.constants import CACHE_KEY_METADATA, FLYTE_USE_OLD_DC_FORMAT, MESSAGEPACK, SERIALIZATION_FORMAT
from flytekit.core.context_manager import FlyteContext
from flytekit.core.hash import HashMethod
from flytekit.core.type_helpers import load_type_from_tag
from flytekit.core.type_match_checking import literal_types_match
from flytekit.core.utils import load_proto_from_file, str2bool, timeit
from flytekit.exceptions import user as user_exceptions
from flytekit.interaction.string_literals import literal_map_string_repr
from flytekit.lazy_import.lazy_module import is_imported
from flytekit.loggers import logger
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.annotation import TypeAnnotation as TypeAnnotationModel
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Binary, Literal, LiteralCollection, LiteralMap, Primitive, Scalar, Union, Void
from flytekit.models.types import LiteralType, SimpleType, TypeStructure, UnionType
from flytekit.utils.asyn import loop_manager

if typing.TYPE_CHECKING:
    from flytekit.core.interface import Interface

T = typing.TypeVar("T")
DEFINITIONS = "definitions"
TITLE = "title"

_TYPE_ENGINE_COROS_BATCH_SIZE = int(os.environ.get("_F_TE_MAX_COROS", "10"))


# In Mashumaro, the default encoder uses strict_map_key=False, while the default decoder uses strict_map_key=True.
# This is relevant for cases like Dict[int, str].
# If strict_map_key=False is not used, the decoder will raise an error when trying to decode keys that are not strictly typed.｀
def _default_msgpack_decoder(data: bytes) -> Any:
    return msgpack.unpackb(data, strict_map_key=False)


class BatchSize:
    """
    This is used to annotate a FlyteDirectory when we want to download/upload the contents of the directory in batches. For example,

    @task
    def t1(directory: Annotated[FlyteDirectory, BatchSize(10)]) -> Annotated[FlyteDirectory, BatchSize(100)]:
        ...
        return FlyteDirectory(...)

    In the above example flytekit will download all files from the input `directory` in chunks of 10, i.e. first it
    downloads 10 files, loads them to memory, then writes those 10 to local disk, then it loads the next 10, so on
    and so forth. Similarly, for outputs, in this case flytekit is going to upload the resulting directory in chunks of
    100.
    """

    def __init__(self, val: int):
        self._val = val

    @property
    def val(self) -> int:
        return self._val


def get_batch_size(t: Type) -> Optional[int]:
    if is_annotated(t):
        for annotation in get_args(t)[1:]:
            if isinstance(annotation, BatchSize):
                return annotation.val
    return None


def modify_literal_uris(lit: Literal):
    """
    Modifies the literal object recursively to replace the URIs with the native paths in case they are of
    type "flyte://"
    """
    from flytekit.remote.remote_fs import FlytePathResolver

    if lit.collection:
        for l in lit.collection.literals:
            modify_literal_uris(l)
    elif lit.map:
        for k, v in lit.map.literals.items():
            modify_literal_uris(v)
    elif lit.scalar:
        if lit.scalar.blob and lit.scalar.blob.uri and lit.scalar.blob.uri.startswith(FlytePathResolver.protocol):
            lit.scalar.blob._uri = FlytePathResolver.resolve_remote_path(lit.scalar.blob.uri)
        elif lit.scalar.union:
            modify_literal_uris(lit.scalar.union.value)
        elif (
            lit.scalar.structured_dataset
            and lit.scalar.structured_dataset.uri
            and lit.scalar.structured_dataset.uri.startswith(FlytePathResolver.protocol)
        ):
            lit.scalar.structured_dataset._uri = FlytePathResolver.resolve_remote_path(
                lit.scalar.structured_dataset.uri
            )


class TypeTransformerFailedError(TypeError, AssertionError, ValueError): ...


class TypeTransformer(typing.Generic[T]):
    """
    Base transformer type that should be implemented for every python native type that can be handled by flytekit
    """

    def __init__(self, name: str, t: Type[T], enable_type_assertions: bool = True):
        self._t = t
        self._name = name
        self._type_assertions_enabled = enable_type_assertions
        self._msgpack_encoder: Dict[Type, MessagePackEncoder] = dict()
        self._msgpack_decoder: Dict[Type, MessagePackDecoder] = dict()

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
    def is_async(self) -> bool:
        return False

    @property
    def type_assertions_enabled(self) -> bool:
        """
        Indicates if the transformer wants type assertions to be enabled at the core type engine layer
        """
        return self._type_assertions_enabled

    def isinstance_generic(self, obj, generic_alias):
        origin = get_origin(generic_alias)  # list from list[int])

        if not isinstance(obj, origin):
            raise TypeTransformerFailedError(f"Value '{obj}' is not of container type {origin}")

    def assert_type(self, t: Type[T], v: T):
        if sys.version_info >= (3, 10):
            import types

            if isinstance(t, types.GenericAlias):
                return self.isinstance_generic(v, t)

        if not hasattr(t, "__origin__") and not isinstance(v, t):
            raise TypeTransformerFailedError(f"Expected value of type {t} but got '{v}' of type {type(v)}")

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
    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> Optional[T]:
        """
        Converts the given Literal to a Python Type. If the conversion cannot be done an AssertionError should be raised
        :param ctx: FlyteContext
        :param lv: The received literal Value
        :param expected_python_type: Expected native python type that should be returned
        """
        raise NotImplementedError(
            f"Conversion to python value expected type {expected_python_type} from literal not implemented"
        )

    def from_binary_idl(self, binary_idl_object: Binary, expected_python_type: Type[T]) -> Optional[T]:
        """
        This function primarily handles deserialization for untyped dicts, dataclasses, Pydantic BaseModels, and attribute access.｀

        For untyped dict, dataclass, and pydantic basemodel:
        Life Cycle (Untyped Dict as example):
            python val -> msgpack bytes -> binary literal scalar -> msgpack bytes -> python val
                          (to_literal)                             (from_binary_idl)

        For attribute access:
        Life Cycle:
            python val -> msgpack bytes -> binary literal scalar -> resolved golang value -> binary literal scalar -> msgpack bytes -> python val
                          (to_literal)                            (propeller attribute access)                       (from_binary_idl)
        """
        if binary_idl_object.tag == MESSAGEPACK:
            try:
                decoder = self._msgpack_decoder[expected_python_type]
            except KeyError:
                decoder = MessagePackDecoder(expected_python_type, pre_decoder_func=_default_msgpack_decoder)
                self._msgpack_decoder[expected_python_type] = decoder
            python_val = decoder.decode(binary_idl_object.value)

            return python_val
        else:
            raise TypeTransformerFailedError(f"Unsupported binary format `{binary_idl_object.tag}`")

    def from_generic_idl(self, generic: Struct, expected_python_type: Type[T]) -> Optional[T]:
        """
        TODO: Support all Flyte Types.
        This is for dataclass attribute access from input created from the Flyte Console.

        Note:
        - This can be removed in the future when the Flyte Console support generate Binary IDL Scalar as input.
        """
        raise NotImplementedError(f"Conversion from generic idl to python type {expected_python_type} not implemented")

    def to_html(self, ctx: FlyteContext, python_val: T, expected_python_type: Type[T]) -> str:
        """
        Converts any python val (dataframe, int, float) to a html string, and it will be wrapped in the HTML div
        """
        return str(python_val)

    def __repr__(self):
        return f"{self._name} Transforms ({self._t}) to Flyte native"

    def __str__(self):
        return str(self.__repr__())


class AsyncTypeTransformer(TypeTransformer[T]):
    def __init__(self, name: str, t: Type[T], enable_type_assertions: bool = True):
        super().__init__(name, t, enable_type_assertions)

    @property
    def is_async(self) -> bool:
        return True

    @abstractmethod
    async def async_to_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> Literal:
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
    async def async_to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> Optional[T]:
        """
        Converts the given Literal to a Python Type. If the conversion cannot be done an AssertionError should be raised
        :param ctx: FlyteContext
        :param lv: The received literal Value
        :param expected_python_type: Expected native python type that should be returned
        """
        raise NotImplementedError(
            f"Conversion to python value expected type {expected_python_type} from literal not implemented"
        )

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        synced = loop_manager.synced(self.async_to_literal)
        result = synced(ctx, python_val, python_type, expected)
        return result

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> Optional[T]:
        synced = loop_manager.synced(self.async_to_python_value)
        result = synced(ctx, lv, expected_python_type)
        return result


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
        self._type = t
        self._lt = lt
        self._to_literal_transformer = to_literal_transformer
        self._from_literal_transformer = from_literal_transformer

    @property
    def base_type(self) -> Type:
        return self._type

    def get_literal_type(self, t: Optional[Type[T]] = None) -> LiteralType:
        return LiteralType.from_flyte_idl(self._lt.to_flyte_idl())

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if type(python_val) != self._type:
            raise TypeTransformerFailedError(
                f"Expected value of type {self._type} but got '{python_val}' of type {type(python_val)}"
            )
        return self._to_literal_transformer(python_val)

    def from_binary_idl(self, binary_idl_object: Binary, expected_python_type: Type[T]) -> Optional[T]:
        if binary_idl_object.tag == MESSAGEPACK:
            if expected_python_type in [datetime.date, datetime.datetime, datetime.timedelta]:
                """
                MessagePack doesn't support datetime, date, and timedelta.
                However, mashumaro's MessagePackEncoder and MessagePackDecoder can convert them to str and vice versa.
                That's why we need to use mashumaro's MessagePackDecoder here.
                """
                try:
                    decoder = self._msgpack_decoder[expected_python_type]
                except KeyError:
                    decoder = MessagePackDecoder(expected_python_type, pre_decoder_func=_default_msgpack_decoder)
                    self._msgpack_decoder[expected_python_type] = decoder
                python_val = decoder.decode(binary_idl_object.value)
            else:
                python_val = msgpack.loads(binary_idl_object.value)
                """
                In the case below, when using Union Transformer + Simple Transformer, then `a`
                can be converted to int, bool, str and float if we use MessagePackDecoder[expected_python_type].

                Life Cycle:
                1 -> msgpack bytes -> (1, true, "1", 1.0)

                Example Code:
                @dataclass
                class DC:
                    a: Union[int, bool, str, float]
                    b: Union[int, bool, str, float]

                @task(container_image=custom_image)
                def add(a: Union[int, bool, str, float], b: Union[int, bool, str, float]) -> Union[int, bool, str, float]:
                    return a + b

                @workflow
                def wf(dc: DC) -> Union[int, bool, str, float]:
                    return add(dc.a, dc.b)

                wf(DC(1, 1))
                """
                assert type(python_val) == expected_python_type

            return python_val
        else:
            raise TypeTransformerFailedError(f"Unsupported binary format `{binary_idl_object.tag}`")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        expected_python_type = get_underlying_type(expected_python_type)

        if expected_python_type != self._type:
            raise TypeTransformerFailedError(
                f"Cannot convert to type {expected_python_type}, only {self._type} is supported"
            )

        if lv.scalar and lv.scalar.binary:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore

        try:  # todo(maximsmol): this is quite ugly and each transformer should really check their Literal
            res = self._from_literal_transformer(lv)
            if type(res) != self._type:
                raise TypeTransformerFailedError(f"Cannot convert literal {lv} to {self._type}")
            return res
        except AttributeError:
            # Assume that this is because a property on `lv` was None
            raise TypeTransformerFailedError(f"Cannot convert literal {lv} to {self._type}")

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        if literal_type.simple is not None and literal_type.simple == self._lt.simple:
            return self.python_type
        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


class RestrictedTypeError(Exception):
    pass


class RestrictedTypeTransformer(TypeTransformer[T], ABC):
    """
    Types registered with the RestrictedTypeTransformer are not allowed to be converted to and from literals. In other words,
    Restricted types are not allowed to be used as inputs or outputs of tasks and workflows.
    """

    def __init__(self, name: str, t: Type[T]):
        super().__init__(name, t)

    def get_literal_type(self, t: Optional[Type[T]] = None) -> LiteralType:
        raise RestrictedTypeError(f"Transformer for type {self.python_type} is restricted currently")

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        raise RestrictedTypeError(f"Transformer for type {self.python_type} is restricted currently")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        raise RestrictedTypeError(f"Transformer for type {self.python_type} is restricted currently")


class DataclassTransformer(TypeTransformer[object]):
    """
    The Dataclass Transformer provides a type transformer for dataclasses.

    The dataclass is converted to and from MessagePack Bytes by the mashumaro library
    and is transported between tasks using the Binary IDL representation.
    Also, the type declaration will try to extract the JSON Schema for the
    object, if possible, and pass it with the definition.

    The lifecycle of the dataclass in the Flyte type system is as follows:

    1. Serialization: The dataclass transformer converts the dataclass to MessagePack Bytes.
        (1) Handle dataclass attributes to make them serializable with mashumaro.
        (2) Use the mashumaro API to serialize the dataclass to MessagePack Bytes.
        (3) Use MessagePack Bytes to create a Flyte Literal.
        (4) Serialize the Flyte Literal to a Binary IDL Object.

    2. Deserialization: The dataclass transformer converts the MessagePack Bytes back to a dataclass.
        (1) Convert MessagePack Bytes to a dataclass using mashumaro.
        (2) Handle dataclass attributes to ensure they are of the correct types.

    For Json Schema, we use https://github.com/fuhrysteve/marshmallow-jsonschema library.

    Example

    .. code-block:: python

        @dataclass
        class Test(DataClassJsonMixin):
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

    def __init__(self) -> None:
        super().__init__("Object-Dataclass-Transformer", object)
        self._json_encoder: Dict[Type, JSONEncoder] = dict()
        self._json_decoder: Dict[Type, JSONDecoder] = dict()

    def assert_type(self, expected_type: Type[DataClassJsonMixin], v: T):
        # Skip iterating all attributes in the dataclass if the type of v already matches the expected_type
        expected_type = get_underlying_type(expected_type)
        if type(v) == expected_type or issubclass(type(v), expected_type):
            return

        # @dataclass
        # class Foo(DataClassJsonMixin):
        #     a: int = 0
        #
        # @task
        # def t1(a: Foo):
        #     ...
        #
        # In above example, the type of v may not equal to the expected_type in some cases
        # For example,
        # 1. The input of t1 is another dataclass (bar), then we should raise an error
        # 2. when using flyte remote to execute the above task, the expected_type is guess_python_type (FooSchema) by default.
        # However, FooSchema is created by flytekit and it's not equal to the user-defined dataclass (Foo).
        # Therefore, we should iterate all attributes in the dataclass and check the type of value in dataclass matches the expected_type.

        expected_fields_dict = {}

        for f in dataclasses.fields(expected_type):
            expected_fields_dict[f.name] = f.type

        if isinstance(v, dict):
            original_dict = v

            # Find the Optional keys in expected_fields_dict
            optional_keys = {k for k, t in expected_fields_dict.items() if UnionTransformer.is_optional_type(t)}

            # Remove the Optional keys from the keys of original_dict
            original_key = set(original_dict.keys()) - optional_keys
            expected_key = set(expected_fields_dict.keys()) - optional_keys

            # Check if original_key is missing any keys from expected_key
            missing_keys = expected_key - original_key
            if missing_keys:
                raise TypeTransformerFailedError(
                    f"The original fields are missing the following keys from the dataclass fields: {list(missing_keys)}"
                )

            # Check if original_key has any extra keys that are not in expected_key
            extra_keys = original_key - expected_key
            if extra_keys:
                raise TypeTransformerFailedError(
                    f"The original fields have the following extra keys that are not in dataclass fields: {list(extra_keys)}"
                )

            for k, v in original_dict.items():
                if k in expected_fields_dict:
                    if isinstance(v, dict):
                        self.assert_type(expected_fields_dict[k], v)
                    else:
                        expected_type = expected_fields_dict[k]
                        original_type = type(v)
                        if UnionTransformer.is_optional_type(expected_type):
                            expected_type = UnionTransformer.get_sub_type_in_optional(expected_type)
                        if original_type != expected_type:
                            raise TypeTransformerFailedError(
                                f"Type of Val '{original_type}' is not an instance of {expected_type}"
                            )

        else:
            for f in dataclasses.fields(type(v)):  # type: ignore
                original_type = f.type
                if f.name not in expected_fields_dict:
                    raise TypeTransformerFailedError(
                        f"Field '{f.name}' is not present in the expected dataclass fields {expected_type.__name__}"
                    )
                expected_type = expected_fields_dict[f.name]

                if UnionTransformer.is_optional_type(original_type):
                    original_type = UnionTransformer.get_sub_type_in_optional(original_type)
                if UnionTransformer.is_optional_type(expected_type):
                    expected_type = UnionTransformer.get_sub_type_in_optional(expected_type)

                val = v.__getattribute__(f.name)
                if dataclasses.is_dataclass(val):
                    self.assert_type(expected_type, val)
                elif original_type != expected_type:
                    raise TypeTransformerFailedError(
                        f"Type of Val '{original_type}' is not an instance of {expected_type}"
                    )

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Extracts the Literal type definition for a Dataclass and returns a type Struct.
        If possible also extracts the JSONSchema for the dataclass.
        """

        if is_annotated(t):
            args = get_args(t)
            for x in args[1:]:
                if isinstance(x, FlyteAnnotation):
                    raise ValueError(
                        "Flytekit does not currently have support for FlyteAnnotations applied to Dataclass."
                        f"Type {t} cannot be parsed."
                    )
            logger.info(f"These annotations will be skipped for dataclasses = {args[1:]}")
            # Drop all annotations and handle only the dataclass type passed in.
            t = args[0]

        schema = None
        try:
            # This produce JSON SCHEMA draft 2020-12
            from mashumaro.jsonschema import build_json_schema

            schema = build_json_schema(cast(DataClassJSONMixin, self._get_origin_type_in_annotation(t))).to_dict()
        except Exception as e:
            logger.error(
                f"Failed to extract schema for object {t}, error: {e}\n"
                f"Please remove `DataClassJsonMixin` and `dataclass_json` decorator from the dataclass definition"
            )

        if schema is None:
            try:
                # This produce JSON SCHEMA draft 2020-12
                from marshmallow_enum import EnumField, LoadDumpOptions

                if issubclass(t, DataClassJsonMixin):
                    s = cast(DataClassJsonMixin, self._get_origin_type_in_annotation(t)).schema()
                    for _, v in s.fields.items():
                        # marshmallow-jsonschema only supports enums loaded by name.
                        # https://github.com/fuhrysteve/marshmallow-jsonschema/blob/81eada1a0c42ff67de216923968af0a6b54e5dcb/marshmallow_jsonschema/base.py#L228
                        if isinstance(v, EnumField):
                            v.load_by = LoadDumpOptions.name
                    # check if DataClass mixin
                    from marshmallow_jsonschema import JSONSchema

                    schema = JSONSchema().dump(s)
            except Exception as e:
                # https://github.com/lovasoa/marshmallow_dataclass/issues/13
                logger.warning(
                    f"Failed to extract schema for object {t}, (will run schemaless) error: {e}"
                    f"If you have postponed annotations turned on (PEP 563) turn it off please. Postponed"
                    f"evaluation doesn't work with json dataclasses"
                )

        # Recursively construct the dataclass_type which contains the literal type of each field
        literal_type = {}

        hints = typing.get_type_hints(t)
        # Get the type of each field from dataclass
        for field in t.__dataclass_fields__.values():  # type: ignore
            try:
                name = field.name
                python_type = hints.get(name, field.type)
                literal_type[name] = TypeEngine.to_literal_type(python_type)
            except Exception as e:
                logger.warning(
                    "Field {} of type {} cannot be converted to a literal type. Error: {}".format(
                        field.name, field.type, e
                    )
                )

        # This is for attribute access in FlytePropeller.
        ts = TypeStructure(tag="", dataclass_type=literal_type)

        return _type_models.LiteralType(
            simple=_type_models.SimpleType.STRUCT,
            metadata=schema,
            structure=ts,
            annotation=TypeAnnotationModel({CACHE_KEY_METADATA: {SERIALIZATION_FORMAT: MESSAGEPACK}}),
        )

    def to_generic_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        """
        Serializes a dataclass or dictionary to a Flyte literal, handling both JSON and MessagePack formats.
        Set `FLYTE_USE_OLD_DC_FORMAT=true` to use the old JSON-based format.
        Note: This is deprecated and will be removed in the future.
        """
        if isinstance(python_val, dict):
            json_str = json.dumps(python_val)
            return Literal(scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())))

        if not dataclasses.is_dataclass(python_val):
            raise TypeTransformerFailedError(
                f"{type(python_val)} is not of type @dataclass, only Dataclasses are supported for "
                f"user defined datatypes in Flytekit"
            )

        self._make_dataclass_serializable(python_val, python_type)

        # JSON serialization using mashumaro's DataClassJSONMixin
        if isinstance(python_val, DataClassJSONMixin):
            json_str = python_val.to_json()
        else:
            try:
                encoder = self._json_encoder[python_type]
            except KeyError:
                encoder = JSONEncoder(python_type)
                self._json_encoder[python_type] = encoder

            try:
                json_str = encoder.encode(python_val)
            except NotImplementedError:
                raise NotImplementedError(
                    f"{python_type} should inherit from mashumaro.types.SerializableType"
                    f" and implement _serialize and _deserialize methods."
                )

        return Literal(scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())))  # type: ignore

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if str2bool(os.getenv(FLYTE_USE_OLD_DC_FORMAT)):
            return self.to_generic_literal(ctx, python_val, python_type, expected)

        if isinstance(python_val, dict):
            msgpack_bytes = msgpack.dumps(python_val)
            return Literal(scalar=Scalar(binary=Binary(value=msgpack_bytes, tag=MESSAGEPACK)))

        if not dataclasses.is_dataclass(python_val):
            raise TypeTransformerFailedError(
                f"{type(python_val)} is not of type @dataclass, only Dataclasses are supported for "
                f"user defined datatypes in Flytekit"
            )

        self._make_dataclass_serializable(python_val, python_type)

        # The `to_json` integrated through mashumaro's `DataClassJSONMixin` allows for more
        # functionality than JSONEncoder
        # We can't use hasattr(python_val, "to_json") here because we rely on mashumaro's API to customize the serialization behavior for Flyte types.
        if isinstance(python_val, DataClassJSONMixin):
            json_str = python_val.to_json()
            dict_obj = json.loads(json_str)
            msgpack_bytes = msgpack.dumps(dict_obj)
        else:
            # The function looks up or creates a MessagePackEncoder specifically designed for the object's type.
            # This encoder is then used to convert a data class into MessagePack Bytes.
            try:
                encoder = self._msgpack_encoder[python_type]
            except KeyError:
                encoder = MessagePackEncoder(python_type)
                self._msgpack_encoder[python_type] = encoder

            try:
                msgpack_bytes = encoder.encode(python_val)
            except NotImplementedError:
                # you can refer FlyteFile, FlyteDirectory and StructuredDataset to see how flyte types can be implemented.
                raise NotImplementedError(
                    f"{python_type} should inherit from mashumaro.types.SerializableType"
                    f" and implement _serialize and _deserialize methods."
                )

        return Literal(scalar=Scalar(binary=Binary(value=msgpack_bytes, tag=MESSAGEPACK)))

    def _get_origin_type_in_annotation(self, python_type: Type[T]) -> Type[T]:
        # dataclass will try to hash python type when calling dataclass.schema(), but some types in the annotation is
        # not hashable, such as Annotated[StructuredDataset, kwtypes(...)]. Therefore, we should just extract the origin
        # type from annotated.
        if get_origin(python_type) is list:
            return typing.List[self._get_origin_type_in_annotation(get_args(python_type)[0])]  # type: ignore
        elif get_origin(python_type) is dict:
            return typing.Dict[  # type: ignore
                self._get_origin_type_in_annotation(get_args(python_type)[0]),
                self._get_origin_type_in_annotation(get_args(python_type)[1]),
            ]
        elif is_annotated(python_type):
            return get_args(python_type)[0]
        elif dataclasses.is_dataclass(python_type):
            for field in dataclasses.fields(copy.deepcopy(python_type)):
                field.type = self._get_origin_type_in_annotation(field.type)
        return python_type

    def _make_dataclass_serializable(self, python_val: T, python_type: Type[T]) -> typing.Any:
        """
        If any field inside the dataclass is flyte type, we should use flyte type transformer for that field.
        Since Flyte types are already serializable, this function is intended for using strings instead of directly creating Flyte files and directories in the dataclass.
        An example shows the lifecycle:

        @dataclass
        class DC:
            ff: FlyteFile

        @task
        def t1() -> DC:
            return DC(ff="s3://path")

        Lifecycle: DC(ff="s3://path") -> to_literal() -> DC(ff=FlyteFile(path="s3://path")) -> msgpack -> to_python_value() -> DC(ff=FlyteFile(path="s3://path"))
        """
        from flytekit.types.directory import FlyteDirectory
        from flytekit.types.file import FlyteFile
        from flytekit.types.structured import StructuredDataset

        # Handle Optional and Union Types
        if _is_union_type(python_type):

            def get_expected_type(python_val: T, types: tuple) -> Type[T | None]:
                if len(set(types) & {FlyteFile, FlyteDirectory, StructuredDataset}) > 1:
                    raise ValueError(
                        "Cannot have more than one Flyte type in the Union when attempting to use the string shortcut. Please specify the full object (e.g. FlyteFile(...)) instead of just passing a string."
                    )

                for t in types:
                    try:
                        trans = TypeEngine.get_transformer(t)  # type: ignore
                        if trans:
                            trans.assert_type(t, python_val)
                            return t
                    except Exception:
                        continue
                return type(None)

            # Get the expected type in the Union type
            expected_type = type(None)
            if python_val is not None:
                expected_type = get_expected_type(python_val, get_args(python_type))  # type: ignore

            return self._make_dataclass_serializable(python_val, expected_type)

        if hasattr(python_type, "__origin__") and get_origin(python_type) is list:
            if python_val is None:
                return None
            return [self._make_dataclass_serializable(v, get_args(python_type)[0]) for v in cast(list, python_val)]

        if hasattr(python_type, "__origin__") and get_origin(python_type) is dict:
            if python_val is None:
                return None
            return {
                k: self._make_dataclass_serializable(v, get_args(python_type)[1])
                for k, v in cast(dict, python_val).items()
            }

        if not dataclasses.is_dataclass(python_type):
            return python_val

        # Transform str to FlyteFile or FlyteDirectory so that mashumaro can serialize the path.
        # For example, if you return s3://my-s3-bucket/a/example.txt,
        # flytekit will convert the path to FlyteFile(path="s3://my-s3-bucket/a/example.txt")
        # so that mashumaro can use the serialize method implemented in FlyteFile.
        if inspect.isclass(python_type) and (
            issubclass(python_type, FlyteFile) or issubclass(python_type, FlyteDirectory)
        ):
            if type(python_val) == str:
                logger.warning(
                    f"Converting string '{python_val}' to {python_type.__name__}.\n"
                    f"Directly using a string instead of {python_type.__name__} is not recommended.\n"
                    f"flytekit will not support it in the future."
                )
                return python_type(python_val)
            return python_val

        dataclass_attributes = typing.get_type_hints(python_type)
        for n, t in dataclass_attributes.items():
            val = python_val.__getattribute__(n)
            object.__setattr__(python_val, n, self._make_dataclass_serializable(val, t))
        return python_val

    def _fix_val_int(self, t: typing.Type, val: typing.Any) -> typing.Any:
        if val is None:
            return val

        if UnionTransformer.is_optional_type(t):
            # Handle optional type. e.g. Optional[int], Optional[dataclass]
            # Marshmallow doesn't support union type, so the type here is always an optional type.
            # https://github.com/marshmallow-code/marshmallow/issues/1191#issuecomment-480831796
            # Note: Union[None, int] is also an optional type, but Marshmallow does not support it.
            t = get_args(t)[0]

        if t == int:
            return int(val)

        if isinstance(val, list):
            # Handle nested List. e.g. [[1, 2], [3, 4]]
            return list(map(lambda x: self._fix_val_int(ListTransformer.get_sub_type(t), x), val))

        if isinstance(val, dict):
            ktype, vtype = DictTransformer.extract_types(t)
            # Handle nested Dict. e.g. {1: {2: 3}, 4: {5: 6}})
            return {
                self._fix_val_int(cast(type, ktype), k): self._fix_val_int(cast(type, vtype), v) for k, v in val.items()
            }

        if dataclasses.is_dataclass(t):
            return self._fix_dataclass_int(t, val)  # type: ignore

        return val

    def _fix_dataclass_int(self, dc_type: Type[dataclasses.dataclass], dc: typing.Any) -> typing.Any:  # type: ignore
        """
        This is a performance penalty to convert to the right types, but this is expected by the user and hence
        needs to be done
        """
        # NOTE: Protobuf Struct does not support explicit int types, int types are upconverted to a double value
        # https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.Value
        # Thus we will have to walk the given dataclass and typecast values to int, where expected.
        for f in dataclasses.fields(dc_type):
            val = getattr(dc, f.name)
            object.__setattr__(dc, f.name, self._fix_val_int(f.type, val))

        return dc

    def from_binary_idl(self, binary_idl_object: Binary, expected_python_type: Type[T]) -> T:
        if binary_idl_object.tag == MESSAGEPACK:
            if issubclass(expected_python_type, DataClassJSONMixin):
                dict_obj = msgpack.loads(binary_idl_object.value, strict_map_key=False)
                json_str = json.dumps(dict_obj)
                dc = expected_python_type.from_json(json_str)  # type: ignore
            else:
                try:
                    decoder = self._msgpack_decoder[expected_python_type]
                except KeyError:
                    decoder = MessagePackDecoder(expected_python_type, pre_decoder_func=_default_msgpack_decoder)
                    self._msgpack_decoder[expected_python_type] = decoder
                dc = decoder.decode(binary_idl_object.value)

            return dc
        else:
            raise TypeTransformerFailedError(f"Unsupported binary format: `{binary_idl_object.tag}`")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if not dataclasses.is_dataclass(expected_python_type):
            raise TypeTransformerFailedError(
                f"{expected_python_type} is not of type @dataclass, only Dataclasses are supported for "
                "user defined datatypes in Flytekit"
            )

        if lv.scalar and lv.scalar.binary:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore

        json_str = _json_format.MessageToJson(lv.scalar.generic)

        # The `from_json` function is provided from mashumaro's `DataClassJSONMixin`.
        # It deserializes a JSON string into a data class, and supports additional functionality over JSONDecoder
        # We can't use hasattr(expected_python_type, "from_json") here because we rely on mashumaro's API to customize the deserialization behavior for Flyte types.
        if issubclass(expected_python_type, DataClassJSONMixin):
            dc = expected_python_type.from_json(json_str)  # type: ignore
        else:
            # The function looks up or creates a JSONDecoder specifically designed for the object's type.
            # This decoder is then used to convert a JSON string into a data class.
            try:
                decoder = self._json_decoder[expected_python_type]
            except KeyError:
                decoder = JSONDecoder(expected_python_type)
                self._json_decoder[expected_python_type] = decoder

            dc = decoder.decode(json_str)

        return self._fix_dataclass_int(expected_python_type, dc)

    # This ensures that calls with the same literal type returns the same dataclass. For example, `pyflyte run``
    # command needs to call guess_python_type to get the TypeEngine-derived dataclass. Without caching here, separate
    # calls to guess_python_type would result in a logically equivalent (but new) dataclass, which
    # TypeEngine.assert_type would not be happy about.
    @lru_cache(typed=True)
    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:  # type: ignore
        if literal_type.simple == SimpleType.STRUCT:
            if literal_type.metadata is not None:
                if DEFINITIONS in literal_type.metadata:
                    schema_name = literal_type.metadata["$ref"].split("/")[-1]
                    return convert_marshmallow_json_schema_to_python_class(
                        literal_type.metadata[DEFINITIONS], schema_name
                    )
                elif TITLE in literal_type.metadata:
                    schema_name = literal_type.metadata[TITLE]
                    return convert_mashumaro_json_schema_to_python_class(literal_type.metadata, schema_name)
        raise ValueError(f"Dataclass transformer cannot reverse {literal_type}")


class ProtobufTransformer(TypeTransformer[Message]):
    PB_FIELD_KEY = "pb_type"

    def __init__(self):
        super().__init__("Protobuf-Transformer", Message)

    @staticmethod
    def tag(expected_python_type: Type[T]) -> str:
        return f"{expected_python_type.__module__}.{expected_python_type.__name__}"

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        return LiteralType(simple=SimpleType.STRUCT, metadata={ProtobufTransformer.PB_FIELD_KEY: self.tag(t)})

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        """
        Convert the protobuf struct to literal.

        This conversion supports two types of python_val:
        1. google.protobuf.struct_pb2.Struct: A dictionary-like message
        2. google.protobuf.struct_pb2.ListValue: An ordered collection of values

        For details, please refer to the following issue:
        https://github.com/flyteorg/flyte/issues/5959

        Because the remote handling works without errors, we implement conversion with the logic as below:
        https://github.com/flyteorg/flyte/blob/a87585ab7cbb6a047c76d994b3f127c4210070fd/flytepropeller/pkg/controller/nodes/attr_path_resolver.go#L72-L106
        """
        try:
            if type(python_val) == _struct.ListValue:
                literals = []
                for v in python_val:
                    literal_type = TypeEngine.to_literal_type(type(v))
                    # Recursively convert python native values to literals
                    literal = TypeEngine.to_literal(ctx, v, type(v), literal_type)
                    literals.append(literal)
                return Literal(collection=LiteralCollection(literals=literals))
            else:
                struct = Struct()
                struct.update(_MessageToDict(cast(Message, python_val)))
                return Literal(scalar=Scalar(generic=struct))
        except Exception:
            raise TypeTransformerFailedError("Failed to convert to generic protobuf struct")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if not (lv and lv.scalar and lv.scalar.generic is not None):
            raise TypeTransformerFailedError("Can only convert a generic literal to a Protobuf")

        pb_obj = expected_python_type()
        dictionary = _MessageToDict(lv.scalar.generic)
        pb_obj = _ParseDict(dictionary, pb_obj)  # type: ignore
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


class EnumTransformer(TypeTransformer[enum.Enum]):
    """
    Enables converting a python type enum.Enum to LiteralType.EnumType
    """

    def __init__(self):
        super().__init__(name="DefaultEnumTransformer", t=enum.Enum)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        if is_annotated(t):
            raise ValueError(
                f"Flytekit does not currently have support \
                    for FlyteAnnotations applied to enums. {t} cannot be \
                    parsed."
            )

        values = [v.value for v in t]  # type: ignore
        if not isinstance(values[0], str):
            raise TypeTransformerFailedError("Only EnumTypes with value of string are supported")
        return LiteralType(enum_type=_core_types.EnumType(values=values))

    def to_literal(
        self, ctx: FlyteContext, python_val: enum.Enum, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        if type(python_val).__class__ != enum.EnumMeta:
            raise TypeTransformerFailedError("Expected an enum")
        if type(python_val.value) != str:
            raise TypeTransformerFailedError("Only string-valued enums are supported")

        return Literal(scalar=Scalar(primitive=Primitive(string_value=python_val.value)))  # type: ignore

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if lv.scalar and lv.scalar.binary:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore
        return expected_python_type(lv.scalar.primitive.string_value)  # type: ignore

    def guess_python_type(self, literal_type: LiteralType) -> Type[enum.Enum]:
        if literal_type.enum_type:
            return enum.Enum("DynamicEnum", {f"{i}": i for i in literal_type.enum_type.values})  # type: ignore
        raise ValueError(f"Enum transformer cannot reverse {literal_type}")

    def assert_type(self, t: Type[enum.Enum], v: T):
        if sys.version_info < (3, 10):
            if not isinstance(v, enum.Enum):
                raise TypeTransformerFailedError(f"Value {v} needs to be an Enum in 3.9")
            if not isinstance(v, t):
                raise TypeTransformerFailedError(f"Value {v} is not in Enum {t}")
            return

        val = v.value if isinstance(v, enum.Enum) else v
        if val not in [t_item.value for t_item in t]:
            raise TypeTransformerFailedError(f"Value {v} is not in Enum {t}")


def generate_attribute_list_from_dataclass_json_mixin(schema: dict, schema_name: typing.Any):
    attribute_list: typing.List[typing.Tuple[Any, Any]] = []
    for property_key, property_val in schema["properties"].items():
        property_type = ""
        if property_val.get("anyOf"):
            property_type = property_val["anyOf"][0]["type"]
        elif property_val.get("enum"):
            property_type = "enum"
        else:
            property_type = property_val["type"]
        # Handle list
        if property_type == "array":
            attribute_list.append((property_key, typing.List[_get_element_type(property_val["items"])]))  # type: ignore
        # Handle dataclass and dict
        elif property_type == "object":
            if property_val.get("anyOf"):
                # For optional with dataclass
                sub_schemea = property_val["anyOf"][0]
                sub_schemea_name = sub_schemea["title"]
                attribute_list.append(
                    (
                        property_key,
                        typing.cast(
                            GenericAlias, convert_mashumaro_json_schema_to_python_class(sub_schemea, sub_schemea_name)
                        ),
                    )
                )
            elif property_val.get("additionalProperties"):
                # For typing.Dict type
                elem_type = _get_element_type(property_val["additionalProperties"])
                attribute_list.append((property_key, typing.Dict[str, elem_type]))  # type: ignore
            elif property_val.get("title"):
                # For nested dataclass
                sub_schemea_name = property_val["title"]
                attribute_list.append(
                    (
                        property_key,
                        typing.cast(
                            GenericAlias, convert_mashumaro_json_schema_to_python_class(property_val, sub_schemea_name)
                        ),
                    )
                )
            else:
                # For untyped dict
                attribute_list.append((property_key, dict))  # type: ignore
        elif property_type == "enum":
            attribute_list.append([property_key, str])  # type: ignore
        # Handle int, float, bool or str
        else:
            attribute_list.append([property_key, _get_element_type(property_val)])  # type: ignore
    return attribute_list


class TypeEngine(typing.Generic[T]):
    """
    Core Extensible TypeEngine of Flytekit. This should be used to extend the capabilities of FlyteKits type system.
    Users can implement their own TypeTransformers and register them with the TypeEngine. This will allow special handling
    of user objects
    """

    _REGISTRY: typing.Dict[type, TypeTransformer[T]] = {}
    _RESTRICTED_TYPES: typing.List[type] = []
    _DATACLASS_TRANSFORMER: TypeTransformer = DataclassTransformer()  # type: ignore
    _ENUM_TRANSFORMER: TypeTransformer = EnumTransformer()  # type: ignore
    lazy_import_lock = threading.Lock()

    @classmethod
    def register(
        cls,
        transformer: TypeTransformer,
        additional_types: Optional[typing.List[Type]] = None,
    ):
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
    def register_restricted_type(
        cls,
        name: str,
        type: Type[T],
    ):
        cls._RESTRICTED_TYPES.append(type)
        cls.register(RestrictedTypeTransformer(name, type))  # type: ignore

    @classmethod
    def register_additional_type(cls, transformer: TypeTransformer[T], additional_type: Type[T], override=False):
        if additional_type not in cls._REGISTRY or override:
            cls._REGISTRY[additional_type] = transformer

    @classmethod
    def _get_transformer(cls, python_type: Type) -> Optional[TypeTransformer[T]]:
        cls.lazy_import_transformers()
        if is_annotated(python_type):
            args = get_args(python_type)
            for annotation in args:
                if isinstance(annotation, TypeTransformer):
                    return annotation
            return cls.get_transformer(args[0])

        if inspect.isclass(python_type) and issubclass(python_type, enum.Enum):
            # Special case: prevent that for a type `FooEnum(str, Enum)`, the str transformer is used.
            return cls._ENUM_TRANSFORMER

        if hasattr(python_type, "__origin__"):
            # If the type is a generic type, we should check the origin type. But consider the case like Iterator[JSON]
            # or List[int] has been specifically registered; we should check for the entire type.
            # The challenge is for StructuredDataset, example List[StructuredDataset] the column names is an OrderedDict
            # are not hashable, thus looking up this type is not possible.
            # In such as case, we will have to skip the "type" lookup and use the origin type only
            try:
                if python_type in cls._REGISTRY:
                    return cls._REGISTRY[python_type]
            except TypeError:
                pass
            if python_type.__origin__ in cls._REGISTRY:
                return cls._REGISTRY[python_type.__origin__]

        # Handling UnionType specially - PEP 604
        if sys.version_info >= (3, 10):
            import types

            if isinstance(python_type, types.UnionType):
                return cls._REGISTRY[types.UnionType]

        if python_type in cls._REGISTRY:
            return cls._REGISTRY[python_type]

        return None

    @classmethod
    def get_transformer(cls, python_type: Type) -> TypeTransformer[T]:
        """
        Implements a recursive search for the transformer.
        """
        v = cls._get_transformer(python_type)
        if v is not None:
            return v

        if hasattr(python_type, "__mro__"):
            class_tree = inspect.getmro(python_type)
            for t in class_tree:
                v = cls._get_transformer(t)
                if v is not None:
                    return v

        # flytekit's dataclass type transformer is left for last to give users a chance to register a type transformer
        # to handle dataclass-like objects as part of the mro evaluation.
        #
        # N.B.: keep in mind that there are no compatibility guarantees between these user-defined dataclass transformers
        # and the flytekit one. This incompatibility is *not* a new behavior introduced by the recent type engine
        # refactor (https://github.com/flyteorg/flytekit/pull/2815), but it is worth calling out explicitly as a known
        # limitation nonetheless.
        if dataclasses.is_dataclass(python_type):
            return cls._DATACLASS_TRANSFORMER

        display_pickle_warning(str(python_type))
        from flytekit.types.pickle.pickle import FlytePickleTransformer

        return FlytePickleTransformer()

    @classmethod
    def lazy_import_transformers(cls):
        """
        Only load the transformers if needed.
        """
        with cls.lazy_import_lock:
            # Avoid a race condition where concurrent threads may exit lazy_import_transformers before the transformers
            # have been imported. This could be implemented without a lock if you assume python assignments are atomic
            # and re-registering transformers is acceptable, but I decided to play it safe.
            from flytekit.types.structured import lazy_import_structured_dataset_handler

            if is_imported("tensorflow"):
                from flytekit.extras import tensorflow  # noqa: F401
            if is_imported("torch"):
                from flytekit.extras import pytorch  # noqa: F401
            if is_imported("sklearn"):
                from flytekit.extras import sklearn  # noqa: F401
            if is_imported("pydantic"):
                from flytekit.extras import pydantic_transformer  # noqa: F401
            if is_imported("pandas"):
                try:
                    from flytekit.types.schema.types_pandas import PandasSchemaReader, PandasSchemaWriter  # noqa: F401
                except ValueError:
                    logger.debug("Transformer for pandas is already registered.")
            if is_imported("numpy"):
                from flytekit.types import numpy  # noqa: F401
            if is_imported("PIL"):
                from flytekit.types.file import image  # noqa: F401
            lazy_import_structured_dataset_handler()

    @classmethod
    def to_literal_type(cls, python_type: Type[T]) -> LiteralType:
        """
        Converts a python type into a flyte specific ``LiteralType``
        """
        transformer = cls.get_transformer(python_type)
        res = transformer.get_literal_type(python_type)
        data = None
        if is_annotated(python_type):
            for x in get_args(python_type)[1:]:
                if not isinstance(x, FlyteAnnotation):
                    continue
                if data is not None:
                    raise ValueError(
                        f"More than one FlyteAnnotation used within {python_type} typehint. Flytekit requires a max of one."
                    )
                data = x.data
        if data is not None:
            # Double-check that `data` does not contain a key called `cache-key-metadata`
            if CACHE_KEY_METADATA in data:
                raise AssertionError(f"FlyteAnnotation cannot contain `{CACHE_KEY_METADATA}`.")
            idl_type_annotation = TypeAnnotationModel(annotations=data)
            if res.annotation:
                idl_type_annotation = TypeAnnotationModel.merge_annotations(idl_type_annotation, res.annotation)
            res = LiteralType.from_flyte_idl(res.to_flyte_idl())
            res._annotation = idl_type_annotation
        return res

    @classmethod
    def to_literal_checks(cls, python_val: typing.Any, python_type: Type[T], expected: LiteralType):
        from flytekit.core.promise import VoidPromise

        if isinstance(python_val, VoidPromise):
            raise AssertionError(
                f"Outputs of a non-output producing task {python_val.task_name} cannot be passed to another task."
            )
        if isinstance(python_val, tuple):
            raise AssertionError(
                "Tuples are not a supported type for individual values in Flyte - got a tuple -"
                f" {python_val}. If using named tuple in an inner task, please, de-reference the"
                "actual attribute that you want to use. For example, in NamedTuple('OP', x=int) then"
                "return v.x, instead of v, even if this has a single element"
            )
        if (
            (python_val is None and python_type != type(None))
            and expected
            and expected.union_type is None
            and python_type is not Any
        ):
            raise TypeTransformerFailedError(f"Python value cannot be None, expected {python_type}/{expected}")

    @classmethod
    def calculate_hash(cls, python_val: typing.Any, python_type: Type[T]) -> Optional[str]:
        # In case the value is an annotated type we inspect the annotations and look for hash-related annotations.
        hsh = None
        if is_annotated(python_type):
            # We are now dealing with one of two cases:
            # 1. The annotated type is a `HashMethod`, which indicates that we should produce the hash using
            #    the method indicated in the annotation.
            # 2. The annotated type is being used for a different purpose other than calculating hash values,
            #    in which case we should just continue.
            for annotation in get_args(python_type)[1:]:
                if not isinstance(annotation, HashMethod):
                    continue
                hsh = annotation.calculate(python_val)
                break
        return hsh

    @classmethod
    def to_literal(
        cls, ctx: FlyteContext, python_val: typing.Any, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        """
        The current dance is because we are allowing users to call from an async function, this synchronous
        to_literal function, and allowing this to_literal function, to then invoke yet another async function,
        namely an async transformer.
        """
        from flytekit.core.promise import Promise

        cls.to_literal_checks(python_val, python_type, expected)
        if isinstance(python_val, Promise):
            # In the example above, this handles the "in2=a" type of argument
            return python_val.val

        transformer = cls.get_transformer(python_type)

        if transformer.type_assertions_enabled:
            transformer.assert_type(python_type, python_val)

        if isinstance(transformer, AsyncTypeTransformer):
            synced = loop_manager.synced(transformer.async_to_literal)
            lv = synced(ctx, python_val, python_type, expected)
        else:
            lv = transformer.to_literal(ctx, python_val, python_type, expected)

        modify_literal_uris(lv)
        lv.hash = cls.calculate_hash(python_val, python_type)
        return lv

    @classmethod
    async def async_to_literal(
        cls, ctx: FlyteContext, python_val: typing.Any, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        """
        Converts a python value of a given type and expected ``LiteralType`` into a resolved ``Literal`` value.
        """
        from flytekit.core.promise import Promise

        cls.to_literal_checks(python_val, python_type, expected)

        if isinstance(python_val, Promise):
            # In the example above, this handles the "in2=a" type of argument
            return python_val.val

        transformer = cls.get_transformer(python_type)
        if transformer.type_assertions_enabled:
            transformer.assert_type(python_type, python_val)

        if isinstance(transformer, AsyncTypeTransformer):
            lv = await transformer.async_to_literal(ctx, python_val, python_type, expected)
        else:
            lv = transformer.to_literal(ctx, python_val, python_type, expected)

        modify_literal_uris(lv)
        lv.hash = cls.calculate_hash(python_val, python_type)

        return lv

    @classmethod
    async def unwrap_offloaded_literal(cls, ctx: FlyteContext, lv: Literal) -> Literal:
        if not lv.offloaded_metadata:
            return lv

        literal_local_file = ctx.file_access.get_random_local_path()
        assert lv.offloaded_metadata.uri, "missing offloaded uri"
        ctx.file_access.download(lv.offloaded_metadata.uri, literal_local_file)
        input_proto = load_proto_from_file(literals_pb2.Literal, literal_local_file)
        return Literal.from_flyte_idl(input_proto)

    @classmethod
    def to_python_value(cls, ctx: FlyteContext, lv: Literal, expected_python_type: Type) -> typing.Any:
        """
        Converts a Literal value with an expected python type into a python value.
        """
        # Initiate the process of loading the offloaded literal if offloaded_metadata is set
        if lv.offloaded_metadata:
            synced = loop_manager.synced(cls.unwrap_offloaded_literal)
            lv = synced(ctx, lv)
        transformer = cls.get_transformer(expected_python_type)

        if isinstance(transformer, AsyncTypeTransformer):
            synced = loop_manager.synced(transformer.async_to_python_value)
            return synced(ctx, lv, expected_python_type)
        else:
            res = transformer.to_python_value(ctx, lv, expected_python_type)
            return res

    @classmethod
    async def async_to_python_value(cls, ctx: FlyteContext, lv: Literal, expected_python_type: Type) -> typing.Any:
        if lv.offloaded_metadata:
            lv = await cls.unwrap_offloaded_literal(ctx, lv)
        transformer = cls.get_transformer(expected_python_type)
        if isinstance(transformer, AsyncTypeTransformer):
            pv = await transformer.async_to_python_value(ctx, lv, expected_python_type)
        else:
            pv = transformer.to_python_value(ctx, lv, expected_python_type)
        return pv

    @classmethod
    def to_html(cls, ctx: FlyteContext, python_val: typing.Any, expected_python_type: Type[typing.Any]) -> str:
        transformer = cls.get_transformer(expected_python_type)
        if is_annotated(expected_python_type):
            expected_python_type, *annotate_args = get_args(expected_python_type)
            from flytekit.deck.renderer import Renderable

            for arg in annotate_args:
                if isinstance(arg, Renderable):
                    return arg.to_html(python_val)
        return transformer.to_html(ctx, python_val, expected_python_type)

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
    @timeit("Translate literal to python value")
    def literal_map_to_kwargs(
        cls,
        ctx: FlyteContext,
        lm: LiteralMap,
        python_types: typing.Optional[typing.Dict[str, type]] = None,
        literal_types: typing.Optional[typing.Dict[str, _interface_models.Variable]] = None,
    ) -> typing.Dict[str, typing.Any]:
        synced = loop_manager.synced(cls._literal_map_to_kwargs)
        return synced(ctx, lm, python_types, literal_types)

    @classmethod
    @timeit("AsyncTranslate literal to python value")
    async def _literal_map_to_kwargs(
        cls,
        ctx: FlyteContext,
        lm: LiteralMap,
        python_types: typing.Optional[typing.Dict[str, type]] = None,
        literal_types: typing.Optional[typing.Dict[str, _interface_models.Variable]] = None,
    ) -> typing.Dict[str, typing.Any]:
        """
        Given a ``LiteralMap`` (usually an input into a task - intermediate), convert to kwargs for the task
        """
        if python_types is None and literal_types is None:
            raise ValueError("At least one of python_types or literal_types must be provided")

        if literal_types:
            python_interface_inputs: dict[str, Type[T]] = {
                name: TypeEngine.guess_python_type(lt.type) for name, lt in literal_types.items()
            }
        else:
            python_interface_inputs = python_types  # type: ignore

        if not python_interface_inputs or len(python_interface_inputs) == 0:
            return {}

        if len(lm.literals) > len(python_interface_inputs):
            raise ValueError(
                f"Received more input values {len(lm.literals)}"
                f" than allowed by the input spec {len(python_interface_inputs)}"
            )
        kwargs = {}
        try:
            for i, k in enumerate(lm.literals):
                kwargs[k] = asyncio.create_task(
                    TypeEngine.async_to_python_value(ctx, lm.literals[k], python_interface_inputs[k])
                )
            await asyncio.gather(*kwargs.values())
        except Exception as e:
            raise TypeTransformerFailedError(
                f"Error converting input '{k}' at position {i}:\n"
                f"Literal value: {lm.literals[k]}\n"
                f"Literal type: {literal_types}\n"
                f"Expected Python type: {python_interface_inputs[k]}\n"
                f"Exception: {e}"
            )

        kwargs = {k: v.result() for k, v in kwargs.items() if v is not None}
        return kwargs

    @classmethod
    def dict_to_literal_map(
        cls,
        ctx: FlyteContext,
        d: typing.Dict[str, typing.Any],
        type_hints: Optional[typing.Dict[str, type]] = None,
    ) -> LiteralMap:
        synced = loop_manager.synced(cls._dict_to_literal_map)
        return synced(ctx, d, type_hints)

    @classmethod
    async def _dict_to_literal_map(
        cls,
        ctx: FlyteContext,
        d: typing.Dict[str, typing.Any],
        type_hints: Optional[typing.Dict[str, type]] = None,
    ) -> LiteralMap:
        """
        Given a dictionary mapping string keys to python values and a dictionary containing guessed types for such string keys,
        convert to a LiteralMap.
        """
        type_hints = type_hints or {}
        literal_map = {}
        for k, v in d.items():
            # The guessed type takes precedence over the type returned by the python runtime. This is needed
            # to account for the type erasure that happens in the case of built-in collection containers, such as
            # `list` and `dict`.
            python_type = type_hints.get(k, type(v))
            literal_map[k] = asyncio.create_task(
                TypeEngine.async_to_literal(
                    ctx=ctx,
                    python_val=v,
                    python_type=python_type,
                    expected=TypeEngine.to_literal_type(python_type),
                )
            )
        await asyncio.gather(*literal_map.values(), return_exceptions=True)
        for idx, (k, v) in enumerate(literal_map.items()):
            if literal_map[k].exception() is not None:
                python_type = type_hints.get(k, type(d[k]))
                e: BaseException = literal_map[k].exception()  # type: ignore
                if isinstance(e, TypeError):
                    raise user_exceptions.FlyteTypeException(type(v), python_type, received_value=v)
                else:
                    raise e
            literal_map[k] = v.result()

        return LiteralMap(literal_map)

    @classmethod
    async def dict_to_literal_map_pb(
        cls,
        ctx: FlyteContext,
        d: typing.Dict[str, typing.Any],
        type_hints: Optional[typing.Dict[str, type]] = None,
    ) -> Optional[literals_pb2.LiteralMap]:
        literal_map = await cls._dict_to_literal_map(ctx, d, type_hints)
        return literal_map.to_flyte_idl()

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
    def guess_python_type(cls, flyte_type: LiteralType) -> Type[T]:
        """
        Transforms a flyte-specific ``LiteralType`` to a regular python value.
        """
        for _, transformer in cls._REGISTRY.items():
            try:
                return transformer.guess_python_type(flyte_type)
            except ValueError:
                logger.debug(f"Skipping transformer {transformer.name} for {flyte_type}")

        # Because the dataclass transformer is handled explicitly in the get_transformer code, we have to handle it
        # separately here too.
        try:
            return cls._DATACLASS_TRANSFORMER.guess_python_type(literal_type=flyte_type)
        except ValueError:
            logger.debug(f"Skipping transformer {cls._DATACLASS_TRANSFORMER.name} for {flyte_type}")
        raise ValueError(f"No transformers could reverse Flyte literal type {flyte_type}")


class ListTransformer(AsyncTypeTransformer[T]):
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
        if (sub_type := ListTransformer.get_sub_type_or_none(t)) is not None:
            return sub_type

        raise ValueError("Only generic univariate typing.List[T] type is supported.")

    @staticmethod
    def get_sub_type_or_none(t: Type[T]) -> Optional[Type[T]]:
        """
        Return the generic Type T of the List, or None if the generic type cannot be inferred
        """
        if hasattr(t, "__origin__"):
            # Handle annotation on list generic, eg:
            # Annotated[typing.List[int], 'foo']
            if is_annotated(t):
                return ListTransformer.get_sub_type(get_args(t)[0])

            if getattr(t, "__origin__") is list and hasattr(t, "__args__"):
                return getattr(t, "__args__")[0]

        return None

    def get_literal_type(self, t: Type[T]) -> Optional[LiteralType]:
        """
        Only univariate Lists are supported in Flyte
        """
        try:
            sub_type = TypeEngine.to_literal_type(self.get_sub_type(t))
            return _type_models.LiteralType(collection_type=sub_type)
        except Exception as e:
            raise ValueError(f"Type of Generic List type is not supported, {e}")

    async def async_to_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        if type(python_val) != list:
            raise TypeTransformerFailedError("Expected a list")

        t = self.get_sub_type(python_type)
        lit_list = [TypeEngine.async_to_literal(ctx, x, t, expected.collection_type) for x in python_val]

        lit_list = await _run_coros_in_chunks(lit_list, batch_size=_TYPE_ENGINE_COROS_BATCH_SIZE)

        return Literal(collection=LiteralCollection(literals=lit_list))

    async def async_to_python_value(  # type: ignore
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]
    ) -> typing.Optional[typing.List[T]]:
        if lv and lv.scalar and lv.scalar.binary is not None:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore

        try:
            lits = lv.collection.literals
        except AttributeError:
            raise TypeTransformerFailedError(
                (
                    f"The expected python type is '{expected_python_type}' but the received Flyte literal value "
                    f"is not a collection (Flyte's representation of Python lists)."
                )
            )

        st = self.get_sub_type(expected_python_type)
        result = [TypeEngine.async_to_python_value(ctx, x, st) for x in lits]
        result = await _run_coros_in_chunks(result, batch_size=_TYPE_ENGINE_COROS_BATCH_SIZE)
        return result  # type: ignore  # should be a list, thinks its a tuple

    def guess_python_type(self, literal_type: LiteralType) -> list:  # type: ignore
        if literal_type.collection_type:
            ct: Type = TypeEngine.guess_python_type(literal_type.collection_type)
            return typing.List[ct]  # type: ignore
        raise ValueError(f"List transformer cannot reverse {literal_type}")


@lru_cache
def display_pickle_warning(python_type: str):
    # This is a warning that is only displayed once per python type
    logger.warning(
        f"Unsupported Type {python_type} found, Flyte will default to use PickleFile as the transport. "
        f"Pickle can only be used to send objects between the exact same version of Python, "
        f"and we strongly recommend to use python type that flyte support."
    )


def _add_tag_to_type(x: LiteralType, tag: str) -> LiteralType:
    x._structure = TypeStructure(tag=tag)
    return x


def _type_essence(x: LiteralType) -> LiteralType:
    if x.metadata is not None or x.structure is not None or x.annotation is not None:
        x = LiteralType.from_flyte_idl(x.to_flyte_idl())
        x._metadata = None
        x._structure = None
        x._annotation = None

    return x


def _are_types_castable(upstream: LiteralType, downstream: LiteralType) -> bool:
    if upstream.collection_type is not None:
        if downstream.collection_type is None:
            return False

        return _are_types_castable(upstream.collection_type, downstream.collection_type)

    if upstream.map_value_type is not None:
        if downstream.map_value_type is None:
            return False

        return _are_types_castable(upstream.map_value_type, downstream.map_value_type)

    # TODO: Structured dataset type matching requires that downstream structured datasets
    # are a strict sub-set of the upstream structured dataset.
    if upstream.structured_dataset_type is not None:
        if downstream.structured_dataset_type is None:
            return False

        usdt = upstream.structured_dataset_type
        dsdt = downstream.structured_dataset_type

        if usdt.format != dsdt.format:
            return False

        if usdt.external_schema_type != dsdt.external_schema_type:
            return False

        if usdt.external_schema_bytes != dsdt.external_schema_bytes:
            return False

        ucols = usdt.columns
        dcols = dsdt.columns

        if len(ucols) != len(dcols):
            return False

        for u, d in zip(ucols, dcols):
            if u.name != d.name:
                return False

            if not _are_types_castable(u.literal_type, d.literal_type):
                return False

        return True

    if upstream.union_type is not None:
        # for each upstream variant, there must be a compatible type downstream
        for v in upstream.union_type.variants:
            if not _are_types_castable(v, downstream):
                return False
        return True

    if downstream.union_type is not None:
        # there must be a compatible downstream type
        for v in downstream.union_type.variants:
            if _are_types_castable(upstream, v):
                return True

    if upstream.enum_type is not None:
        # enums are castable to string
        if downstream.simple == SimpleType.STRING:
            return True

    if _type_essence(upstream) == _type_essence(downstream):
        return True

    return False


def _is_union_type(t):
    """Returns True if t is a Union type."""

    if sys.version_info >= (3, 10):
        import types

        UnionType = types.UnionType
    else:
        UnionType = None

    return t is typing.Union or get_origin(t) is typing.Union or UnionType and isinstance(t, UnionType)


class UnionTransformer(AsyncTypeTransformer[T]):
    """
    Transformer that handles a typing.Union[T1, T2, ...]
    """

    def __init__(self):
        super().__init__("Typed Union", typing.Union)

    @staticmethod
    def is_optional_type(t: Type) -> bool:
        return _is_union_type(t) and type(None) in get_args(t)

    @staticmethod
    def get_sub_type_in_optional(t: Type[T]) -> Type[T]:
        """
        Return the generic Type T of the Optional type
        """
        return get_args(t)[0]

    def assert_type(self, t: Type[T], v: T):
        python_type = get_underlying_type(t)
        if _is_union_type(python_type):
            for sub_type in get_args(python_type):
                if sub_type == typing.Any:
                    # this is an edge case
                    return
                try:
                    sub_trans: TypeTransformer = TypeEngine.get_transformer(sub_type)
                    if sub_trans.type_assertions_enabled:
                        sub_trans.assert_type(sub_type, v)
                        return
                    else:
                        return
                except TypeTransformerFailedError:
                    continue
                except TypeError:
                    continue
            raise TypeTransformerFailedError(f"Value {v} is not of type {t}")

    def get_literal_type(self, t: Type[T]) -> Optional[LiteralType]:
        t = get_underlying_type(t)

        try:
            trans: typing.List[typing.Tuple[TypeTransformer, typing.Any]] = [
                (TypeEngine.get_transformer(x), x) for x in get_args(t)
            ]
            # must go through TypeEngine.to_literal_type instead of trans.get_literal_type
            # to handle Annotated
            variants = [_add_tag_to_type(TypeEngine.to_literal_type(x), t.name) for (t, x) in trans]
            return _type_models.LiteralType(union_type=UnionType(variants))
        except Exception as e:
            raise ValueError(f"Type of Generic Union type is not supported, {e}")

    async def async_to_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> typing.Union[Literal, asyncio.Future]:
        python_type = get_underlying_type(python_type)

        potential_types = []
        found_res = False
        is_ambiguous = False
        res = None
        res_type = None
        t = None
        for i in range(len(get_args(python_type))):
            try:
                t = get_args(python_type)[i]
                trans: TypeTransformer[T] = TypeEngine.get_transformer(t)
                if isinstance(trans, AsyncTypeTransformer):
                    attempt = trans.async_to_literal(ctx, python_val, t, expected.union_type.variants[i])
                    res = await attempt
                else:
                    res = trans.to_literal(ctx, python_val, t, expected.union_type.variants[i])
                if found_res:
                    logger.debug(f"Current type {get_args(python_type)[i]} old res {res_type}")
                    is_ambiguous = True
                res_type = _add_tag_to_type(trans.get_literal_type(t), trans.name)
                found_res = True
                potential_types.append(t)
            except Exception as e:
                logger.debug(
                    f"UnionTransformer failed attempt to convert from {python_val} to {t} error: {e}",
                )
                continue

        if is_ambiguous:
            raise TypeError(
                f"Ambiguous choice of variant for union type.\n"
                f"Potential types: {potential_types}\n"
                "These types are structurally the same, because it's attributes have the same names and associated types."
            )

        if found_res:
            return Literal(scalar=Scalar(union=Union(value=res, stored_type=res_type)))

        raise TypeTransformerFailedError(f"Cannot convert from {python_val} to {python_type}")

    async def async_to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]
    ) -> Optional[typing.Any]:
        expected_python_type = get_underlying_type(expected_python_type)

        union_tag = None
        union_type = None
        if lv.scalar is not None and lv.scalar.union is not None:
            union_type = lv.scalar.union.stored_type
            if union_type.structure is not None:
                union_tag = union_type.structure.tag

        found_res = False
        is_ambiguous = False
        cur_transformer = ""
        res = None
        res_tag = None
        # This is serial, not actually async, but should be okay since it's more reasonable for Unions.
        for v in get_args(expected_python_type):
            try:
                trans: TypeTransformer[T] = TypeEngine.get_transformer(v)
                if union_tag is not None:
                    if trans.name != union_tag:
                        continue

                    expected_literal_type = TypeEngine.to_literal_type(v)
                    if not _are_types_castable(union_type, expected_literal_type):
                        continue

                    assert lv.scalar is not None  # type checker
                    assert lv.scalar.union is not None  # type checker

                    if isinstance(trans, AsyncTypeTransformer):
                        if lv.scalar.binary:
                            res = await trans.async_to_python_value(ctx, lv, v)
                        else:
                            res = await trans.async_to_python_value(ctx, lv.scalar.union.value, v)
                    else:
                        if lv.scalar.binary:
                            res = trans.to_python_value(ctx, lv, v)
                        else:
                            res = trans.to_python_value(ctx, lv.scalar.union.value, v)
                        if isinstance(res, asyncio.Future):
                            res = await res

                    if found_res:
                        is_ambiguous = True
                        cur_transformer = trans.name
                        break
                else:
                    if isinstance(trans, AsyncTypeTransformer):
                        res = await trans.async_to_python_value(ctx, lv, v)
                    else:
                        res = trans.to_python_value(ctx, lv, v)
                    if found_res:
                        is_ambiguous = True
                        cur_transformer = trans.name
                        break
                res_tag = trans.name
                found_res = True
            except Exception as e:
                logger.debug(f"Failed to convert from {lv} to {v} with error: {e}")

        if is_ambiguous:
            raise TypeError(
                "Ambiguous choice of variant for union type. "
                + f"Both {res_tag} and {cur_transformer} transformers match"
            )

        if found_res:
            return res

        raise TypeError(f"Cannot convert from {lv} to {expected_python_type} (using tag {union_tag})")

    def guess_python_type(self, literal_type: LiteralType) -> type:
        if literal_type.union_type is not None:
            return typing.Union[tuple(TypeEngine.guess_python_type(v) for v in literal_type.union_type.variants)]  # type: ignore

        raise ValueError(f"Union transformer cannot reverse {literal_type}")


class DictTransformer(AsyncTypeTransformer[dict]):
    """
    Transformer that transforms an univariate dictionary Dict[str, T] to a Literal Map or
    transforms an untyped dictionary to a Binary Scalar Literal with a Struct Literal Type.
    """

    def __init__(self):
        super().__init__("Typed Dict", dict)

    @staticmethod
    def extract_types(t: Optional[Type[dict]]) -> typing.Tuple:
        if t is None:
            return None, None

        # Get the origin and type arguments.
        _origin = get_origin(t)
        _args = get_args(t)

        # If not annotated or dict, return None, None.
        if _origin is None:
            return None, None

        # If this is something like Annotated[dict[int, str], FlyteAnnotation("abc")],
        # we need to check if there's a FlyteAnnotation in the metadata.
        if _origin is Annotated:
            # This case should never happen since Python's typing system requires at least two arguments
            # for Annotated[...] - a type and an annotation. Including this check for completeness.
            if not _args:
                return None, None

            first_arg = _args[0]
            # Check the rest of the metadata (after the dict type itself).
            for x in _args[1:]:
                if isinstance(x, FlyteAnnotation):
                    raise ValueError(
                        f"Flytekit does not currently have support for FlyteAnnotations applied to dicts. {t} cannot be parsed."
                    )
            # Recursively process the first argument if it's Annotated (or dict).
            return DictTransformer.extract_types(first_arg)

        # If the origin is dict, return the type arguments if they exist.
        if _origin is dict:
            # _args can be ().
            if _args is not None:
                return _args  # type: ignore

        # Otherwise, we do not support this type in extract_types.
        raise ValueError(f"Trying to extract dictionary type information from a non-dict type {t}")

    @staticmethod
    async def dict_to_generic_literal(
        ctx: FlyteContext, v: dict, python_type: Type[dict], allow_pickle: bool
    ) -> Literal:
        """
        This is deprecated from flytekit 1.14.0.
        Creates a flyte-specific ``Literal`` value from a native python dictionary.
        Note: This is deprecated and will be removed in the future.
        """
        from flytekit.types.pickle import FlytePickle

        try:
            try:
                # JSONEncoder is mashumaro's codec and this can triggered Flyte Types customized serialization and deserialization.
                encoder = JSONEncoder(python_type)
                json_str = encoder.encode(v)
            except NotImplementedError:
                raise NotImplementedError(
                    f"{python_type} should inherit from mashumaro.types.SerializableType"
                    f" and implement _serialize and _deserialize methods."
                )

            return Literal(
                scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())),
                metadata={"format": "json"},
            )
        except TypeError as e:
            if allow_pickle:
                remote_path = await FlytePickle.to_pickle(ctx, v)
                return Literal(
                    scalar=Scalar(
                        generic=_json_format.Parse(json.dumps({"pickle_file": remote_path}), _struct.Struct())
                    ),
                    metadata={"format": "pickle"},
                )
            raise TypeTransformerFailedError(f"Cannot convert `{v}` to Flyte Literal.\n" f"Error Message: {e}")

    @staticmethod
    async def dict_to_binary_literal(
        ctx: FlyteContext, v: dict, python_type: Type[dict], allow_pickle: bool
    ) -> Literal:
        """
        Converts a Python dictionary to a Flyte-specific ``Literal`` using MessagePack encoding.
        Falls back to Pickle if encoding fails and `allow_pickle` is True.
        """
        from flytekit.types.pickle import FlytePickle

        try:
            # Handle dictionaries with non-string keys (e.g., Dict[int, Type])
            encoder = MessagePackEncoder(python_type)
            msgpack_bytes = encoder.encode(v)
            return Literal(scalar=Scalar(binary=Binary(value=msgpack_bytes, tag=MESSAGEPACK)))
        except TypeError as e:
            if allow_pickle:
                remote_path = await FlytePickle.to_pickle(ctx, v)
                return Literal(
                    scalar=Scalar(
                        generic=_json_format.Parse(json.dumps({"pickle_file": remote_path}), _struct.Struct())
                    ),
                    metadata={"format": "pickle"},
                )
            raise TypeTransformerFailedError(f"Cannot convert `{v}` to Flyte Literal.\n" f"Error Message: {e}")

    @staticmethod
    def is_pickle(python_type: Type[dict]) -> bool:
        _origin = get_origin(python_type)
        metadata: typing.Tuple = ()
        if _origin is Annotated:
            metadata = get_args(python_type)[1:]

        for each_metadata in metadata:
            if isinstance(each_metadata, OrderedDict):
                allow_pickle = each_metadata.get("allow_pickle", False)
                return allow_pickle

        return False

    def get_literal_type(self, t: Type[dict]) -> LiteralType:
        """
        Transforms a native python dictionary to a flyte-specific ``LiteralType``
        """
        tp = DictTransformer.extract_types(t)

        if tp:
            if tp[0] == str:
                try:
                    sub_type = TypeEngine.to_literal_type(cast(type, tp[1]))
                    return _type_models.LiteralType(map_value_type=sub_type)
                except Exception as e:
                    raise ValueError(f"Type of Generic List type is not supported, {e}")
        return _type_models.LiteralType(
            simple=_type_models.SimpleType.STRUCT,
            annotation=TypeAnnotationModel({CACHE_KEY_METADATA: {SERIALIZATION_FORMAT: MESSAGEPACK}}),
        )

    async def async_to_literal(
        self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[dict], expected: LiteralType
    ) -> Literal:
        if type(python_val) != dict:
            raise TypeTransformerFailedError("Expected a dict")

        allow_pickle = False

        if get_origin(python_type) is Annotated:
            allow_pickle = DictTransformer.is_pickle(python_type)

        if expected and expected.simple and expected.simple == SimpleType.STRUCT:
            if str2bool(os.getenv(FLYTE_USE_OLD_DC_FORMAT)):
                return await self.dict_to_generic_literal(ctx, python_val, python_type, allow_pickle)
            return await self.dict_to_binary_literal(ctx, python_val, python_type, allow_pickle)

        lit_map = {}
        for k, v in python_val.items():
            if type(k) != str:
                raise ValueError("Flyte MapType expects all keys to be strings")
            # TODO: log a warning for Annotated objects that contain HashMethod

            _, v_type = self.extract_types(python_type)
            lit_map[k] = TypeEngine.async_to_literal(ctx, v, cast(type, v_type), expected.map_value_type)
        vals = await _run_coros_in_chunks([c for c in lit_map.values()], batch_size=_TYPE_ENGINE_COROS_BATCH_SIZE)
        for idx, k in zip(range(len(vals)), lit_map.keys()):
            lit_map[k] = vals[idx]

        return Literal(map=LiteralMap(literals=lit_map))

    async def async_to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[dict]) -> dict:
        if lv and lv.scalar and lv.scalar.binary is not None:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore

        if lv and lv.map and lv.map.literals is not None:
            tp = DictTransformer.extract_types(expected_python_type)

            if tp is None or len(tp) == 0 or tp[0] is None:
                raise TypeError(
                    "TypeMismatch: Cannot convert to python dictionary from Flyte Literal Dictionary as the given "
                    "dictionary does not have sub-type hints or they do not match with the originating dictionary "
                    "source. Flytekit does not currently support implicit conversions"
                )
            if tp[0] != str:
                raise TypeError("TypeMismatch. Destination dictionary does not accept 'str' key")
            py_map = {}
            for k, v in lv.map.literals.items():
                py_map[k] = TypeEngine.async_to_python_value(ctx, v, cast(Type, tp[1]))

            vals = await _run_coros_in_chunks([c for c in py_map.values()], batch_size=_TYPE_ENGINE_COROS_BATCH_SIZE)
            for idx, k in zip(range(len(vals)), py_map.keys()):
                py_map[k] = vals[idx]

            return py_map

        # for empty generic we have to explicitly test for lv.scalar.generic is not None as empty dict
        # evaluates to false
        if lv and lv.scalar and lv.scalar.generic is not None:
            if lv.metadata and lv.metadata.get("format", None) == "pickle":
                from flytekit.types.pickle import FlytePickle

                uri = json.loads(_json_format.MessageToJson(lv.scalar.generic)).get("pickle_file")
                return await FlytePickle.from_pickle(uri)

            try:
                """
                Handles the case where Flyte Console provides input as a protobuf struct.
                When resolving an attribute like 'dc.dict_int_ff', FlytePropeller retrieves a dictionary.
                Mashumaro's decoder can convert this dictionary to the expected Python object if the correct type is provided.
                Since Flyte Types handle their own deserialization, the dictionary is automatically converted to the expected Python object.

                Example Code:
                @dataclass
                class DC:
                    dict_int_ff: Dict[int, FlyteFile]

                @workflow
                def wf(dc: DC):
                    t_ff(dc.dict_int_ff)

                Life Cycle:
                json str            -> protobuf struct         -> resolved protobuf struct   -> dictionary                -> expected Python object
                (console user input)   (console output)           (propeller)                   (flytekit dict transformer)  (mashumaro decoder)

                Related PR:
                - Title: Override Dataclass Serialization/Deserialization Behavior for FlyteTypes via Mashumaro
                - Link: https://github.com/flyteorg/flytekit/pull/2554
                - Title: Binary IDL With MessagePack
                - Link: https://github.com/flyteorg/flytekit/pull/2760
                """

                dict_obj = json.loads(_json_format.MessageToJson(lv.scalar.generic))
                msgpack_bytes = msgpack.dumps(dict_obj)

                try:
                    decoder = self._msgpack_decoder[expected_python_type]
                except KeyError:
                    decoder = MessagePackDecoder(expected_python_type, pre_decoder_func=_default_msgpack_decoder)
                    self._msgpack_decoder[expected_python_type] = decoder

                return decoder.decode(msgpack_bytes)
            except TypeError:
                raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

        raise TypeTransformerFailedError(f"Cannot convert from {lv} to {expected_python_type}")

    def guess_python_type(self, literal_type: LiteralType) -> Union[Type[dict], typing.Dict[Type, Type]]:
        if literal_type.map_value_type:
            mt: Type = TypeEngine.guess_python_type(literal_type.map_value_type)
            return typing.Dict[str, mt]  # type: ignore

        if literal_type.simple == SimpleType.STRUCT:
            if literal_type.metadata is None:
                return dict  # type: ignore

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

    def get_literal_type(self, t: typing.TextIO) -> LiteralType:  # type: ignore
        return _type_models.LiteralType(blob=self._blob_type())

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


def generate_attribute_list_from_dataclass_json(schema: dict, schema_name: typing.Any):
    attribute_list: typing.List[tuple[Any, GenericAlias]] = []
    for property_key, property_val in schema[schema_name]["properties"].items():
        property_type = property_val["type"]
        # Handle list
        if property_val["type"] == "array":
            attribute_list.append((property_key, List[_get_element_type(property_val["items"])]))  # type: ignore[misc,index]
        # Handle dataclass and dict
        elif property_type == "object":
            # For nested dataclass
            if property_val.get("$ref"):
                name = property_val["$ref"].split("/")[-1]
                attribute_list.append(
                    (
                        property_key,
                        typing.cast(GenericAlias, convert_marshmallow_json_schema_to_python_class(schema, name)),
                    )
                )
            # typed dict and untyped dict
            elif property_val.get("additionalProperties"):
                # untyped dict
                if property_val["additionalProperties"] == dict():
                    attribute_list.append((property_key, dict))  # type: ignore
                else:
                    # typed dict
                    attribute_list.append(
                        (property_key, Dict[str, _get_element_type(property_val["additionalProperties"])])  # type: ignore[misc,index]
                    )
        # Handle primitive types like int, float, bool or str
        else:
            attribute_list.append([property_key, _get_element_type(property_val)])  # type: ignore
    return attribute_list


def convert_marshmallow_json_schema_to_python_class(schema: dict, schema_name: typing.Any) -> type:
    """
    Generate a model class based on the provided JSON Schema
    :param schema: dict representing valid JSON schema
    :param schema_name: dataclass name of return type
    """

    attribute_list = generate_attribute_list_from_dataclass_json(schema, schema_name)
    return dataclass_json(dataclasses.make_dataclass(schema_name, attribute_list))


def convert_mashumaro_json_schema_to_python_class(schema: dict, schema_name: typing.Any) -> type:
    """
    Generate a model class based on the provided JSON Schema
    :param schema: dict representing valid JSON schema
    :param schema_name: dataclass name of return type
    """

    attribute_list = generate_attribute_list_from_dataclass_json_mixin(schema, schema_name)
    return dataclass_json(dataclasses.make_dataclass(schema_name, attribute_list))


def _get_element_type(element_property: typing.Dict[str, str]) -> Type:
    element_type = (
        [e_property["type"] for e_property in element_property["anyOf"]]  # type: ignore
        if element_property.get("anyOf")
        else element_property["type"]
    )
    element_format = element_property["format"] if "format" in element_property else None

    if isinstance(element_type, list):
        # Element type of Optional[int] is [integer, None]
        return typing.Optional[_get_element_type({"type": element_type[0]})]  # type: ignore

    if element_type == "string":
        return str
    elif element_type == "integer":
        return int
    elif element_type == "boolean":
        return bool
    elif element_type == "number":
        if element_format == "integer":
            return int
        else:
            return float
    return str


def dataclass_from_dict(cls: type, src: typing.Dict[str, typing.Any]) -> typing.Any:
    """
    Utility function to construct a dataclass object from dict
    """
    field_types_lookup = {field.name: field.type for field in dataclasses.fields(cls)}

    constructor_inputs = {}
    for field_name, value in src.items():
        if dataclasses.is_dataclass(field_types_lookup[field_name]):
            constructor_inputs[field_name] = dataclass_from_dict(field_types_lookup[field_name], value)
        else:
            constructor_inputs[field_name] = value

    return cls(**constructor_inputs)


def strict_type_hint_matching(input_val: typing.Any, target_literal_type: LiteralType) -> typing.Type:
    """
    Try to be smarter about guessing the type of the input (and hence the transformer).
    If the literal type from the transformer for type(v), matches the literal type of the interface, then we
    can use type(). Otherwise, fall back to guess python type from the literal type.
    Raises ValueError, like in case of [1,2,3] type() will just give `list`, which won't work.
    Raises ValueError also if the transformer found for the raw type doesn't have a literal type match.
    """
    native_type = type(input_val)
    transformer: TypeTransformer = TypeEngine.get_transformer(native_type)
    inferred_literal_type = transformer.get_literal_type(native_type)
    # note: if no good match, transformer will be the pickle transformer, but type will not match unless it's the
    # pickle type so will fall back to normal guessing
    if literal_types_match(inferred_literal_type, target_literal_type):
        return type(input_val)

    raise ValueError(
        f"Transformer for {native_type} returned literal type {inferred_literal_type} which doesn't match {target_literal_type}"
    )


def _check_and_covert_float(lv: Literal) -> float:
    if lv.scalar.primitive.float_value is not None:
        return lv.scalar.primitive.float_value
    elif lv.scalar.primitive.integer is not None:
        return float(lv.scalar.primitive.integer)
    raise TypeTransformerFailedError(f"Cannot convert literal {lv} to float")


def _handle_flyte_console_float_input_to_int(lv: Literal) -> int:
    """
    Flyte Console is written by JavaScript and JavaScript has only one number type which is Number.
    Sometimes it keeps track of trailing 0s and sometimes it doesn't.
    We have to convert float to int back in the following example.

    Example Code:
    @dataclass
    class DC:
        a: int

    @workflow
    def wf(dc: DC):
        t_int(a=dc.a)

    Life Cycle:
    json str            -> protobuf struct         -> resolved float    -> float                          -> int
    (console user input)   (console output)           (propeller)          (flytekit simple transformer)  (_handle_flyte_console_float_input_to_int)
    """
    if lv.scalar.primitive.integer is not None:
        return lv.scalar.primitive.integer

    if lv.scalar.primitive.float_value is not None:
        logger.info(f"Converting literal float {lv.scalar.primitive.float_value} to int, might have precision loss.")
        return int(lv.scalar.primitive.float_value)

    raise TypeTransformerFailedError(f"Cannot convert literal {lv} to int")


def _check_and_convert_void(lv: Literal) -> None:
    if lv.scalar.none_type is None:
        raise TypeTransformerFailedError(f"Cannot convert literal {lv} to None")
    return None


IntTransformer = SimpleTransformer(
    "int",
    int,
    _type_models.LiteralType(simple=_type_models.SimpleType.INTEGER),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(integer=x))),
    _handle_flyte_console_float_input_to_int,
)

FloatTransformer = SimpleTransformer(
    "float",
    float,
    _type_models.LiteralType(simple=_type_models.SimpleType.FLOAT),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(float_value=x))),
    _check_and_covert_float,
)

BoolTransformer = SimpleTransformer(
    "bool",
    bool,
    _type_models.LiteralType(simple=_type_models.SimpleType.BOOLEAN),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(boolean=x))),
    lambda x: x.scalar.primitive.boolean,
)

StrTransformer = SimpleTransformer(
    "str",
    str,
    _type_models.LiteralType(simple=_type_models.SimpleType.STRING),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(string_value=x))),
    lambda x: x.scalar.primitive.string_value,
)

DatetimeTransformer = SimpleTransformer(
    "datetime",
    datetime.datetime,
    _type_models.LiteralType(simple=_type_models.SimpleType.DATETIME),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(datetime=x))),
    lambda x: x.scalar.primitive.datetime,
)

TimedeltaTransformer = SimpleTransformer(
    "timedelta",
    datetime.timedelta,
    _type_models.LiteralType(simple=_type_models.SimpleType.DURATION),
    lambda x: Literal(scalar=Scalar(primitive=Primitive(duration=x))),
    lambda x: x.scalar.primitive.duration,
)

DateTransformer = SimpleTransformer(
    "date",
    datetime.date,
    _type_models.LiteralType(simple=_type_models.SimpleType.DATETIME),
    lambda x: Literal(
        scalar=Scalar(primitive=Primitive(datetime=datetime.datetime.combine(x, datetime.time.min)))
    ),  # convert datetime to date
    lambda x: x.scalar.primitive.datetime.date(),  # get date from datetime
)

NoneTransformer = SimpleTransformer(
    "none",
    type(None),
    _type_models.LiteralType(simple=_type_models.SimpleType.NONE),
    lambda x: Literal(scalar=Scalar(none_type=Void())),
    lambda x: _check_and_convert_void(x),
)


def _register_default_type_transformers():
    TypeEngine.register(IntTransformer)
    TypeEngine.register(FloatTransformer)
    TypeEngine.register(StrTransformer)
    TypeEngine.register(DatetimeTransformer)
    TypeEngine.register(DateTransformer)
    TypeEngine.register(TimedeltaTransformer)
    TypeEngine.register(BoolTransformer)
    TypeEngine.register(NoneTransformer, [None])  # noqa
    TypeEngine.register(ListTransformer())
    if sys.version_info >= (3, 10):
        from types import UnionType

        TypeEngine.register(UnionTransformer(), [UnionType])
    else:
        TypeEngine.register(UnionTransformer())
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
    TypeEngine.register_restricted_type("non typed tuple", tuple)
    TypeEngine.register_restricted_type("non typed tuple", typing.Tuple)
    TypeEngine.register_restricted_type("named tuple", NamedTuple)


class LiteralsResolver(collections.UserDict):
    """
    LiteralsResolver is a helper class meant primarily for use with the FlyteRemote experience or any other situation
    where you might be working with LiteralMaps. This object allows the caller to specify the Python type that should
    correspond to an element of the map.
    """

    def __init__(
        self,
        literals: typing.Dict[str, Literal],
        variable_map: Optional[Dict[str, _interface_models.Variable]] = None,
        ctx: Optional[FlyteContext] = None,
    ):
        """
        :param literals: A Python map of strings to Flyte Literal models.
        :param variable_map: This map should be basically one side (either input or output) of the Flyte
          TypedInterface model and is used to guess the Python type through the TypeEngine if a Python type is not
          specified by the user. TypeEngine guessing is flaky though, so calls to get() should specify the as_type
          parameter when possible.
        """
        super().__init__(literals)
        if literals is None:
            raise ValueError("Cannot instantiate LiteralsResolver without a map of Literals.")
        self._literals = literals
        self._variable_map = variable_map
        self._native_values: Dict[str, type] = {}
        self._type_hints: Dict[str, type] = {}
        self._ctx = ctx

    def __str__(self) -> str:
        if self.literals:
            if len(self.literals) == len(self.native_values):
                return str(self.native_values)
            if self.native_values:
                header = "Partially converted to native values, call get(key, <type_hint>) to convert rest...\n"
                strs = []
                for key, literal in self._literals.items():
                    if key in self._native_values:
                        strs.append(f"{key}: " + str(self._native_values[key]) + "\n")
                    else:
                        lit_txt = str(self._literals[key])
                        lit_txt = textwrap.indent(lit_txt, " " * (len(key) + 2))
                        strs.append(f"{key}: \n" + lit_txt)

                return header + "{\n" + textwrap.indent("".join(strs), " " * 2) + "\n}"
            else:
                return str(literal_map_string_repr(self.literals))
        return "{}"

    def __repr__(self):
        return self.__str__()

    @property
    def native_values(self) -> typing.Dict[str, typing.Any]:
        return self._native_values

    @property
    def variable_map(self) -> Optional[Dict[str, _interface_models.Variable]]:
        return self._variable_map

    @property
    def literals(self):
        return self._literals

    def update_type_hints(self, type_hints: typing.Dict[str, typing.Type]):
        self._type_hints.update(type_hints)

    def get_literal(self, key: str) -> Literal:
        if key not in self._literals:
            raise ValueError(f"Key {key} is not in the literal map")

        return self._literals[key]

    def as_python_native(self, python_interface: Interface) -> typing.Any:
        """
        This should return the native Python representation, compatible with unpacking.
        This function relies on Python interface outputs being ordered correctly.

        :param python_interface: Only outputs are used but easier to pass the whole interface.
        """
        if len(self.literals) == 0:
            return None

        if self.variable_map is None:
            raise AssertionError(f"Variable map is empty in literals resolver with {self.literals}")

        # Trigger get() on everything to make sure native values are present using the python interface as type hint
        for lit_key, lit in self.literals.items():
            self.get(lit_key, as_type=python_interface.outputs.get(lit_key))

        # if 1 item, then return 1 item
        if len(self.native_values) == 1:
            return next(iter(self.native_values.values()))

        # if more than 1 item, then return a tuple - can ignore naming the tuple unless it becomes a problem
        # This relies on python_interface.outputs being ordered correctly.
        res = cast(typing.Tuple[typing.Any, ...], ())
        for var_name, _ in python_interface.outputs.items():
            if var_name not in self.native_values:
                raise ValueError(f"Key {var_name} is not in the native values")

            res += (self.native_values[var_name],)

        return res

    def __getitem__(self, key: str):
        # First check to see if it's even in the literal map.
        if key not in self._literals:
            raise ValueError(f"Key {key} is not in the literal map")

        # Return the cached value if it's cached
        if key in self._native_values:
            return self._native_values[key]

        return self.get(key)

    def get(self, attr: str, as_type: Optional[typing.Type] = None) -> typing.Any:  # type: ignore
        """
        This will get the ``attr`` value from the Literal map, and invoke the TypeEngine to convert it into a Python
        native value. A Python type can optionally be supplied. If successful, the native value will be cached and
        future calls will return the cached value instead.

        :param attr:
        :param as_type:
        :return: Python native value from the LiteralMap
        """
        if attr not in self._literals:
            raise AttributeError(f"Attribute {attr} not found")
        if attr in self.native_values:
            return self.native_values[attr]

        if as_type is None:
            if attr in self._type_hints:
                as_type = self._type_hints[attr]
            else:
                if self.variable_map and attr in self.variable_map:
                    try:
                        as_type = TypeEngine.guess_python_type(self.variable_map[attr].type)
                    except ValueError as e:
                        logger.error(f"Could not guess a type for Variable {self.variable_map[attr]}")
                        raise e
                else:
                    raise ValueError("as_type argument not supplied and Variable map not specified in LiteralsResolver")
        val = TypeEngine.to_python_value(
            self._ctx or FlyteContext.current_context(), self._literals[attr], cast(Type, as_type)
        )
        self._native_values[attr] = val
        return val


_register_default_type_transformers()


def is_annotated(t: Type) -> bool:
    return get_origin(t) is Annotated


def get_underlying_type(t: Type) -> Type:
    """Return the underlying type for annotated types or the type itself"""
    if is_annotated(t):
        return get_args(t)[0]
    return t
