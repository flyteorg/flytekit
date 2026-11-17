import collections.abc
import json
import os
import types as _pytypes
import typing
from typing import Annotated, Any, Type, Union

import msgpack
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from pydantic import BaseModel, RootModel

from flytekit import FlyteContext
from flytekit.core.constants import CACHE_KEY_METADATA, FLYTE_USE_OLD_DC_FORMAT, MESSAGEPACK, SERIALIZATION_FORMAT
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.core.utils import str2bool
from flytekit.loggers import logger
from flytekit.models import types
from flytekit.models.annotation import TypeAnnotation as TypeAnnotationModel
from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.models.types import LiteralType, TypeStructure


def _strip_computed_fields(tp: Any, data: Any) -> Any:
    """
    Drop ``@computed_field`` values from serialized data before it is validated as ``tp``.

    ``model_dump_json()`` includes computed fields, so they are kept in stored outputs and shown in the UI. Pydantic
    treats them as extra inputs when it reads the model back, which fails for models with ``extra="forbid"``, and
    computes them again anyway. Stripping them on read also handles outputs that are already stored.
    """
    origin = typing.get_origin(tp)
    if origin is Annotated:
        return _strip_computed_fields(typing.get_args(tp)[0], data)
    if origin is Union or origin is _pytypes.UnionType:
        candidates = [arg for arg in typing.get_args(tp) if arg is not type(None)]
        # With several candidate types it isn't known which one the data is for, so leave it as it is.
        return _strip_computed_fields(candidates[0], data) if len(candidates) == 1 else data

    if isinstance(tp, type) and issubclass(tp, BaseModel):
        if issubclass(tp, RootModel):
            return _strip_computed_fields(tp.model_fields["root"].annotation, data)
        if not isinstance(data, dict):
            return data
        field_types = {}
        for name, info in tp.model_fields.items():
            for key in (name, info.alias, info.validation_alias):
                if isinstance(key, str):
                    field_types[key] = info.annotation
        computed_keys = set()
        for name, info in tp.model_computed_fields.items():
            computed_keys.update(key for key in (name, info.alias) if isinstance(key, str))
        return {
            key: _strip_computed_fields(field_types[key], value) if key in field_types else value
            for key, value in data.items()
            if key in field_types or key not in computed_keys
        }

    args = typing.get_args(tp)
    if not args:
        return data
    if isinstance(data, list):
        if origin is tuple:
            if len(args) == 2 and args[1] is Ellipsis:
                return [_strip_computed_fields(args[0], item) for item in data]
            if len(args) != len(data):
                return data
            return [_strip_computed_fields(arg, item) for arg, item in zip(args, data)]
        if isinstance(origin, type) and issubclass(origin, collections.abc.Iterable):
            return [_strip_computed_fields(args[0], item) for item in data]
    if isinstance(data, dict) and isinstance(origin, type) and issubclass(origin, collections.abc.Mapping):
        if len(args) == 2:
            return {key: _strip_computed_fields(args[1], value) for key, value in data.items()}
    return data


class PydanticTransformer(TypeTransformer[BaseModel]):
    def __init__(self):
        super().__init__("Pydantic Transformer", BaseModel, enable_type_assertions=False)

    def get_literal_type(self, t: Type[BaseModel]) -> LiteralType:
        schema = t.model_json_schema()
        literal_type = {}

        for name, field_info in t.model_fields.items():
            python_type = field_info.annotation
            try:
                literal_type[name] = TypeEngine.to_literal_type(python_type)
            except Exception as e:
                logger.warning(
                    "Field {} of type {} cannot be converted to a literal type. Error: {}".format(name, python_type, e)
                )

        # This is for attribute access in FlytePropeller.
        ts = TypeStructure(tag="", dataclass_type=literal_type)

        return types.LiteralType(
            simple=types.SimpleType.STRUCT,
            metadata=schema,
            structure=ts,
            annotation=TypeAnnotationModel({CACHE_KEY_METADATA: {SERIALIZATION_FORMAT: MESSAGEPACK}}),
        )

    def to_generic_literal(
        self,
        ctx: FlyteContext,
        python_val: BaseModel,
        python_type: Type[BaseModel],
        expected: types.LiteralType,
    ) -> Literal:
        """
        Note: This is deprecated and will be removed in the future.
        """
        json_str = python_val.model_dump_json()
        return Literal(scalar=Scalar(generic=_json_format.Parse(json_str, _struct.Struct())))

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: BaseModel,
        python_type: Type[BaseModel],
        expected: types.LiteralType,
    ) -> Literal:
        """
        For pydantic basemodel, we have to go through json first.
        This is for handling enum in basemodel.
        More details: https://github.com/flyteorg/flytekit/pull/2792
        """
        if str2bool(os.getenv(FLYTE_USE_OLD_DC_FORMAT)):
            return self.to_generic_literal(ctx, python_val, python_type, expected)

        json_str = python_val.model_dump_json()
        dict_obj = json.loads(json_str)
        msgpack_bytes = msgpack.dumps(dict_obj)
        return Literal(scalar=Scalar(binary=Binary(value=msgpack_bytes, tag=MESSAGEPACK)))

    def from_binary_idl(self, binary_idl_object: Binary, expected_python_type: Type[BaseModel]) -> BaseModel:
        if binary_idl_object.tag == MESSAGEPACK:
            dict_obj = msgpack.loads(binary_idl_object.value, strict_map_key=False)
            json_str = json.dumps(_strip_computed_fields(expected_python_type, dict_obj))
            python_val = expected_python_type.model_validate_json(
                json_data=json_str, strict=False, context={"deserialize": True}
            )
            return python_val
        else:
            raise TypeTransformerFailedError(f"Unsupported binary format: `{binary_idl_object.tag}`")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[BaseModel]) -> BaseModel:
        """
        There will have 2 kinds of literal values:
        1. protobuf Struct (From Flyte Console)
        2. binary scalar (Others)
        Hence we have to handle 2 kinds of cases.
        """
        if lv and lv.scalar and lv.scalar.binary is not None:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore

        dict_obj = json.loads(_json_format.MessageToJson(lv.scalar.generic))
        json_str = json.dumps(_strip_computed_fields(expected_python_type, dict_obj))
        python_val = expected_python_type.model_validate_json(json_str, strict=False, context={"deserialize": True})
        return python_val


TypeEngine.register(PydanticTransformer())
