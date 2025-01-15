import json
import os
from typing import Any, List, Optional, Type

import msgpack
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from pydantic import BaseModel, create_model

from flytekit import FlyteContext
from flytekit.core.constants import CACHE_KEY_METADATA, FLYTE_USE_OLD_DC_FORMAT, MESSAGEPACK, SERIALIZATION_FORMAT
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.core.utils import str2bool
from flytekit.loggers import logger
from flytekit.models import types
from flytekit.models.annotation import TypeAnnotation as TypeAnnotationModel
from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.models.types import LiteralType, TypeStructure


class PydanticTransformer(TypeTransformer[BaseModel]):
    def __init__(self):
        super().__init__("Pydantic Transformer", BaseModel, enable_type_assertions=False)

    def get_literal_type(self, t: Type[BaseModel]) -> LiteralType:
        schema = t.model_json_schema()
        literal_type = {}
        fields = t.__annotations__.items()

        for name, python_type in fields:
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
            json_str = json.dumps(dict_obj)
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

        json_str = _json_format.MessageToJson(lv.scalar.generic)
        python_val = expected_python_type.model_validate_json(json_str, strict=False, context={"deserialize": True})
        return python_val

    def guess_python_type(self, literal_type: LiteralType) -> Type[BaseModel]:
        """
        Reconstructs the Pydantic BaseModel subclass from the JSON schema stored in LiteralType metadata.
        """

        schema = literal_type.metadata
        model_name = schema.get("title", "DynamicModel")

        properties = schema.get("properties", {})
        required_fields = schema.get("required", [])

        annotations = {}
        field_definitions = {}

        for field_name, field_info in properties.items():
            field_type = self._map_json_type_to_python(field_info)
            annotations[field_name] = Optional[field_type] if field_name not in required_fields else field_type
            field_definitions[field_name] = (field_type, ... if field_name in required_fields else None)

        try:
            DynamicModel = create_model(model_name, **field_definitions)
            return DynamicModel
        except Exception as e:
            raise TypeTransformerFailedError(f"Failed to create Pydantic model from schema: {e}")

    def _map_json_type_to_python(self, field_info: dict) -> Any:
        """
        Maps JSON schema types to Python types for Pydantic model fields.
        """
        json_type = field_info.get("type")
        if isinstance(json_type, list):
            # Handle Union types like ["string", "null"]
            json_type = [t for t in json_type if t != "null"]
            if len(json_type) == 1:
                json_type = json_type[0]
            else:
                # More complex unions can be handled here if needed
                json_type = "string"  # default fallback

        type_mapping = {
            "string": str,
            "integer": int,
            "number": float,
            "boolean": bool,
            "object": dict,
            "array": list,
        }

        python_type = type_mapping.get(json_type, Any)

        # Handle nested objects
        if python_type == dict and "properties" in field_info:
            # Recursively create a nested Pydantic model
            nested_model = self._create_nested_model(field_info)
            return nested_model

        # Handle arrays with specified items
        if python_type == list and "items" in field_info:
            item_type = self._map_json_type_to_python(field_info["items"])
            return List[item_type]

        return python_type

    def _create_nested_model(self, field_info: dict) -> Type[BaseModel]:
        """
        Recursively creates nested Pydantic models for objects within the schema.
        """
        properties = field_info.get("properties", {})
        required_fields = field_info.get("required", [])

        model_name = field_info.get("title", "NestedModel")
        annotations = {}
        field_definitions = {}

        for field_name, sub_field_info in properties.items():
            sub_field_type = self._map_json_type_to_python(sub_field_info)
            annotations[field_name] = Optional[sub_field_type] if field_name not in required_fields else sub_field_type
            field_definitions[field_name] = (sub_field_type, ... if field_name in required_fields else None)

        try:
            NestedModel = create_model(model_name, **field_definitions)
            return NestedModel
        except Exception as e:
            raise TypeTransformerFailedError(f"Failed to create nested Pydantic model: {e}")


TypeEngine.register(PydanticTransformer())
