import json
import os
from typing import Type
from functools import lru_cache
from typing import Any, Dict, List, Type, Union

import msgpack
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from pydantic import BaseModel, Field, create_model


from flytekit import FlyteContext
from flytekit.core.constants import FLYTE_USE_OLD_DC_FORMAT, MESSAGEPACK
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.core.utils import str2bool
from flytekit.loggers import logger
from flytekit.models import types
from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.models.types import LiteralType, TypeStructure

DEFINITIONS = "definitions"
TITLE = "title"

# Helper function to convert JSON schema to Pydantic BaseModel
# Reference: https://github.com/pydantic/pydantic/issues/643#issuecomment-1999755873
def json_schema_to_model(json_schema: Dict[str, Any]) -> Type[BaseModel]:
    """
    Converts a JSON schema to a Pydantic BaseModel class.

    Args:
        json_schema: The JSON schema to convert.

    Returns:
        A Pydantic BaseModel class.
    """
    # Extract the model name from the schema title.
    model_name = json_schema.get('title', 'DynamicModel')

    # Extract the field definitions from the schema properties.
    field_definitions = {
        name: json_schema_to_pydantic_field(name, prop, json_schema.get('required', []))
        for name, prop in json_schema.get('properties', {}).items()
    }

    # Create the BaseModel class using create_model().
    return create_model(model_name, **field_definitions)

def json_schema_to_pydantic_field(name: str, json_schema: Dict[str, Any], required: List[str]) -> Any:
    """
    Converts a JSON schema property to a Pydantic field definition.

    Args:
        name: The field name.
        json_schema: The JSON schema property.

    Returns:
        A Pydantic field definition.
    """
    # Get the field type.
    type_ = json_schema_to_pydantic_type(json_schema)

    # Get the field description.
    description = json_schema.get('description', None)

    # Get the field examples.
    examples = json_schema.get('examples', None)

    # Create a Field object with the type, description, and examples.
    # The 'required' flag will be set later when creating the model.
    return (type_, Field(description=description, examples=examples, default=... if name in required else None))

def json_schema_to_pydantic_type(json_schema: Dict[str, Any]) -> Any:
    """
    Converts a JSON schema type to a Pydantic type.

    Args:
        json_schema: The JSON schema to convert.

    Returns:
        A Pydantic type.
    """
    type_ = json_schema.get('type')

    if type_ == 'string':
        return str
    elif type_ == 'integer':
        return int
    elif type_ == 'number':
        return float
    elif type_ == 'boolean':
        return bool
    elif type_ == 'array':
        items_schema = json_schema.get('items')
        if items_schema:
            item_type = json_schema_to_pydantic_type(items_schema)
            return List[item_type]
        else:
            return List
    elif type_ == 'object':
        # Handle nested models.
        properties = json_schema.get('properties')
        if properties:
            nested_model = json_schema_to_model(json_schema)
            return nested_model
        else:
            return Dict
    elif type_ == 'null':
        return Union[None, Any]  # Use Union[None, Any] for nullable fields
    else:
        raise ValueError(f'Unsupported JSON schema type: {type_}')


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

        return types.LiteralType(simple=types.SimpleType.STRUCT, metadata=schema, structure=ts)

    @lru_cache(typed=True)
    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:  # type: ignore
        if literal_type.simple == types.SimpleType.STRUCT:
            if literal_type.metadata is not None:
                return json_schema_to_model(literal_type.metadata)
        raise ValueError(f"Pydantic transformer cannot reverse {literal_type}")

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


TypeEngine.register(PydanticTransformer())
