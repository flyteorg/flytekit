"""Serializes & deserializes the pydantic basemodels """

from typing import Type

import pydantic
from google.protobuf import json_format
from typing_extensions import Annotated

from flytekit import FlyteContext
from flytekit.core import type_engine
from flytekit.models import literals, types

from . import deserialization, serialization

BaseModelLiteralMap = Annotated[
    literals.LiteralMap,
    """
    BaseModel serialized to a LiteralMap consisting of: 
        1) the basemodel json with placeholders for flyte types 
        2) mapping from placeholders to serialized flyte type values in the object store
    """,
]


class BaseModelTransformer(type_engine.TypeTransformer[pydantic.BaseModel]):
    _TYPE_INFO = types.LiteralType(simple=types.SimpleType.STRUCT)

    def __init__(self):
        """Construct pydantic.BaseModelTransformer."""
        super().__init__(name="basemodel-transform", t=pydantic.BaseModel)

    def get_literal_type(self, t: Type[pydantic.BaseModel]) -> types.LiteralType:
        return types.LiteralType(simple=types.SimpleType.STRUCT)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: pydantic.BaseModel,
        python_type: Type[pydantic.BaseModel],
        expected: types.LiteralType,
    ) -> literals.Literal:
        """Convert a given ``pydantic.BaseModel`` to the Literal representation."""
        return serialization.serialize_basemodel(python_val)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: literals.Literal,
        expected_python_type: Type[pydantic.BaseModel],
    ) -> pydantic.BaseModel:
        """Re-hydrate the pydantic BaseModel object from Flyte Literal value."""
        literalmap: BaseModelLiteralMap = lv.map
        basemodel_json_w_placeholders = read_basemodel_json_from_literalmap(literalmap)
        flyte_obj_literalmap = literalmap.literals[serialization.FLYTETYPE_OBJSTORE_KEY]
        with deserialization.PydanticDeserializationLiteralStore.attach(flyte_obj_literalmap):
            return expected_python_type.parse_raw(basemodel_json_w_placeholders)


def read_basemodel_json_from_literalmap(lv: BaseModelLiteralMap) -> serialization.SerializedBaseModel:
    basemodel_literal: literals.Literal = lv.literals[serialization.BASEMODEL_JSON_KEY]
    basemodel_json_w_placeholders = json_format.MessageToJson(basemodel_literal.scalar.generic)
    assert isinstance(basemodel_json_w_placeholders, str)
    return basemodel_json_w_placeholders


type_engine.TypeEngine.register(BaseModelTransformer())
