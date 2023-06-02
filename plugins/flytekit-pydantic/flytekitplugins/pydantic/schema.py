from typing import Type

import pydantic
from google.protobuf import json_format, struct_pb2

from flytekit import FlyteContext
from flytekit.core import type_engine
from flytekit.models import literals, types

from . import basemodel_extensions

"""
Serializes & deserializes the pydantic basemodels
"""


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
        """This method is used to convert from given python type object pydantic ``pydantic.BaseModel`` to the Literal representation."""

        s = struct_pb2.Struct()
        python_val.__config__.json_encoders.update(basemodel_extensions.flyteobject_json_encoders)
        schema = python_val.schema_json()
        data = python_val.json()
        s.update({"schema": schema, "data": data})
        literal = literals.Literal(scalar=literals.Scalar(generic=s))  # type: ignore
        return literal

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: literals.Literal,
        expected_python_type: Type[pydantic.BaseModel],
    ) -> pydantic.BaseModel:
        """Re-hydrate the pydantic pydantic.BaseModel object from Flyte Literal value."""
        base_model = json_format.MessageToDict(lv.scalar.generic)
        schema = base_model["schema"]
        data = base_model["data"]
        if (expected_schema := expected_python_type.schema_json()) != schema:
            raise type_engine.TypeTransformerFailedError(
                f"The schema `{expected_schema}` of the expected python type {expected_python_type} is not equal to the received schema `{schema}`."
            )

        return expected_python_type.parse_raw(data)


type_engine.TypeEngine.register(BaseModelTransformer())
