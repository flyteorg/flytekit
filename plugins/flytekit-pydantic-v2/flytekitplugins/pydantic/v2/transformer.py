import json
from typing import Type, TypeVar

import msgpack
from google.protobuf import json_format as _json_format

from flytekit import FlyteContext
from flytekit.core.constants import MESSAGEPACK
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models import types
from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.models.types import LiteralType
from pydantic import BaseModel

T = TypeVar("T")


class PydanticTransformer(TypeTransformer[BaseModel]):
    def __init__(self):
        super().__init__("Pydantic Transformer", BaseModel, enable_type_assertions=False)

    def get_literal_type(self, t: Type[BaseModel]) -> LiteralType:
        return types.LiteralType(simple=types.SimpleType.STRUCT)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: BaseModel,
        python_type: Type[BaseModel],
        expected: types.LiteralType,
    ) -> Literal:
        dict_obj = python_val.model_dump()
        msgpack_bytes = msgpack.dumps(dict_obj)
        return Literal(scalar=Scalar(binary=Binary(value=msgpack_bytes, tag="msgpack")))

    def from_binary_idl(self, binary_idl_object: Binary, expected_python_type: Type[BaseModel]) -> T:
        if binary_idl_object.tag == MESSAGEPACK:
            dict_obj = msgpack.loads(binary_idl_object.value)
            python_val = expected_python_type.model_validate(obj=dict_obj, strict=False)
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
        dict_obj = json.loads(json_str)
        python_val = expected_python_type.model_validate(obj=dict_obj, strict=False)
        return python_val


TypeEngine.register(PydanticTransformer())
