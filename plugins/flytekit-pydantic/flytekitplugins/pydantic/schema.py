from typing import Type

from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, TypeTransformerFailedError
from flytekit.models.literals import Literal, Scalar
from flytekit.models.types import LiteralType, SimpleType


class BaseModelTransformer(TypeTransformer[BaseModel]):
    _TYPE_INFO = LiteralType(simple=SimpleType.STRUCT)

    def __init__(self):
        """Construct BaseModelTransformer."""
        super().__init__(name="basemodel-transform", t=BaseModel)

    def get_literal_type(self, t: Type[BaseModel]) -> LiteralType:
        return LiteralType(simple=SimpleType.STRUCT)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: BaseModel,
        python_type: Type[BaseModel],
        expected: LiteralType,
    ) -> Literal:
        """This method is used to convert from given python type object pydantic ``BaseModel`` to the Literal representation."""
        s = Struct()

        s.update({"schema": python_val.schema(), "data": python_val.dict()})

        return Literal(scalar=Scalar(generic=s))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[BaseModel]) -> BaseModel:
        """Re-hydrate the pydantic BaseModel object from Flyte Literal value."""
        base_model = MessageToDict(lv.scalar.generic)
        schema = base_model["schema"]
        data = base_model["data"]

        if (expected_schema := expected_python_type.schema()) != schema:
            raise TypeTransformerFailedError(
                f"The schema `{expected_schema}` of the expected python type {expected_python_type} is not equal to the received schema `{schema}`."
            )

        return expected_python_type.parse_obj(data)


TypeEngine.register(BaseModelTransformer())
