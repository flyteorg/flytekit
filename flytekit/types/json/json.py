import json
import typing
from typing import Type

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer, DictTransformer
from flytekit.models.literals import Binary, Literal, Scalar
from flytekit.models.types import LiteralType, SimpleType

T = typing.TypeVar("T")


class JsonTransformer(TypeTransformer[dict]):
    def __init__(self):
        super().__init__(name="Typed Json", t=dict)

    def assert_type(self, t: Type[T], v: T):
        if isinstance(v, dict):
            return
        raise TypeError(f"{v} is not a dict")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        if lv.scalar.binary is None:
            return DictTransformer().to_python_value(ctx, lv, expected_python_type)
        print("metadata", lv.metadata)
        return json.loads(lv.scalar.binary.value.decode())

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        if python_val is None:
            raise AssertionError("Cannot serialize None")
        byte = str.encode(json.dumps(python_val))
        return Literal(scalar=Scalar(binary=Binary(value=byte, tag="json")), metadata={"hello": "world"})

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[dict]:
        if literal_type.simple.BINARY is not None and literal_type.metadata.get("flytejson") == "True":
            return dict

        return DictTransformer().guess_python_type(literal_type)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        lt = LiteralType(simple=SimpleType.BINARY)
        lt.metadata = {"python_class_name": str(t), "flytejson": "True"}
        return lt


TypeEngine.register(JsonTransformer())
