import enum
import importlib
from typing import Type, TypeVar

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformer, TypeTransformerFailedError
from flytekit.extend import TypeEngine
from flytekit.models.literals import Literal, LiteralMap, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType

T = TypeVar("T")


class GenericEnumTransformer(TypeTransformer[enum.Enum]):
    def __init__(self):
        """Transformer for arbitrary enum.Enum objects."""
        super().__init__(name="GenericEnumTransformer", t=enum.Enum)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Provide type hint for Flytekit type system.

        To support the multivariate typing of generic enums, we encode them as binaries (no introspection) with
        multiple files.
        """
        return LiteralType(simple=SimpleType.STRUCT)

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        """Transform python enum to Flyte literal for serialisation"""
        if not isinstance(python_val, enum.Enum):
            raise TypeTransformerFailedError("Expected an enum")

        enum_type = type(python_val)
        node_type = type(python_val.value)
        transformer = TypeEngine.get_transformer(node_type)
        literal_type = transformer.get_literal_type(node_type)

        return Literal(
            map=LiteralMap(
                literals={
                    "enum_type": Literal(
                        scalar=Scalar(primitive=Primitive(string_value=f"{enum_type.__module__}/{enum_type.__name__}"))
                    ),
                    "type": Literal(
                        scalar=Scalar(primitive=Primitive(string_value=f"{node_type.__module__}/{node_type.__name__}"))
                    ),
                    "value": transformer.to_literal(ctx, python_val.value, node_type, literal_type),
                }
            )
        )

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        """Transform Literal back to python object (Enum)."""
        module_name, class_name = lv.map.literals["type"].scalar.primitive.string_value.split("/")
        value_literal = lv.map.literals["value"]
        node_type = importlib.import_module(module_name).__getattribute__(class_name)
        transformer = TypeEngine.get_transformer(node_type)
        base_value = transformer.to_python_value(ctx, value_literal, node_type)

        enum_module, enum_class = lv.map.literals["enum_type"].scalar.primitive.string_value.split("/")
        enum_type = importlib.import_module(enum_module).__getattribute__(enum_class)
        return enum_type(base_value)


TypeEngine._ENUM_TRANSFORMER = GenericEnumTransformer()
TypeEngine.register_additional_type(GenericEnumTransformer(), enum.Enum, override=True)
