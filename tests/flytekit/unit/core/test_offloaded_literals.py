from dataclasses import dataclass
import typing

from mashumaro.mixins.json import DataClassJSONMixin
import pytest
from flytekit import task
from flytekit.configuration import Image, ImageConfig, SerializationSettings
from flytekit.models import literals as literal_models
from flytekit.core import context_manager
from flytekit.models.types import SimpleType
from flytekit.core.type_engine import TypeEngine

@pytest.fixture
def flyte_ctx():
    return context_manager.FlyteContext.current_context()


def test_task_offloaded_literal_single_input(tmp_path):
    @task
    def t1(a: int) -> str:
        return str(a)

    original_input_literal = literal_models.Literal(
        scalar=literal_models.Scalar(primitive=literal_models.Primitive(integer=3))
    )

    # Write offloaded_lv as bytes to a temp file
    with open(f"{tmp_path}/offloaded_proto.pb", "wb") as f:
        f.write(original_input_literal.to_flyte_idl().SerializeToString())

    offloaded_input_literal = literal_models.Literal(
        offloaded_metadata=literal_models.LiteralOffloadedMetadata(
            uri=f"{tmp_path}/offloaded_proto.pb",
            inferred_type=literal_models.LiteralType(simple=SimpleType.INTEGER),
        )
    )

    ctx = context_manager.FlyteContextManager.current_context()
    output_lm = t1.dispatch_execute(
        ctx,
        literal_models.LiteralMap(
            literals={
                "a": offloaded_input_literal,
            }
        ),
    )
    assert output_lm.literals["o0"].scalar.primitive.string_value == "3"


def test_task_offloaded_literal_multiple_input(tmp_path):
    @task
    def t1(a: int, b: int) -> int:
        return a + b

    original_input_literal_a = literal_models.Literal(
        scalar=literal_models.Scalar(primitive=literal_models.Primitive(integer=3))
    )
    original_input_literal_b = literal_models.Literal(
        scalar=literal_models.Scalar(primitive=literal_models.Primitive(integer=4))
    )

    # Write offloaded_lv as bytes to a temp file
    with open(f"{tmp_path}/offloaded_proto_a.pb", "wb") as f:
        f.write(original_input_literal_a.to_flyte_idl().SerializeToString())
    with open(f"{tmp_path}/offloaded_proto_b.pb", "wb") as f:
        f.write(original_input_literal_b.to_flyte_idl().SerializeToString())

    offloaded_input_literal_a = literal_models.Literal(
        offloaded_metadata=literal_models.LiteralOffloadedMetadata(
            uri=f"{tmp_path}/offloaded_proto_a.pb",
            inferred_type=literal_models.LiteralType(simple=SimpleType.INTEGER),
        )
    )
    offloaded_input_literal_b = literal_models.Literal(
        offloaded_metadata=literal_models.LiteralOffloadedMetadata(
            uri=f"{tmp_path}/offloaded_proto_b.pb",
            inferred_type=literal_models.LiteralType(simple=SimpleType.INTEGER),
        )
    )

    ctx = context_manager.FlyteContextManager.current_context()
    output_lm = t1.dispatch_execute(
        ctx,
        literal_models.LiteralMap(
            literals={
                "a": offloaded_input_literal_a,
                "b": offloaded_input_literal_b,
            }
        ),
    )
    assert output_lm.literals["o0"].scalar.primitive.integer == 7


def test_task_offloaded_literal_single_dataclass(tmp_path, flyte_ctx):
    @dataclass
    class DC(DataClassJSONMixin):
        x: int
        y: str
        z: typing.List[int]

    @task
    def t1(dc: DC) -> DC:
        return dc

    lt = TypeEngine.to_literal_type(DC)
    original_input_literal = TypeEngine.to_literal(flyte_ctx, DC(x=3, y="hello", z=[1, 2, 3]), DC, lt)

    with open(f"{tmp_path}/offloaded_proto.pb", "wb") as f:
        f.write(original_input_literal.to_flyte_idl().SerializeToString())

    offloaded_input_literal = literal_models.Literal(
        offloaded_metadata=literal_models.LiteralOffloadedMetadata(
            uri=f"{tmp_path}/offloaded_proto.pb",
            inferred_type=lt,
        )
    )

    ctx = context_manager.FlyteContextManager.current_context()
    output_lm = t1.dispatch_execute(
        ctx,
        literal_models.LiteralMap(
            literals={
                "dc": offloaded_input_literal,
            }
        ),
    )
    assert output_lm.literals["o0"] == original_input_literal


def test_task_offloaded_literal_list_int(tmp_path):
    @task
    def t1(xs: typing.List[int]) -> typing.List[str]:
        return [str(a) for a in xs]

    original_input_literal = literal_models.Literal(
        collection=literal_models.LiteralCollection(
            literals=[
                literal_models.Literal(
                    scalar=literal_models.Scalar(primitive=literal_models.Primitive(integer=3))
                ),
                literal_models.Literal(
                    scalar=literal_models.Scalar(primitive=literal_models.Primitive(integer=4))
                ),
            ]
        )
    )
    expected_output_literal = literal_models.Literal(
        collection=literal_models.LiteralCollection(
            literals=[
                literal_models.Literal(
                    scalar=literal_models.Scalar(primitive=literal_models.Primitive(string_value="3"))
                ),
                literal_models.Literal(
                    scalar=literal_models.Scalar(primitive=literal_models.Primitive(string_value="4"))
                ),
            ]
        )
    )

    with open(f"{tmp_path}/offloaded_proto.pb", "wb") as f:
        f.write(original_input_literal.to_flyte_idl().SerializeToString())

    offloaded_input_literal = literal_models.Literal(
        offloaded_metadata=literal_models.LiteralOffloadedMetadata(
            uri=f"{tmp_path}/offloaded_proto.pb",
            inferred_type=literal_models.LiteralType(collection_type=SimpleType.INTEGER),
        )
    )

    ctx = context_manager.FlyteContextManager.current_context()
    output_lm = t1.dispatch_execute(
        ctx,
        literal_models.LiteralMap(
            literals={
                "xs": offloaded_input_literal,
            }
        ),
    )
    assert output_lm.literals["o0"] == expected_output_literal
