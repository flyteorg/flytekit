import datetime
import os
import typing
from datetime import timedelta

import pytest

from flytekit import kwtypes
from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.type_engine import (
    DictTransformer,
    ListTransformer,
    PathLikeTransformer,
    SimpleTransformer,
    TypeEngine,
)
from flytekit.models import types as model_types
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralMap, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType
from flytekit.types.flyte_file import FlyteFile
from flytekit.types.schema import FlyteSchema, SchemaFormat


def test_type_engine():
    t = int
    lt = TypeEngine.to_literal_type(t)
    assert lt.simple == model_types.SimpleType.INTEGER

    t = typing.Dict[str, typing.List[typing.Dict[str, timedelta]]]
    lt = TypeEngine.to_literal_type(t)
    assert lt.map_value_type.collection_type.map_value_type.simple == model_types.SimpleType.DURATION


def test_named_tuple():
    t = typing.NamedTuple("Outputs", [("x_str", str), ("y_int", int)])
    var_map = TypeEngine.named_tuple_to_variable_map(t)
    assert var_map.variables["x_str"].type.simple == model_types.SimpleType.STRING
    assert var_map.variables["y_int"].type.simple == model_types.SimpleType.INTEGER


def test_type_resolution():
    assert type(TypeEngine.get_transformer(typing.List[int])) == ListTransformer
    assert type(TypeEngine.get_transformer(typing.List)) == ListTransformer
    assert type(TypeEngine.get_transformer(list)) == ListTransformer

    assert type(TypeEngine.get_transformer(typing.Dict[str, int])) == DictTransformer
    assert type(TypeEngine.get_transformer(typing.Dict)) == DictTransformer
    assert type(TypeEngine.get_transformer(dict)) == DictTransformer

    assert type(TypeEngine.get_transformer(int)) == SimpleTransformer

    assert type(TypeEngine.get_transformer(os.PathLike)) == PathLikeTransformer


def test_file_formats_getting_literal_type():
    transformer = TypeEngine.get_transformer(FlyteFile)

    lt = transformer.get_literal_type(FlyteFile)
    assert lt.blob.format == ""

    # Works with formats that we define
    lt = transformer.get_literal_type(FlyteFile["txt"])
    assert lt.blob.format == "txt"

    lt = transformer.get_literal_type(FlyteFile[typing.TypeVar("jpg")])
    assert lt.blob.format == "jpg"

    # Empty default to the default
    lt = transformer.get_literal_type(FlyteFile)
    assert lt.blob.format == ""

    lt = transformer.get_literal_type(FlyteFile[typing.TypeVar(".png")])
    assert lt.blob.format == "png"


def test_file_format_getting_python_value():
    transformer = TypeEngine.get_transformer(FlyteFile)

    ctx = FlyteContext.current_context()

    # This file probably won't exist, but it's okay. It won't be downloaded unless we try to read the thing returned
    lv = Literal(
        scalar=Scalar(
            blob=Blob(metadata=BlobMetadata(type=BlobType(format="txt", dimensionality=0)), uri="file:///tmp/test")
        )
    )

    pv = transformer.to_python_value(ctx, lv, expected_python_type=FlyteFile["txt"])
    assert isinstance(pv, FlyteFile)
    assert pv.extension() == "txt"


def test_typed_schema():
    s = FlyteSchema[kwtypes(x=int, y=float)]
    assert s.format() == SchemaFormat.PARQUET
    assert s.columns() == {"x": int, "y": float}


def test_dict_transformer():
    d = DictTransformer()

    def assert_struct(lit: LiteralType):
        assert lit is not None
        assert lit.simple == SimpleType.STRUCT

    def recursive_assert(lit: LiteralType, expected: LiteralType, expected_depth: int = 1, curr_depth: int = 0):
        assert curr_depth <= expected_depth
        assert lit is not None
        if lit.map_value_type is None:
            assert lit == expected
            return
        recursive_assert(lit.map_value_type, expected, expected_depth, curr_depth + 1)

    # Type inference
    assert_struct(d.get_literal_type(dict))
    assert_struct(d.get_literal_type(typing.Dict[int, int]))
    recursive_assert(d.get_literal_type(typing.Dict[str, str]), LiteralType(simple=SimpleType.STRING))
    recursive_assert(d.get_literal_type(typing.Dict[str, int]), LiteralType(simple=SimpleType.INTEGER))
    recursive_assert(d.get_literal_type(typing.Dict[str, datetime.datetime]), LiteralType(simple=SimpleType.DATETIME))
    recursive_assert(d.get_literal_type(typing.Dict[str, datetime.timedelta]), LiteralType(simple=SimpleType.DURATION))
    recursive_assert(d.get_literal_type(typing.Dict[str, dict]), LiteralType(simple=SimpleType.STRUCT))
    recursive_assert(
        d.get_literal_type(typing.Dict[str, typing.Dict[str, str]]),
        LiteralType(simple=SimpleType.STRING),
        expected_depth=2,
    )
    recursive_assert(
        d.get_literal_type(typing.Dict[str, typing.Dict[int, str]]),
        LiteralType(simple=SimpleType.STRUCT),
        expected_depth=2,
    )
    recursive_assert(
        d.get_literal_type(typing.Dict[str, typing.Dict[str, typing.Dict[str, str]]]),
        LiteralType(simple=SimpleType.STRING),
        expected_depth=3,
    )
    recursive_assert(
        d.get_literal_type(typing.Dict[str, typing.Dict[str, typing.Dict[str, dict]]]),
        LiteralType(simple=SimpleType.STRUCT),
        expected_depth=3,
    )
    recursive_assert(
        d.get_literal_type(typing.Dict[str, typing.Dict[str, typing.Dict[int, dict]]]),
        LiteralType(simple=SimpleType.STRUCT),
        expected_depth=2,
    )

    # Literal to python
    ctx = FlyteContext.current_context()
    with pytest.raises(TypeError):
        d.to_python_value(ctx, Literal(scalar=Scalar(primitive=Primitive(integer=10))), dict)
    with pytest.raises(TypeError):
        d.to_python_value(ctx, Literal(), dict)
    with pytest.raises(TypeError):
        d.to_python_value(ctx, Literal(map=LiteralMap(literals={"x": None})), dict)
    with pytest.raises(TypeError):
        d.to_python_value(ctx, Literal(map=LiteralMap(literals={"x": None})), typing.Dict[int, str])

    d.to_python_value(
        ctx,
        Literal(map=LiteralMap(literals={"x": Literal(scalar=Scalar(primitive=Primitive(integer=1)))})),
        typing.Dict[str, int],
    )

    lit = d.to_literal(ctx, {}, typing.Dict, LiteralType(SimpleType.STRUCT))
    pv = d.to_python_value(ctx, lit, typing.Dict)
    assert pv == {}
