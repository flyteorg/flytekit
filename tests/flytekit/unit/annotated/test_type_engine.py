import os
import typing
from datetime import timedelta

from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.task import kwtypes
from flytekit.annotated.type_engine import (
    DictTransformer,
    FlyteSchema,
    ListTransformer,
    PathLikeTransformer,
    SimpleTransformer,
    TypeEngine,
)
from flytekit.models import types as model_types
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.typing import FlyteFilePath


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
    transformer = TypeEngine.get_transformer(FlyteFilePath)

    lt = transformer.get_literal_type(FlyteFilePath)
    assert lt.blob.format == ""

    # Works with formats that we define
    lt = transformer.get_literal_type(FlyteFilePath["txt"])
    assert lt.blob.format == ".txt"

    lt = transformer.get_literal_type(FlyteFilePath["jpg"])
    assert lt.blob.format == ".jpg"

    # Empty default to the default
    lt = transformer.get_literal_type(FlyteFilePath)
    assert lt.blob.format == ""

    lt = transformer.get_literal_type(FlyteFilePath[".png"])
    assert lt.blob.format == ".png"


def test_file_format_getting_python_value():
    transformer = TypeEngine.get_transformer(FlyteFilePath)

    ctx = FlyteContext.current_context()

    # This file probably won't exist, but it's okay. It won't be downloaded unless we try to read the thing returned
    lv = Literal(
        scalar=Scalar(
            blob=Blob(metadata=BlobMetadata(type=BlobType(format="txt", dimensionality=0)), uri="file:///tmp/test")
        )
    )

    pv = transformer.to_python_value(ctx, lv, expected_python_type=FlyteFilePath[".txt"])
    assert isinstance(pv, FlyteFilePath)
    assert pv.extension() == ".txt"


def test_typed_schema():
    s = FlyteSchema[kwtypes(x=int, y=float)]
    assert s.format() == FlyteSchema.FlyteSchemaFormat.PARQUET
    assert s.columns() == {"x": int, "y": float}
