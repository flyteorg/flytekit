import datetime
import os
import tempfile
import typing
from dataclasses import asdict, dataclass
from datetime import timedelta
from enum import Enum

import pandas as pd
import pyarrow as pa
import pytest
import typing_extensions
from dataclasses_json import DataClassJsonMixin, dataclass_json
from flyteidl.core import errors_pb2
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct
from marshmallow_enum import LoadDumpOptions
from marshmallow_jsonschema import JSONSchema
from pandas._testing import assert_frame_equal

from flytekit import kwtypes
from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import (
    DataclassTransformer,
    DictTransformer,
    ListTransformer,
    LiteralsResolver,
    SimpleTransformer,
    TypeEngine,
    convert_json_schema_to_python_class,
    dataclass_from_dict,
)
from flytekit.exceptions import user as user_exceptions
from flytekit.models import types as model_types
from flytekit.models.annotation import TypeAnnotation
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralCollection, LiteralMap, Primitive, Scalar
from flytekit.models.types import LiteralType, SimpleType
from flytekit.types.directory import TensorboardLogs
from flytekit.types.directory.types import FlyteDirectory
from flytekit.types.file import JPEGImageFile
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer, noop
from flytekit.types.pickle import FlytePickle
from flytekit.types.pickle.pickle import FlytePickleTransformer
from flytekit.types.schema import FlyteSchema
from flytekit.types.structured.structured_dataset import StructuredDataset

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated


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

    assert type(TypeEngine.get_transformer(os.PathLike)) == FlyteFilePathTransformer
    assert type(TypeEngine.get_transformer(FlytePickle)) == FlytePickleTransformer

    with pytest.raises(ValueError):
        TypeEngine.get_transformer(typing.Any)


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


def test_list_of_dict_getting_python_value():
    transformer = TypeEngine.get_transformer(typing.List)
    ctx = FlyteContext.current_context()
    lv = Literal(
        collection=LiteralCollection(
            literals=[Literal(map=LiteralMap({"foo": Literal(scalar=Scalar(primitive=Primitive(integer=1)))}))]
        )
    )

    pv = transformer.to_python_value(ctx, lv, expected_python_type=typing.List[typing.Dict[str, int]])
    assert isinstance(pv, list)


def test_list_of_dataclass_getting_python_value():
    @dataclass_json
    @dataclass()
    class Bar(object):
        w: typing.Optional[str]
        x: float
        y: str
        z: typing.Dict[str, bool]

    @dataclass_json
    @dataclass()
    class Foo(object):
        w: int
        x: typing.List[int]
        y: typing.Dict[str, str]
        z: Bar

    foo = Foo(w=1, x=[1], y={"hello": "10"}, z=Bar(w=None, x=1.0, y="hello", z={"world": False}))
    generic = _json_format.Parse(typing.cast(DataClassJsonMixin, foo).to_json(), _struct.Struct())
    lv = Literal(collection=LiteralCollection(literals=[Literal(scalar=Scalar(generic=generic))]))

    transformer = TypeEngine.get_transformer(typing.List)
    ctx = FlyteContext.current_context()

    schema = JSONSchema().dump(typing.cast(DataClassJsonMixin, Foo).schema())
    foo_class = convert_json_schema_to_python_class(schema["definitions"], "FooSchema")

    pv = transformer.to_python_value(ctx, lv, expected_python_type=typing.List[foo_class])
    assert isinstance(pv, list)
    assert pv[0].w == foo.w
    assert pv[0].x == foo.x
    assert pv[0].y == foo.y
    assert pv[0].z.x == foo.z.x
    assert type(pv[0].z.x) == float
    assert pv[0].z.y == foo.z.y
    assert pv[0].z.z == foo.z.z
    assert foo == dataclass_from_dict(Foo, asdict(pv[0]))


def test_file_no_downloader_default():
    # The idea of this test is to assert that if a FlyteFile is created with no download specified,
    # then it should return the set path itself. This matches if we use open method
    transformer = TypeEngine.get_transformer(FlyteFile)

    ctx = FlyteContext.current_context()
    local_file = "/usr/local/bin/file"

    lv = Literal(
        scalar=Scalar(blob=Blob(metadata=BlobMetadata(type=BlobType(format="", dimensionality=0)), uri=local_file))
    )

    pv = transformer.to_python_value(ctx, lv, expected_python_type=FlyteFile)
    assert isinstance(pv, FlyteFile)
    assert pv.download() == local_file


def test_dir_no_downloader_default():
    # The idea of this test is to assert that if a FlyteFile is created with no download specified,
    # then it should return the set path itself. This matches if we use open method
    transformer = TypeEngine.get_transformer(FlyteDirectory)

    ctx = FlyteContext.current_context()

    local_dir = "/usr/local/bin/"
    lv = Literal(
        scalar=Scalar(blob=Blob(metadata=BlobMetadata(type=BlobType(format="", dimensionality=1)), uri=local_dir))
    )

    pv = transformer.to_python_value(ctx, lv, expected_python_type=FlyteDirectory)
    assert isinstance(pv, FlyteDirectory)
    assert pv.download() == local_dir


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

    ctx = FlyteContext.current_context()

    lit = d.to_literal(ctx, {}, typing.Dict, LiteralType(SimpleType.STRUCT))
    pv = d.to_python_value(ctx, lit, typing.Dict)
    assert pv == {}

    lit_empty = Literal(map=LiteralMap(literals={}))
    pv_empty = d.to_python_value(ctx, lit_empty, typing.Dict[str, str])
    assert pv_empty == {}

    # Literal to python
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


def test_convert_json_schema_to_python_class():
    @dataclass_json
    @dataclass
    class Foo(object):
        x: int
        y: str

    schema = JSONSchema().dump(typing.cast(DataClassJsonMixin, Foo).schema())
    foo_class = convert_json_schema_to_python_class(schema["definitions"], "FooSchema")
    foo = foo_class(x=1, y="hello")
    foo.x = 2
    assert foo.x == 2
    assert foo.y == "hello"
    with pytest.raises(AttributeError):
        _ = foo.c


def test_list_transformer():
    l0 = Literal(scalar=Scalar(primitive=Primitive(integer=3)))
    l1 = Literal(scalar=Scalar(primitive=Primitive(integer=4)))
    lc = LiteralCollection(literals=[l0, l1])
    lit = Literal(collection=lc)

    ctx = FlyteContext.current_context()
    xx = TypeEngine.to_python_value(ctx, lit, typing.List[int])
    assert xx == [3, 4]


def test_protos():
    ctx = FlyteContext.current_context()

    pb = errors_pb2.ContainerError(code="code", message="message")
    lt = TypeEngine.to_literal_type(errors_pb2.ContainerError)
    assert lt.simple == SimpleType.STRUCT
    assert lt.metadata["pb_type"] == "flyteidl.core.errors_pb2.ContainerError"

    lit = TypeEngine.to_literal(ctx, pb, errors_pb2.ContainerError, lt)
    new_python_val = TypeEngine.to_python_value(ctx, lit, errors_pb2.ContainerError)
    assert new_python_val == pb

    # Test error
    l0 = Literal(scalar=Scalar(primitive=Primitive(integer=4)))
    with pytest.raises(AssertionError):
        TypeEngine.to_python_value(ctx, l0, errors_pb2.ContainerError)

    default_proto = errors_pb2.ContainerError()
    lit = TypeEngine.to_literal(ctx, default_proto, errors_pb2.ContainerError, lt)
    assert lit.scalar
    assert lit.scalar.generic is not None
    new_python_val = TypeEngine.to_python_value(ctx, lit, errors_pb2.ContainerError)
    assert new_python_val == default_proto


def test_guessing_basic():
    b = model_types.LiteralType(simple=model_types.SimpleType.BOOLEAN)
    pt = TypeEngine.guess_python_type(b)
    assert pt is bool

    lt = model_types.LiteralType(simple=model_types.SimpleType.INTEGER)
    pt = TypeEngine.guess_python_type(lt)
    assert pt is int

    lt = model_types.LiteralType(simple=model_types.SimpleType.STRING)
    pt = TypeEngine.guess_python_type(lt)
    assert pt is str

    lt = model_types.LiteralType(simple=model_types.SimpleType.DURATION)
    pt = TypeEngine.guess_python_type(lt)
    assert pt is timedelta

    lt = model_types.LiteralType(simple=model_types.SimpleType.DATETIME)
    pt = TypeEngine.guess_python_type(lt)
    assert pt is datetime.datetime

    lt = model_types.LiteralType(simple=model_types.SimpleType.FLOAT)
    pt = TypeEngine.guess_python_type(lt)
    assert pt is float

    lt = model_types.LiteralType(simple=model_types.SimpleType.NONE)
    pt = TypeEngine.guess_python_type(lt)
    assert pt is None

    lt = model_types.LiteralType(
        blob=BlobType(
            format=FlytePickleTransformer.PYTHON_PICKLE_FORMAT, dimensionality=BlobType.BlobDimensionality.SINGLE
        )
    )
    pt = TypeEngine.guess_python_type(lt)
    assert pt is FlytePickle


def test_guessing_containers():
    b = model_types.LiteralType(simple=model_types.SimpleType.BOOLEAN)
    lt = model_types.LiteralType(collection_type=b)
    pt = TypeEngine.guess_python_type(lt)
    assert pt == typing.List[bool]

    dur = model_types.LiteralType(simple=model_types.SimpleType.DURATION)
    lt = model_types.LiteralType(map_value_type=dur)
    pt = TypeEngine.guess_python_type(lt)
    assert pt == typing.Dict[str, timedelta]


def test_zero_floats():
    ctx = FlyteContext.current_context()

    l0 = Literal(scalar=Scalar(primitive=Primitive(integer=0)))
    l1 = Literal(scalar=Scalar(primitive=Primitive(float_value=0.0)))

    assert TypeEngine.to_python_value(ctx, l0, float) == 0
    assert TypeEngine.to_python_value(ctx, l1, float) == 0


@dataclass_json
@dataclass
class InnerStruct(object):
    a: int
    b: typing.Optional[str]
    c: typing.List[int]


@dataclass_json
@dataclass
class TestStruct(object):
    s: InnerStruct
    m: typing.Dict[str, str]


@dataclass_json
@dataclass
class TestStructB(object):
    s: InnerStruct
    m: typing.Dict[int, str]
    n: typing.List[typing.List[int]] = None
    o: typing.Dict[int, typing.Dict[int, int]] = None


@dataclass_json
@dataclass
class TestStructC(object):
    s: InnerStruct
    m: typing.Dict[str, int]


@dataclass_json
@dataclass
class TestStructD(object):
    s: InnerStruct
    m: typing.Dict[str, typing.List[int]]


class UnsupportedSchemaType:
    def __init__(self):
        self._a = "Hello"


@dataclass_json
@dataclass
class UnsupportedNestedStruct(object):
    a: int
    s: UnsupportedSchemaType


def test_dataclass_transformer():
    schema = {
        "$ref": "#/definitions/TeststructSchema",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "definitions": {
            "InnerstructSchema": {
                "additionalProperties": False,
                "properties": {
                    "a": {"title": "a", "type": "integer"},
                    "b": {"default": None, "title": "b", "type": ["string", "null"]},
                    "c": {
                        "items": {"title": "c", "type": "integer"},
                        "title": "c",
                        "type": "array",
                    },
                },
                "type": "object",
            },
            "TeststructSchema": {
                "additionalProperties": False,
                "properties": {
                    "m": {"additionalProperties": {"title": "m", "type": "string"}, "title": "m", "type": "object"},
                    "s": {"$ref": "#/definitions/InnerstructSchema", "field_many": False, "type": "object"},
                },
                "type": "object",
            },
        },
    }
    tf = DataclassTransformer()
    t = tf.get_literal_type(TestStruct)
    assert t is not None
    assert t.simple is not None
    assert t.simple == SimpleType.STRUCT
    assert t.metadata is not None
    assert t.metadata == schema

    t = TypeEngine.to_literal_type(TestStruct)
    assert t is not None
    assert t.simple is not None
    assert t.simple == SimpleType.STRUCT
    assert t.metadata is not None
    assert t.metadata == schema

    t = tf.get_literal_type(UnsupportedNestedStruct)
    assert t is not None
    assert t.simple is not None
    assert t.simple == SimpleType.STRUCT
    assert t.metadata is None


def test_dataclass_int_preserving():
    ctx = FlyteContext.current_context()

    o = InnerStruct(a=5, b=None, c=[1, 2, 3])
    tf = DataclassTransformer()
    lv = tf.to_literal(ctx, o, InnerStruct, tf.get_literal_type(InnerStruct))
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=InnerStruct)
    assert ot == o

    o = TestStructB(
        s=InnerStruct(a=5, b=None, c=[1, 2, 3]), m={5: "b"}, n=[[1, 2, 3], [4, 5, 6]], o={1: {2: 3}, 4: {5: 6}}
    )
    lv = tf.to_literal(ctx, o, TestStructB, tf.get_literal_type(TestStructB))
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=TestStructB)
    assert ot == o

    o = TestStructC(s=InnerStruct(a=5, b=None, c=[1, 2, 3]), m={"a": 5})
    lv = tf.to_literal(ctx, o, TestStructC, tf.get_literal_type(TestStructC))
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=TestStructC)
    assert ot == o

    o = TestStructD(s=InnerStruct(a=5, b=None, c=[1, 2, 3]), m={"a": [5]})
    lv = tf.to_literal(ctx, o, TestStructD, tf.get_literal_type(TestStructD))
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=TestStructD)
    assert ot == o


def test_flyte_file_in_dataclass():
    @dataclass_json
    @dataclass
    class TestInnerFileStruct(object):
        a: JPEGImageFile
        b: typing.List[FlyteFile]
        c: typing.Dict[str, FlyteFile]

    @dataclass_json
    @dataclass
    class TestFileStruct(object):
        a: FlyteFile
        b: TestInnerFileStruct

    f = FlyteFile("s3://tmp/file")
    o = TestFileStruct(a=f, b=TestInnerFileStruct(a=JPEGImageFile("s3://tmp/file.jpeg"), b=[f], c={"hello": f}))

    ctx = FlyteContext.current_context()
    tf = DataclassTransformer()
    lt = tf.get_literal_type(TestFileStruct)
    lv = tf.to_literal(ctx, o, TestFileStruct, lt)
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=TestFileStruct)
    assert ot.a._downloader is not noop
    assert ot.b.a._downloader is not noop
    assert ot.b.b[0]._downloader is not noop
    assert ot.b.c["hello"]._downloader is not noop

    assert o.a.path == ot.a.remote_source
    assert o.b.a.path == ot.b.a.remote_source
    assert o.b.b[0].path == ot.b.b[0].remote_source
    assert o.b.c["hello"].path == ot.b.c["hello"].remote_source


def test_flyte_directory_in_dataclass():
    @dataclass_json
    @dataclass
    class TestInnerFileStruct(object):
        a: TensorboardLogs
        b: typing.List[FlyteDirectory]
        c: typing.Dict[str, FlyteDirectory]

    @dataclass_json
    @dataclass
    class TestFileStruct(object):
        a: FlyteDirectory
        b: TestInnerFileStruct

    tempdir = tempfile.mkdtemp(prefix="flyte-")
    f = FlyteDirectory(tempdir)
    o = TestFileStruct(a=f, b=TestInnerFileStruct(a=TensorboardLogs("s3://tensorboard"), b=[f], c={"hello": f}))

    ctx = FlyteContext.current_context()
    tf = DataclassTransformer()
    lt = tf.get_literal_type(TestFileStruct)
    lv = tf.to_literal(ctx, o, TestFileStruct, lt)
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=TestFileStruct)

    assert ot.a._downloader is not noop
    assert ot.b.a._downloader is not noop
    assert ot.b.b[0]._downloader is not noop
    assert ot.b.c["hello"]._downloader is not noop

    assert o.a.path == ot.a.path
    assert o.b.a.path == ot.b.a.remote_source
    assert o.b.b[0].path == ot.b.b[0].path
    assert o.b.c["hello"].path == ot.b.c["hello"].path


def test_structured_dataset_in_dataclass():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})

    @dataclass_json
    @dataclass
    class InnerDatasetStruct(object):
        a: StructuredDataset

    @dataclass_json
    @dataclass
    class DatasetStruct(object):
        a: StructuredDataset
        b: InnerDatasetStruct

    sd = StructuredDataset(dataframe=df, file_format="parquet")
    o = DatasetStruct(a=sd, b=InnerDatasetStruct(a=sd))

    ctx = FlyteContext.current_context()
    tf = DataclassTransformer()
    lt = tf.get_literal_type(DatasetStruct)
    lv = tf.to_literal(ctx, o, DatasetStruct, lt)
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=DatasetStruct)

    assert_frame_equal(df, ot.a.open(pd.DataFrame).all())
    assert_frame_equal(df, ot.b.a.open(pd.DataFrame).all())
    assert "parquet" == ot.a.file_format
    assert "parquet" == ot.b.a.file_format


# Enums should have string values
class Color(Enum):
    RED = "red"
    GREEN = "green"
    BLUE = "blue"


# Enums with integer values are not supported
class UnsupportedEnumValues(Enum):
    RED = 1
    GREEN = 2
    BLUE = 3


def test_structured_dataset_type():
    name = "Name"
    age = "Age"
    data = {name: ["Tom", "Joseph"], age: [20, 22]}
    superset_cols = kwtypes(Name=str, Age=int)
    subset_cols = kwtypes(Name=str)
    df = pd.DataFrame(data)

    from flytekit.types.structured.structured_dataset import StructuredDataset, StructuredDatasetTransformerEngine

    tf = StructuredDatasetTransformerEngine()
    lt = tf.get_literal_type(Annotated[StructuredDataset, superset_cols, "parquet"])
    assert lt.structured_dataset_type is not None

    ctx = FlyteContextManager.current_context()
    lv = tf.to_literal(ctx, df, pd.DataFrame, lt)
    assert "/tmp/flyte" in lv.scalar.structured_dataset.uri
    metadata = lv.scalar.structured_dataset.metadata
    assert metadata.structured_dataset_type.format == "parquet"
    v1 = tf.to_python_value(ctx, lv, pd.DataFrame)
    v2 = tf.to_python_value(ctx, lv, pa.Table)
    assert_frame_equal(df, v1)
    assert_frame_equal(df, v2.to_pandas())

    subset_lt = tf.get_literal_type(Annotated[StructuredDataset, subset_cols, "parquet"])
    assert subset_lt.structured_dataset_type is not None

    subset_lv = tf.to_literal(ctx, df, pd.DataFrame, subset_lt)
    assert "/tmp/flyte" in subset_lv.scalar.structured_dataset.uri
    v1 = tf.to_python_value(ctx, subset_lv, pd.DataFrame)
    v2 = tf.to_python_value(ctx, subset_lv, pa.Table)
    subset_data = pd.DataFrame({name: ["Tom", "Joseph"]})
    assert_frame_equal(subset_data, v1)
    assert_frame_equal(subset_data, v2.to_pandas())

    empty_lt = tf.get_literal_type(Annotated[StructuredDataset, "parquet"])
    assert empty_lt.structured_dataset_type is not None
    empty_lv = tf.to_literal(ctx, df, pd.DataFrame, empty_lt)
    v1 = tf.to_python_value(ctx, empty_lv, pd.DataFrame)
    v2 = tf.to_python_value(ctx, empty_lv, pa.Table)
    assert_frame_equal(df, v1)
    assert_frame_equal(df, v2.to_pandas())


def test_enum_type():
    t = TypeEngine.to_literal_type(Color)
    assert t is not None
    assert t.enum_type is not None
    assert t.enum_type.values
    assert t.enum_type.values == [c.value for c in Color]

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, Color.RED, Color, TypeEngine.to_literal_type(Color))
    assert lv
    assert lv.scalar
    assert lv.scalar.primitive.string_value == "red"

    v = TypeEngine.to_python_value(ctx, lv, Color)
    assert v
    assert v == Color.RED

    v = TypeEngine.to_python_value(ctx, lv, str)
    assert v
    assert v == "red"

    with pytest.raises(ValueError):
        TypeEngine.to_python_value(ctx, Literal(scalar=Scalar(primitive=Primitive(string_value=str(Color.RED)))), Color)

    with pytest.raises(ValueError):
        TypeEngine.to_python_value(ctx, Literal(scalar=Scalar(primitive=Primitive(string_value="bad"))), Color)

    with pytest.raises(AssertionError):
        TypeEngine.to_literal_type(UnsupportedEnumValues)


def test_pickle_type():
    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    lt = TypeEngine.to_literal_type(FlytePickle)
    assert lt.blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT
    assert lt.blob.dimensionality == BlobType.BlobDimensionality.SINGLE

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, Foo(1), FlytePickle, lt)
    assert "/tmp/flyte/" in lv.scalar.blob.uri

    transformer = FlytePickleTransformer()
    gt = transformer.guess_python_type(lt)
    pv = transformer.to_python_value(ctx, lv, expected_python_type=gt)
    assert Foo(1).number == pv.number


def test_enum_in_dataclass():
    @dataclass_json
    @dataclass
    class Datum(object):
        x: int
        y: Color

    lt = TypeEngine.to_literal_type(Datum)
    schema = Datum.schema()
    schema.fields["y"].load_by = LoadDumpOptions.name
    assert lt.metadata == JSONSchema().dump(schema)

    transformer = DataclassTransformer()
    ctx = FlyteContext.current_context()
    datum = Datum(5, Color.RED)
    lv = transformer.to_literal(ctx, datum, Datum, lt)
    gt = transformer.guess_python_type(lt)
    pv = transformer.to_python_value(ctx, lv, expected_python_type=gt)
    assert datum.x == pv.x
    assert datum.y.value == pv.y


@pytest.mark.parametrize(
    "python_value,python_types,expected_literal_map",
    [
        (
            {"a": [1, 2, 3]},
            {"a": typing.List[int]},
            LiteralMap(
                literals={
                    "a": Literal(
                        collection=LiteralCollection(
                            literals=[
                                Literal(scalar=Scalar(primitive=Primitive(integer=1))),
                                Literal(scalar=Scalar(primitive=Primitive(integer=2))),
                                Literal(scalar=Scalar(primitive=Primitive(integer=3))),
                            ]
                        )
                    )
                }
            ),
        ),
        (
            {"p1": {"k1": "v1", "k2": "2"}},
            {"p1": typing.Dict[str, str]},
            LiteralMap(
                literals={
                    "p1": Literal(
                        map=LiteralMap(
                            literals={
                                "k1": Literal(scalar=Scalar(primitive=Primitive(string_value="v1"))),
                                "k2": Literal(scalar=Scalar(primitive=Primitive(string_value="2"))),
                            },
                        )
                    )
                }
            ),
        ),
        (
            {"p1": TestStructD(s=InnerStruct(a=5, b=None, c=[1, 2, 3]), m={"a": [5]})},
            {"p1": TestStructD},
            LiteralMap(
                literals={
                    "p1": Literal(
                        scalar=Scalar(
                            generic=_json_format.Parse(
                                typing.cast(
                                    DataClassJsonMixin,
                                    TestStructD(s=InnerStruct(a=5, b=None, c=[1, 2, 3]), m={"a": [5]}),
                                ).to_json(),
                                _struct.Struct(),
                            )
                        )
                    )
                }
            ),
        ),
        (
            {"p1": "s3://tmp/file.jpeg"},
            {"p1": JPEGImageFile},
            LiteralMap(
                literals={
                    "p1": Literal(
                        scalar=Scalar(
                            blob=Blob(
                                metadata=BlobMetadata(
                                    type=BlobType(format="jpeg", dimensionality=BlobType.BlobDimensionality.SINGLE)
                                ),
                                uri="s3://tmp/file.jpeg",
                            )
                        )
                    )
                }
            ),
        ),
    ],
)
def test_dict_to_literal_map(python_value, python_types, expected_literal_map):
    ctx = FlyteContext.current_context()

    assert TypeEngine.dict_to_literal_map(ctx, python_value, python_types) == expected_literal_map


def test_dict_to_literal_map_with_wrong_input_type():
    ctx = FlyteContext.current_context()
    input = {"a": 1}
    guessed_python_types = {"a": str}
    with pytest.raises(user_exceptions.FlyteTypeException):
        TypeEngine.dict_to_literal_map(ctx, input, guessed_python_types)


def test_annotated_simple_types():
    def _check_annotation(t, annotation):
        lt = TypeEngine.to_literal_type(t)
        assert isinstance(lt.annotation, TypeAnnotation)
        assert lt.annotation.annotations == annotation

    _check_annotation(typing_extensions.Annotated[int, FlyteAnnotation({"foo": "bar"})], {"foo": "bar"})
    _check_annotation(typing_extensions.Annotated[int, FlyteAnnotation(["foo", "bar"])], ["foo", "bar"])
    _check_annotation(
        typing_extensions.Annotated[int, FlyteAnnotation({"d": {"test": "data"}, "l": ["nested", ["list"]]})],
        {"d": {"test": "data"}, "l": ["nested", ["list"]]},
    )
    _check_annotation(
        typing_extensions.Annotated[int, FlyteAnnotation(InnerStruct(a=1, b="fizz", c=[1]))],
        InnerStruct(a=1, b="fizz", c=[1]),
    )


def test_annotated_list():
    t = typing_extensions.Annotated[typing.List[int], FlyteAnnotation({"foo": "bar"})]
    lt = TypeEngine.to_literal_type(t)
    assert isinstance(lt.annotation, TypeAnnotation)
    assert lt.annotation.annotations == {"foo": "bar"}

    t = typing.List[typing_extensions.Annotated[int, FlyteAnnotation({"foo": "bar"})]]
    lt = TypeEngine.to_literal_type(t)
    assert isinstance(lt.collection_type.annotation, TypeAnnotation)
    assert lt.collection_type.annotation.annotations == {"foo": "bar"}


def test_type_alias():
    inner_t = typing_extensions.Annotated[int, FlyteAnnotation("foo")]
    t = typing_extensions.Annotated[inner_t, FlyteAnnotation("bar")]
    with pytest.raises(ValueError):
        TypeEngine.to_literal_type(t)


def test_unsupported_complex_literals():
    t = typing_extensions.Annotated[typing.Dict[int, str], FlyteAnnotation({"foo": "bar"})]
    with pytest.raises(ValueError):
        TypeEngine.to_literal_type(t)

    # Enum.
    t = typing_extensions.Annotated[Color, FlyteAnnotation({"foo": "bar"})]
    with pytest.raises(ValueError):
        TypeEngine.to_literal_type(t)

    # Dataclass.
    t = typing_extensions.Annotated[Result, FlyteAnnotation({"foo": "bar"})]
    with pytest.raises(ValueError):
        TypeEngine.to_literal_type(t)


def test_multiple_annotations():
    t = typing_extensions.Annotated[int, FlyteAnnotation({"foo": "bar"}), FlyteAnnotation({"anotha": "one"})]
    with pytest.raises(Exception):
        TypeEngine.to_literal_type(t)


TestSchema = FlyteSchema[kwtypes(some_str=str)]


@dataclass_json
@dataclass
class InnerResult:
    number: int
    schema: TestSchema


@dataclass_json
@dataclass
class Result:
    result: InnerResult
    schema: TestSchema


def test_schema_in_dataclass():
    schema = TestSchema()
    df = pd.DataFrame(data={"some_str": ["a", "b", "c"]})
    schema.open().write(df)
    o = Result(result=InnerResult(number=1, schema=schema), schema=schema)
    ctx = FlyteContext.current_context()
    tf = DataclassTransformer()
    lt = tf.get_literal_type(Result)
    lv = tf.to_literal(ctx, o, Result, lt)
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=Result)

    assert o == ot


@pytest.mark.parametrize(
    "literal_value,python_type,expected_python_value",
    [
        (
            Literal(
                collection=LiteralCollection(
                    literals=[
                        Literal(scalar=Scalar(primitive=Primitive(integer=1))),
                        Literal(scalar=Scalar(primitive=Primitive(integer=2))),
                        Literal(scalar=Scalar(primitive=Primitive(integer=3))),
                    ]
                )
            ),
            typing.List[int],
            [1, 2, 3],
        ),
        (
            Literal(
                map=LiteralMap(
                    literals={
                        "k1": Literal(scalar=Scalar(primitive=Primitive(string_value="v1"))),
                        "k2": Literal(scalar=Scalar(primitive=Primitive(string_value="2"))),
                    },
                )
            ),
            typing.Dict[str, str],
            {"k1": "v1", "k2": "2"},
        ),
    ],
)
def test_literals_resolver(literal_value, python_type, expected_python_value):
    lit_dict = {"a": literal_value}

    lr = LiteralsResolver(lit_dict)
    out = lr.get("a", python_type)
    assert out == expected_python_value


def test_guess_of_dataclass():
    @dataclass_json
    @dataclass()
    class Foo(object):
        x: int
        y: str
        z: typing.Dict[str, int]

        def hello(self):
            ...

    lt = TypeEngine.to_literal_type(Foo)
    foo = Foo(1, "hello", {"world": 3})
    lv = TypeEngine.to_literal(FlyteContext.current_context(), foo, Foo, lt)
    lit_dict = {"a": lv}
    lr = LiteralsResolver(lit_dict)
    assert lr.get("a", Foo) == foo
    assert hasattr(lr.get("a", Foo), "hello") is True
