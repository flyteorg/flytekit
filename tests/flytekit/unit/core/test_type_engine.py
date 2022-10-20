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
from typing_extensions import Annotated

from flytekit import kwtypes
from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.data_persistence import flyte_tmp_dir
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.hash import HashMethod
from flytekit.core.task import task
from flytekit.core.type_engine import (
    DataclassTransformer,
    DictTransformer,
    ListTransformer,
    LiteralsResolver,
    SimpleTransformer,
    TypeEngine,
    TypeTransformer,
    TypeTransformerFailedError,
    UnionTransformer,
    convert_json_schema_to_python_class,
    dataclass_from_dict,
)
from flytekit.exceptions import user as user_exceptions
from flytekit.models import types as model_types
from flytekit.models.annotation import TypeAnnotation
from flytekit.models.core.types import BlobType
from flytekit.models.literals import Blob, BlobMetadata, Literal, LiteralCollection, LiteralMap, Primitive, Scalar, Void
from flytekit.models.types import LiteralType, SimpleType, TypeStructure
from flytekit.types.directory import TensorboardLogs
from flytekit.types.directory.types import FlyteDirectory
from flytekit.types.file import JPEGImageFile
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer, noop
from flytekit.types.pickle import FlytePickle
from flytekit.types.pickle.pickle import FlytePickleTransformer
from flytekit.types.schema import FlyteSchema
from flytekit.types.schema.types_pandas import PandasDataFrameTransformer
from flytekit.types.structured.structured_dataset import StructuredDataset

T = typing.TypeVar("T")


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


def test_list_of_single_dataclass():
    @dataclass_json
    @dataclass()
    class Bar(object):
        v: typing.Optional[typing.List[int]]
        w: typing.Optional[typing.List[float]]

    @dataclass_json
    @dataclass()
    class Foo(object):
        a: typing.Optional[typing.List[str]]
        b: Bar

    foo = Foo(a=["abc", "def"], b=Bar(v=[1, 2, 99], w=[3.1415, 2.7182]))
    generic = _json_format.Parse(typing.cast(DataClassJsonMixin, foo).to_json(), _struct.Struct())
    lv = Literal(collection=LiteralCollection(literals=[Literal(scalar=Scalar(generic=generic))]))

    transformer = TypeEngine.get_transformer(typing.List)
    ctx = FlyteContext.current_context()

    pv = transformer.to_python_value(ctx, lv, expected_python_type=typing.List[Foo])
    assert pv[0].a == ["abc", "def"]
    assert pv[0].b == Bar(v=[1, 2, 99], w=[3.1415, 2.7182])


def test_list_of_dataclass_getting_python_value():
    @dataclass_json
    @dataclass()
    class Bar(object):
        v: typing.Union[int, None]
        w: typing.Optional[str]
        x: float
        y: str
        z: typing.Dict[str, bool]

    @dataclass_json
    @dataclass()
    class Foo(object):
        u: typing.Optional[int]
        v: typing.Optional[int]
        w: int
        x: typing.List[int]
        y: typing.Dict[str, str]
        z: Bar

    foo = Foo(u=5, v=None, w=1, x=[1], y={"hello": "10"}, z=Bar(v=3, w=None, x=1.0, y="hello", z={"world": False}))
    generic = _json_format.Parse(typing.cast(DataClassJsonMixin, foo).to_json(), _struct.Struct())
    lv = Literal(collection=LiteralCollection(literals=[Literal(scalar=Scalar(generic=generic))]))

    transformer = TypeEngine.get_transformer(typing.List)
    ctx = FlyteContext.current_context()

    schema = JSONSchema().dump(typing.cast(DataClassJsonMixin, Foo).schema())
    foo_class = convert_json_schema_to_python_class(schema["definitions"], "FooSchema")

    guessed_pv = transformer.to_python_value(ctx, lv, expected_python_type=typing.List[foo_class])
    pv = transformer.to_python_value(ctx, lv, expected_python_type=typing.List[Foo])
    assert isinstance(guessed_pv, list)
    assert guessed_pv[0].u == pv[0].u
    assert guessed_pv[0].v == pv[0].v
    assert guessed_pv[0].w == pv[0].w
    assert guessed_pv[0].x == pv[0].x
    assert guessed_pv[0].y == pv[0].y
    assert guessed_pv[0].z.x == pv[0].z.x
    assert type(guessed_pv[0].u) == int
    assert guessed_pv[0].v is None
    assert type(guessed_pv[0].w) == int
    assert type(guessed_pv[0].z.v) == int
    assert type(guessed_pv[0].z.x) == float
    assert guessed_pv[0].z.v == pv[0].z.v
    assert guessed_pv[0].z.y == pv[0].z.y
    assert guessed_pv[0].z.z == pv[0].z.z
    assert pv[0] == dataclass_from_dict(Foo, asdict(guessed_pv[0]))


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
    assert pt is type(None)  # noqa: E721

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
        d: typing.List[FlyteFile]
        e: typing.Dict[str, FlyteFile]

    @dataclass_json
    @dataclass
    class TestFileStruct(object):
        a: FlyteFile
        b: TestInnerFileStruct

    remote_path = "s3://tmp/file"
    f1 = FlyteFile(remote_path)
    f2 = FlyteFile("/tmp/file")
    f2._remote_source = remote_path
    o = TestFileStruct(
        a=f1,
        b=TestInnerFileStruct(a=JPEGImageFile("s3://tmp/file.jpeg"), b=[f1], c={"hello": f1}, d=[f2], e={"hello": f2}),
    )

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
    assert ot.b.d[0].remote_source == remote_path
    assert not ctx.file_access.is_remote(ot.b.d[0].path)
    assert ot.b.e["hello"].remote_source == remote_path
    assert not ctx.file_access.is_remote(ot.b.e["hello"].path)


def test_flyte_directory_in_dataclass():
    @dataclass_json
    @dataclass
    class TestInnerFileStruct(object):
        a: TensorboardLogs
        b: typing.List[FlyteDirectory]
        c: typing.Dict[str, FlyteDirectory]
        d: typing.List[FlyteDirectory]
        e: typing.Dict[str, FlyteDirectory]

    @dataclass_json
    @dataclass
    class TestFileStruct(object):
        a: FlyteDirectory
        b: TestInnerFileStruct

    remote_path = "s3://tmp/file"
    tempdir = tempfile.mkdtemp(prefix="flyte-")
    f1 = FlyteDirectory(tempdir)
    f1._remote_source = remote_path
    f2 = FlyteDirectory(remote_path)
    o = TestFileStruct(
        a=f1,
        b=TestInnerFileStruct(a=TensorboardLogs("s3://tensorboard"), b=[f1], c={"hello": f1}, d=[f2], e={"hello": f2}),
    )

    ctx = FlyteContext.current_context()
    tf = DataclassTransformer()
    lt = tf.get_literal_type(TestFileStruct)
    lv = tf.to_literal(ctx, o, TestFileStruct, lt)
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=TestFileStruct)

    assert ot.a._downloader is not noop
    assert ot.b.a._downloader is not noop
    assert ot.b.b[0]._downloader is not noop
    assert ot.b.c["hello"]._downloader is not noop

    assert o.a.remote_directory == ot.a.remote_directory
    assert not ctx.file_access.is_remote(ot.a.path)
    assert o.b.a.path == ot.b.a.remote_source
    assert o.b.b[0].remote_directory == ot.b.b[0].remote_directory
    assert not ctx.file_access.is_remote(ot.b.b[0].path)
    assert o.b.c["hello"].remote_directory == ot.b.c["hello"].remote_directory
    assert not ctx.file_access.is_remote(ot.b.c["hello"].path)
    assert o.b.d[0].path == ot.b.d[0].remote_source
    assert o.b.e["hello"].path == ot.b.e["hello"].remote_source


def test_structured_dataset_in_dataclass():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})

    @dataclass_json
    @dataclass
    class InnerDatasetStruct(object):
        a: StructuredDataset
        b: typing.List[StructuredDataset]
        c: typing.Dict[str, StructuredDataset]

    @dataclass_json
    @dataclass
    class DatasetStruct(object):
        a: StructuredDataset
        b: InnerDatasetStruct

    sd = StructuredDataset(dataframe=df, file_format="parquet")
    o = DatasetStruct(a=sd, b=InnerDatasetStruct(a=sd, b=[sd], c={"hello": sd}))

    ctx = FlyteContext.current_context()
    tf = DataclassTransformer()
    lt = tf.get_literal_type(DatasetStruct)
    lv = tf.to_literal(ctx, o, DatasetStruct, lt)
    ot = tf.to_python_value(ctx, lv=lv, expected_python_type=DatasetStruct)

    assert_frame_equal(df, ot.a.open(pd.DataFrame).all())
    assert_frame_equal(df, ot.b.a.open(pd.DataFrame).all())
    assert_frame_equal(df, ot.b.b[0].open(pd.DataFrame).all())
    assert_frame_equal(df, ot.b.c["hello"].open(pd.DataFrame).all())
    assert "parquet" == ot.a.file_format
    assert "parquet" == ot.b.a.file_format
    assert "parquet" == ot.b.b[0].file_format
    assert "parquet" == ot.b.c["hello"].file_format


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
    assert flyte_tmp_dir in lv.scalar.structured_dataset.uri
    metadata = lv.scalar.structured_dataset.metadata
    assert metadata.structured_dataset_type.format == "parquet"
    v1 = tf.to_python_value(ctx, lv, pd.DataFrame)
    v2 = tf.to_python_value(ctx, lv, pa.Table)
    assert_frame_equal(df, v1)
    assert_frame_equal(df, v2.to_pandas())

    subset_lt = tf.get_literal_type(Annotated[StructuredDataset, subset_cols, "parquet"])
    assert subset_lt.structured_dataset_type is not None

    subset_lv = tf.to_literal(ctx, df, pd.DataFrame, subset_lt)
    assert flyte_tmp_dir in subset_lv.scalar.structured_dataset.uri
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


def union_type_tags_unique(t: LiteralType):
    seen = set()
    for x in t.union_type.variants:
        if x.structure.tag in seen:
            return False
        seen.add(x.structure.tag)

    return True


def test_union_type():
    pt = typing.Union[str, int]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.STRING, structure=TypeStructure(tag="str")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
    ]
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 3, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "int"
    assert lv.scalar.union.value.scalar.primitive.integer == 3
    assert v == 3

    lv = TypeEngine.to_literal(ctx, "hello", pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "str"
    assert lv.scalar.union.value.scalar.primitive.string_value == "hello"
    assert v == "hello"


def test_assert_dataclass_type():
    @dataclass_json
    @dataclass
    class Args(object):
        x: int
        y: typing.Optional[str]

    @dataclass_json
    @dataclass
    class Schema(object):
        x: typing.Optional[Args] = None

    pt = Schema
    lt = TypeEngine.to_literal_type(pt)
    gt = TypeEngine.guess_python_type(lt)
    pv = Schema(x=Args(x=3, y="hello"))
    DataclassTransformer().assert_type(gt, pv)
    DataclassTransformer().assert_type(Schema, pv)

    @dataclass_json
    @dataclass
    class Bar(object):
        x: int

    pv = Bar(x=3)
    with pytest.raises(
        TypeTransformerFailedError, match="Type of Val '<class 'int'>' is not an instance of <class 'types.ArgsSchema'>"
    ):
        DataclassTransformer().assert_type(gt, pv)


def test_union_transformer():
    assert UnionTransformer.is_optional_type(typing.Optional[int])
    assert not UnionTransformer.is_optional_type(str)
    assert UnionTransformer.get_sub_type_in_optional(typing.Optional[int]) == int


def test_union_type_with_annotated():
    pt = typing.Union[
        Annotated[str, FlyteAnnotation({"hello": "world"})], Annotated[int, FlyteAnnotation({"test": 123})]
    ]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(
            simple=SimpleType.STRING, structure=TypeStructure(tag="str"), annotation=TypeAnnotation({"hello": "world"})
        ),
        LiteralType(
            simple=SimpleType.INTEGER, structure=TypeStructure(tag="int"), annotation=TypeAnnotation({"test": 123})
        ),
    ]
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 3, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "int"
    assert lv.scalar.union.value.scalar.primitive.integer == 3
    assert v == 3

    lv = TypeEngine.to_literal(ctx, "hello", pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "str"
    assert lv.scalar.union.value.scalar.primitive.string_value == "hello"
    assert v == "hello"


def test_annotated_union_type():
    pt = Annotated[typing.Union[str, int], FlyteAnnotation({"hello": "world"})]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.STRING, structure=TypeStructure(tag="str")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
    ]
    assert lt.annotation == TypeAnnotation({"hello": "world"})
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 3, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "int"
    assert lv.scalar.union.value.scalar.primitive.integer == 3
    assert v == 3

    lv = TypeEngine.to_literal(ctx, "hello", pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "str"
    assert lv.scalar.union.value.scalar.primitive.string_value == "hello"
    assert v == "hello"


def test_optional_type():
    pt = typing.Optional[int]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
        LiteralType(simple=SimpleType.NONE, structure=TypeStructure(tag="none")),
    ]
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 3, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "int"
    assert lv.scalar.union.value.scalar.primitive.integer == 3
    assert v == 3

    lv = TypeEngine.to_literal(ctx, None, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "none"
    assert lv.scalar.union.value.scalar.none_type == Void()
    assert v is None


def test_union_from_unambiguous_literal():
    pt = typing.Union[str, int]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.STRING, structure=TypeStructure(tag="str")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
    ]
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 3, int, LiteralType(simple=SimpleType.INTEGER))
    assert lv.scalar.primitive.integer == 3

    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert v == 3


def test_union_custom_transformer():
    class MyInt:
        def __init__(self, x: int):
            self.val = x

        def __eq__(self, other):
            if not isinstance(other, MyInt):
                return False
            return other.val == self.val

    TypeEngine.register(
        SimpleTransformer(
            "MyInt",
            MyInt,
            LiteralType(simple=SimpleType.INTEGER),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(integer=x.val))),
            lambda x: MyInt(x.scalar.primitive.integer),
        )
    )

    pt = typing.Union[int, MyInt]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="MyInt")),
    ]
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 3, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "int"
    assert lv.scalar.union.value.scalar.primitive.integer == 3
    assert v == 3

    lv = TypeEngine.to_literal(ctx, MyInt(10), pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "MyInt"
    assert lv.scalar.union.value.scalar.primitive.integer == 10
    assert v == MyInt(10)

    lv = TypeEngine.to_literal(ctx, 4, int, LiteralType(simple=SimpleType.INTEGER))
    assert lv.scalar.primitive.integer == 4
    try:
        TypeEngine.to_python_value(ctx, lv, pt)
    except TypeError as e:
        assert "Ambiguous choice of variant" in str(e)

    del TypeEngine._REGISTRY[MyInt]


def test_union_custom_transformer_sanity_check():
    class UnsignedInt:
        def __init__(self, x: int):
            self.val = x

        def __eq__(self, other):
            if not isinstance(other, UnsignedInt):
                return False
            return other.val == self.val

    # This transformer will not work in the implicit wrapping case
    class UnsignedIntTransformer(TypeTransformer[UnsignedInt]):
        def __init__(self):
            super().__init__("UnsignedInt", UnsignedInt)

        def get_literal_type(self, t: typing.Type[T]) -> LiteralType:
            return LiteralType(simple=SimpleType.INTEGER)

        def to_literal(
            self, ctx: FlyteContext, python_val: T, python_type: typing.Type[T], expected: LiteralType
        ) -> Literal:
            if type(python_val) != int:
                raise TypeTransformerFailedError("Expected an integer")

            if python_val < 0:
                raise TypeTransformerFailedError("Expected a non-negative integer")

            return Literal(scalar=Scalar(primitive=Primitive(integer=python_val)))

        def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: typing.Type[T]) -> T:
            val = lv.scalar.primitive.integer
            return UnsignedInt(0 if val < 0 else val)

    TypeEngine.register(UnsignedIntTransformer())

    pt = typing.Union[int, UnsignedInt]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="UnsignedInt")),
    ]
    assert union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    with pytest.raises(TypeError, match="Ambiguous choice of variant for union type"):
        TypeEngine.to_literal(ctx, 3, pt, lt)

    del TypeEngine._REGISTRY[UnsignedInt]


def test_union_of_lists():
    pt = typing.Union[typing.List[int], typing.List[str]]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.union_type.variants == [
        LiteralType(
            collection_type=LiteralType(simple=SimpleType.INTEGER),
            structure=TypeStructure(tag="Typed List"),
        ),
        LiteralType(
            collection_type=LiteralType(simple=SimpleType.STRING),
            structure=TypeStructure(tag="Typed List"),
        ),
    ]
    # Tags are deliberately NOT unique beacuse they are not required to encode the deep type structure,
    # only the top-level type transformer choice
    #
    # The stored typed will be used to differentiate union variants and must produce a unique choice.
    assert not union_type_tags_unique(lt)

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, ["hello", "world"], pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "Typed List"
    assert [x.scalar.primitive.string_value for x in lv.scalar.union.value.collection.literals] == ["hello", "world"]
    assert v == ["hello", "world"]

    lv = TypeEngine.to_literal(ctx, [1, 3], pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert lv.scalar.union.stored_type.structure.tag == "Typed List"
    assert [x.scalar.primitive.integer for x in lv.scalar.union.value.collection.literals] == [1, 3]
    assert v == [1, 3]


def test_list_of_unions():
    pt = typing.List[typing.Union[str, int]]
    lt = TypeEngine.to_literal_type(pt)
    # todo(maximsmol): seems like the order here is non-deterministic
    assert lt.collection_type.union_type.variants == [
        LiteralType(simple=SimpleType.STRING, structure=TypeStructure(tag="str")),
        LiteralType(simple=SimpleType.INTEGER, structure=TypeStructure(tag="int")),
    ]
    assert union_type_tags_unique(lt.collection_type)  # tags are deliberately NOT unique

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, ["hello", 123, "world"], pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert [x.scalar.union.stored_type.structure.tag for x in lv.collection.literals] == ["str", "int", "str"]
    assert v == ["hello", 123, "world"]


def test_pickle_type():
    class Foo(object):
        def __init__(self, number: int):
            self.number = number

    lt = TypeEngine.to_literal_type(FlytePickle)
    assert lt.blob.format == FlytePickleTransformer.PYTHON_PICKLE_FORMAT
    assert lt.blob.dimensionality == BlobType.BlobDimensionality.SINGLE

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, Foo(1), FlytePickle, lt)
    assert flyte_tmp_dir in lv.scalar.blob.uri

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


def test_nested_annotated():
    """
    Test to show that nested Annotated types are flattened.
    """
    pt = Annotated[Annotated[int, "inner-annotation"], "outer-annotation"]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.simple == model_types.SimpleType.INTEGER

    ctx = FlyteContextManager.current_context()
    lv = TypeEngine.to_literal(ctx, 42, pt, lt)
    v = TypeEngine.to_python_value(ctx, lv, pt)
    assert v == 42


def test_pass_annotated_to_downstream_tasks():
    """
    Test to confirm that the loaded dataframe is not affected and can be used in @dynamic.
    """

    # pandas dataframe hash function
    def hash_pandas_dataframe(df: pd.DataFrame) -> str:
        return str(pd.util.hash_pandas_object(df))

    @task
    def t0(a: int) -> Annotated[int, HashMethod(function=str)]:
        return a + 1

    @task
    def annotated_return_task() -> Annotated[pd.DataFrame, HashMethod(hash_pandas_dataframe)]:
        return pd.DataFrame({"column_1": [1, 2, 3]})

    @task(cache=True, cache_version="42")
    def downstream_t(a: int, df: pd.DataFrame) -> int:
        return a + 2 + len(df)

    @dynamic
    def t1(a: int) -> int:
        v = t0(a=a)
        df = annotated_return_task()

        # We should have a cache miss in the first call to downstream_t
        v_1 = downstream_t(a=v, df=df)
        downstream_t(a=v, df=df)

        return v_1

    assert t1(a=3) == 9


def test_literal_hash_int_can_be_set():
    """
    Test to confirm that annotating an integer with `HashMethod` is allowed.
    """
    ctx = FlyteContext.current_context()
    lv = TypeEngine.to_literal(
        ctx, 42, Annotated[int, HashMethod(str)], LiteralType(simple=model_types.SimpleType.INTEGER)
    )
    assert lv.scalar.primitive.integer == 42
    assert lv.hash == "42"


def test_literal_hash_to_python_value():
    """
    Test to confirm that literals can be converted to python values, regardless of the hash value set in the literal.
    """
    ctx = FlyteContext.current_context()

    def constant_hash(df: pd.DataFrame) -> str:
        return "h4Sh"

    df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    pandas_df_transformer = PandasDataFrameTransformer()
    literal_with_hash_set = TypeEngine.to_literal(
        ctx,
        df,
        Annotated[pd.DataFrame, HashMethod(constant_hash)],
        pandas_df_transformer.get_literal_type(pd.DataFrame),
    )
    assert literal_with_hash_set.hash == "h4Sh"
    # Confirm tha the loaded dataframe is not affected
    python_df = TypeEngine.to_python_value(ctx, literal_with_hash_set, pd.DataFrame)
    expected_df = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    assert expected_df.equals(python_df)


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
