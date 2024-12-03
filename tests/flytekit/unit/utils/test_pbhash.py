import tempfile
import mock
import pytest
import typing
from dataclasses import dataclass, field
from google.protobuf import json_format
from google.protobuf import struct_pb2
from dataclasses_json import DataClassJsonMixin

from flyteidl.core.literals_pb2 import Blob, BlobMetadata, Literal, LiteralCollection, LiteralMap, Primitive, Scalar
from flyteidl.core.types_pb2 import BlobType
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import DataclassTransformer
from flytekit.types.file.file import FlyteFile
from flytekit.utils.pbhash import compute_hash_string


@pytest.mark.parametrize(
    "lit, expected_hash",
    [
        (
            Literal(scalar=Scalar(primitive=Primitive(integer=1))),
            "aJB6fp0kDrfAZt22e/IFnT8IJIlobjxcweiZA8I7/dA=",
        ),
        (
            Literal(collection=LiteralCollection(literals=[Literal(scalar=Scalar(primitive=Primitive(integer=1)))])),
            "qN7iA0GnbLzFGcHB7y09lbxgx+9cTIViSlyL9/kCSC0=",
        ),
        (
            Literal(map=LiteralMap(literals={"a": Literal(scalar=Scalar(primitive=Primitive(integer=1)))})),
            "JhrkdOQ+xzPVNYiKzD5sHhZprQB5Nq1GsYUVbmLAswU=",
        ),
        (
            Literal(
                scalar=Scalar(
                    blob=Blob(
                        uri="s3://my-bucket/my-key",
                        metadata=BlobMetadata(
                            type=BlobType(
                                format="PythonPickle",
                                dimensionality=BlobType.BlobDimensionality.SINGLE,
                            ),
                        ),
                    ),
                ),
            ),
            "KdNNbLBYoamXYLz8SBuJd/kVDPxO4gVGdNQl61qeTfA=",
        ),
        (
            Literal(
                scalar=Scalar(
                    blob=Blob(
                        metadata=BlobMetadata(
                            type=BlobType(
                                format="PythonPickle",
                                dimensionality=BlobType.BlobDimensionality.SINGLE,
                            ),
                        ),
                        uri="s3://my-bucket/my-key",
                    ),
                ),
            ),
            "KdNNbLBYoamXYLz8SBuJd/kVDPxO4gVGdNQl61qeTfA=",
        ),
        (
            # Literal collection
            Literal(
                collection=LiteralCollection(
                    literals=[
                        Literal(
                            scalar=Scalar(
                                blob=Blob(
                                    metadata=BlobMetadata(
                                        type=BlobType(
                                            format="PythonPickle",
                                            dimensionality=BlobType.BlobDimensionality.SINGLE,
                                        ),
                                    ),
                                    uri="s3://my-bucket/my-key",
                                ),
                            ),
                        ),
                    ],
                ),
            ),
            "RauoCNnZfCSHgcmMKVugozLAcssq/mWdMjbGanRJufI=",
        )
    ],
)
def test_direct_literals(lit, expected_hash):
    assert compute_hash_string(lit) == expected_hash


@mock.patch("flytekit.core.data_persistence.FileAccessProvider.async_put_data")
def test_dataclass_literals(mock_put_data):

    @dataclass
    class A(DataClassJsonMixin):
        a: int

    @dataclass
    class TestFileStruct(DataClassJsonMixin):
        a: FlyteFile
        b: typing.Optional[FlyteFile]
        b_prime: typing.Optional[FlyteFile]
        c: typing.Union[FlyteFile, None]
        d: typing.List[FlyteFile]
        e: typing.List[typing.Optional[FlyteFile]]
        e_prime: typing.List[typing.Optional[FlyteFile]]
        f: typing.Dict[str, FlyteFile]
        g: typing.Dict[str, typing.Optional[FlyteFile]]
        g_prime: typing.Dict[str, typing.Optional[FlyteFile]]
        h: typing.Optional[FlyteFile] = None
        h_prime: typing.Optional[FlyteFile] = None
        i: typing.Optional[A] = None
        i_prime: typing.Optional[A] = field(default_factory=lambda: A(a=99))

    remote_path = "s3://tmp/file"
    mock_put_data.return_value = remote_path

    with tempfile.TemporaryFile() as f:
        f.write(b"abc")
        f1 = FlyteFile("f1", remote_path=remote_path)
        o = TestFileStruct(
            a=f1,
            b=f1,
            b_prime=None,
            c=f1,
            d=[f1],
            e=[f1],
            e_prime=[None],
            f={"a": f1},
            g={"a": f1},
            g_prime={"a": None},
            h=f1,
            i=A(a=42),
        )

        ctx = FlyteContext.current_context()
        tf = DataclassTransformer()
        lt = tf.get_literal_type(TestFileStruct)
        lv = tf.to_literal(ctx, o, TestFileStruct, lt)

        assert compute_hash_string(lv.to_flyte_idl()) == "Hp/cWul3sBI5r8XKdVzAlvNBJ4OSX9L2d/SADI8+YOY="
