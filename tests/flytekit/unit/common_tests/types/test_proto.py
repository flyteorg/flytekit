import base64 as _base64

import pytest as _pytest
from flyteidl.core import errors_pb2 as _errors_pb2

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import proto as _proto
from flytekit.common.types.proto import ProtobufType
from flytekit.models import types as _type_models


def test_wrong_type():
    with _pytest.raises(_user_exceptions.FlyteTypeException):
        _proto.create_protobuf(int)


def test_proto_to_literal_type():
    proto_type = _proto.create_protobuf(_errors_pb2.ContainerError)
    assert proto_type.to_flyte_literal_type().simple == _type_models.SimpleType.BINARY
    assert len(proto_type.to_flyte_literal_type().metadata) == 1
    assert (
        proto_type.to_flyte_literal_type().metadata[_proto.Protobuf.PB_FIELD_KEY]
        == "flyteidl.core.errors_pb2.ContainerError"
    )


def test_proto():
    proto_type = _proto.create_protobuf(_errors_pb2.ContainerError)
    run_test_proto_type(proto_type)


def test_generic_proto():
    proto_type = _proto.create_generic(_errors_pb2.ContainerError)
    run_test_proto_type(proto_type)


def run_test_proto_type(proto_type: ProtobufType):
    assert proto_type.short_class_string() == "Types.Proto(flyteidl.core.errors_pb2.ContainerError)"

    pb = _errors_pb2.ContainerError(code="code", message="message")
    obj = proto_type.from_python_std(pb)
    obj2 = proto_type.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2

    obj = obj.to_python_std()
    obj2 = obj2.to_python_std()

    assert obj.code == "code"
    assert obj.message == "message"

    assert obj2.code == "code"
    assert obj2.message == "message"


def test_from_string():
    proto_type = _proto.create_protobuf(_errors_pb2.ContainerError)

    pb = _errors_pb2.ContainerError(code="code", message="message")
    pb_str = _base64.b64encode(pb.SerializeToString())

    obj = proto_type.from_string(pb_str)
    assert obj.to_python_std().code == "code"
    assert obj.to_python_std().message == "message"
