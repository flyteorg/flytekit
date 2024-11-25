from flytekit.models.core import errors, execution
from google.protobuf.timestamp_pb2 import Timestamp

def test_container_error():
    obj = errors.ContainerError(
        "code", "my message", errors.ContainerError.Kind.RECOVERABLE, execution.ExecutionError.ErrorKind.SYSTEM
    )
    assert obj.code == "code"
    assert obj.message == "my message"
    assert obj.kind == errors.ContainerError.Kind.RECOVERABLE
    assert obj.origin == 2
    assert obj.timestamp == Timestamp(seconds=0, nanos=0)
    assert obj.worker == ""

    converted_obj = errors.ContainerError.from_flyte_idl(obj.to_flyte_idl())
    assert obj == converted_obj
    assert converted_obj.code == "code"
    assert converted_obj.message == "my message"
    assert converted_obj.kind == errors.ContainerError.Kind.RECOVERABLE
    assert converted_obj.origin == 2
    assert converted_obj.timestamp == Timestamp(seconds=0, nanos=0)
    assert converted_obj.worker == ""

    obj1 = errors.ContainerError(
        "code", "my message", errors.ContainerError.Kind.RECOVERABLE, execution.ExecutionError.ErrorKind.SYSTEM,
        timestamp=Timestamp(seconds=10, nanos=20), worker="my_worker"
    )
    assert obj1.code == "code"
    assert obj1.message == "my message"
    assert obj1.kind == errors.ContainerError.Kind.RECOVERABLE
    assert obj1.origin == 2
    assert obj1.timestamp == Timestamp(seconds=10, nanos=20)
    assert obj1.worker == "my_worker"

    converted_obj1 = errors.ContainerError.from_flyte_idl(obj1.to_flyte_idl())
    assert obj1 == converted_obj1
    assert converted_obj1.code == "code"
    assert converted_obj1.message == "my message"
    assert converted_obj1.kind == errors.ContainerError.Kind.RECOVERABLE
    assert converted_obj1.origin == 2
    assert converted_obj1.timestamp == Timestamp(seconds=10, nanos=20)
    assert converted_obj1.worker == "my_worker"


def test_error_document():
    ce = errors.ContainerError(
        "code", "my message", errors.ContainerError.Kind.RECOVERABLE, execution.ExecutionError.ErrorKind.USER
    )
    obj = errors.ErrorDocument(ce)
    assert obj.error == ce

    obj2 = errors.ErrorDocument.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.error == ce
