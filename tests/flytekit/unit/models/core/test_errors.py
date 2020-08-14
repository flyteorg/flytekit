from __future__ import absolute_import
from flytekit.models.core import errors


def test_container_error():
    obj = errors.ContainerError(
        "code", "my message", errors.ContainerError.Kind.RECOVERABLE
    )
    assert obj.code == "code"
    assert obj.message == "my message"
    assert obj.kind == errors.ContainerError.Kind.RECOVERABLE

    obj2 = errors.ContainerError.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.code == "code"
    assert obj2.message == "my message"
    assert obj2.kind == errors.ContainerError.Kind.RECOVERABLE


def test_error_document():
    ce = errors.ContainerError(
        "code", "my message", errors.ContainerError.Kind.RECOVERABLE
    )
    obj = errors.ErrorDocument(ce)
    assert obj.error == ce

    obj2 = errors.ErrorDocument.from_flyte_idl(obj.to_flyte_idl())
    assert obj == obj2
    assert obj2.error == ce
