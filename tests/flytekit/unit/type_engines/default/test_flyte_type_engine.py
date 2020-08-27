from __future__ import absolute_import

import pytest
from flyteidl.core import errors_pb2 as _errors_pb2

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import proto as _proto
from flytekit.models import literals as _literal_models
from flytekit.models import types as _type_models
from flytekit.type_engines.default import flyte as _flyte_engine


def test_proto_from_literal_type():
    sdk_type = _flyte_engine.FlyteDefaultTypeEngine().get_sdk_type_from_literal_type(
        _type_models.LiteralType(
            simple=_type_models.SimpleType.BINARY,
            metadata={_proto.Protobuf.PB_FIELD_KEY: "flyteidl.core.errors_pb2.ContainerError"},
        )
    )

    assert sdk_type.pb_type == _errors_pb2.ContainerError


def test_unloadable_module_from_literal_type():
    with pytest.raises(_user_exceptions.FlyteAssertion):
        _flyte_engine.FlyteDefaultTypeEngine().get_sdk_type_from_literal_type(
            _type_models.LiteralType(
                simple=_type_models.SimpleType.BINARY,
                metadata={_proto.Protobuf.PB_FIELD_KEY: "flyteidl.core.errors_pb2_no_exist.ContainerError"},
            )
        )


def test_unloadable_proto_from_literal_type():
    with pytest.raises(_user_exceptions.FlyteAssertion):
        _flyte_engine.FlyteDefaultTypeEngine().get_sdk_type_from_literal_type(
            _type_models.LiteralType(
                simple=_type_models.SimpleType.BINARY,
                metadata={_proto.Protobuf.PB_FIELD_KEY: "flyteidl.core.errors_pb2.ContainerErrorNoExist"},
            )
        )


def test_infer_proto_from_literal():
    sdk_type = _flyte_engine.FlyteDefaultTypeEngine().infer_sdk_type_from_literal(
        _literal_models.Literal(
            scalar=_literal_models.Scalar(
                binary=_literal_models.Binary(
                    value="",
                    tag="{}{}".format(
                        _proto.Protobuf.TAG_PREFIX,
                        "flyteidl.core.errors_pb2.ContainerError",
                    ),
                )
            )
        )
    )
    assert sdk_type.pb_type == _errors_pb2.ContainerError
