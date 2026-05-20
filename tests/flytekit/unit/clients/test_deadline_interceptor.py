from unittest.mock import Mock

import grpc
import pytest

from flytekit.clients.grpc_utils.auth_interceptor import _ClientCallDetails
from flytekit.clients.grpc_utils.deadline_interceptor import (
    ScopedGrpcDeadlineInterceptor,
    get_scoped_grpc_deadline,
    scoped_grpc_deadline,
)
from flytekit.clients.grpc_utils.wrap_exception_interceptor import RetryExceptionWrapperInterceptor
from flytekit.exceptions.user import FlyteTimeout


class DeadlineExceededError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.DEADLINE_EXCEEDED

    def details(self):
        return "deadline exceeded"


def test_scoped_grpc_deadline_resets():
    assert get_scoped_grpc_deadline() is None
    with scoped_grpc_deadline(5):
        assert get_scoped_grpc_deadline() == 5
    assert get_scoped_grpc_deadline() is None


def test_deadline_interceptor_only_handles_unary_unary():
    interceptor = ScopedGrpcDeadlineInterceptor()
    assert isinstance(interceptor, grpc.UnaryUnaryClientInterceptor)
    assert not isinstance(interceptor, grpc.UnaryStreamClientInterceptor)


def test_deadline_interceptor_applies_scoped_timeout():
    interceptor = ScopedGrpcDeadlineInterceptor()
    request = object()
    continuation = Mock(return_value="response")
    call_details = _ClientCallDetails("/flyteidl.service.AdminService/GetExecution", None, None, None)

    with scoped_grpc_deadline(30):
        assert interceptor.intercept_unary_unary(continuation, call_details, request) == "response"

    continuation.assert_called_once()
    updated_call_details = continuation.call_args.args[0]
    assert updated_call_details.timeout == 30
    assert continuation.call_args.args[1] is request


def test_deadline_interceptor_preserves_shorter_existing_timeout():
    interceptor = ScopedGrpcDeadlineInterceptor()
    continuation = Mock(return_value="response")
    call_details = _ClientCallDetails("/flyteidl.service.AdminService/GetExecution", 10, None, None)

    with scoped_grpc_deadline(30):
        assert interceptor.intercept_unary_unary(continuation, call_details, object()) == "response"

    updated_call_details = continuation.call_args.args[0]
    assert updated_call_details.timeout == 10


def test_deadline_interceptor_ignores_calls_without_scoped_timeout():
    interceptor = ScopedGrpcDeadlineInterceptor()
    continuation = Mock(return_value="response")
    call_details = _ClientCallDetails("/flyteidl.service.AdminService/GetExecution", None, None, None)

    assert interceptor.intercept_unary_unary(continuation, call_details, object()) == "response"

    assert continuation.call_args.args[0] is call_details


def test_deadline_exceeded_maps_to_flyte_timeout_without_retry():
    interceptor = RetryExceptionWrapperInterceptor()
    continuation = Mock()
    future = Mock()
    future.exception.return_value = DeadlineExceededError()
    continuation.return_value = future
    call_details = _ClientCallDetails("/flyteidl.service.AdminService/GetExecution", None, None, None)

    with pytest.raises(FlyteTimeout, match="deadline exceeded"):
        interceptor.intercept_unary_unary(continuation, call_details, object())

    assert continuation.call_count == 1
