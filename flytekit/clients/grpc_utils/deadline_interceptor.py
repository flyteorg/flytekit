import contextlib
import contextvars
import typing

import grpc

from flytekit.clients.grpc_utils.auth_interceptor import _ClientCallDetails

_GRPC_DEADLINE_SECONDS: contextvars.ContextVar[typing.Optional[float]] = contextvars.ContextVar(
    "flytekit_grpc_deadline_seconds", default=None
)


def get_scoped_grpc_deadline() -> typing.Optional[float]:
    return _GRPC_DEADLINE_SECONDS.get()


@contextlib.contextmanager
def scoped_grpc_deadline(seconds: float):
    token = _GRPC_DEADLINE_SECONDS.set(seconds)
    try:
        yield
    finally:
        _GRPC_DEADLINE_SECONDS.reset(token)


class ScopedGrpcDeadlineInterceptor(grpc.UnaryUnaryClientInterceptor):
    """Applies the currently scoped gRPC timeout to unary-unary calls."""

    def intercept_unary_unary(
        self,
        continuation: typing.Callable,
        client_call_details: grpc.ClientCallDetails,
        request: typing.Any,
    ):
        scoped_timeout = get_scoped_grpc_deadline()
        if scoped_timeout is None:
            return continuation(client_call_details, request)

        timeout = scoped_timeout
        if client_call_details.timeout is not None:
            timeout = min(scoped_timeout, client_call_details.timeout)

        updated_call_details = _ClientCallDetails(
            client_call_details.method,
            timeout,
            client_call_details.metadata,
            client_call_details.credentials,
        )
        return continuation(updated_call_details, request)
