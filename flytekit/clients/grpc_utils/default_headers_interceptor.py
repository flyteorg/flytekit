from typing import Any, Callable

import grpc
from grpc_interceptor import ClientCallDetails, ClientInterceptor


class DefaultHeadersInterceptor(ClientInterceptor):
    def intercept(
        self,
        method: Callable,
        request_or_iterator: Any,
        call_details: grpc.ClientCallDetails,
    ):
        default_headers = (("accept", "application/grpc"),)
        new_details = ClientCallDetails(
            call_details.method,
            call_details.timeout,
            default_headers,
            call_details.credentials,
            call_details.wait_for_ready,
            call_details.compression,
        )

        return method(request_or_iterator, new_details)
