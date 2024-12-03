import logging
import typing
from collections import namedtuple

import grpc

from flytekit.clients.auth.authenticator import Authenticator, ClientConfigStore
from flytekit.configuration import PlatformConfig


class _ClientCallDetails(
    namedtuple("_ClientCallDetails", ("method", "timeout", "metadata", "credentials")),
    grpc.ClientCallDetails,
):
    """
    Wrapper class for initializing a new ClientCallDetails instance.
    We cannot make this of type - NamedTuple because, NamedTuple has a metaclass of type NamedTupleMeta and both
    the metaclasses conflict
    """

    pass


class AuthUnaryInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor):
    """
    This Interceptor can be used to automatically add Auth Metadata for every call - lazily in case authentication
    is needed.
    """

    def __init__(self, authenticator: Authenticator, cfg: PlatformConfig = None, cfg_store: ClientConfigStore = None):
        self._authenticator = authenticator
        self._cfg = cfg
        self._cfg_store = cfg_store

    def _call_details_with_auth_metadata(self, client_call_details: grpc.ClientCallDetails) -> grpc.ClientCallDetails:
        """
        Returns new ClientCallDetails with metadata added.
        """
        metadata = client_call_details.metadata
        auth_metadata = self._authenticator.fetch_grpc_call_auth_metadata()
        if auth_metadata:
            metadata = []
            if client_call_details.metadata:
                metadata.extend(list(client_call_details.metadata))
            metadata.append(auth_metadata)

        return _ClientCallDetails(
            client_call_details.method,
            client_call_details.timeout,
            metadata,
            client_call_details.credentials,
        )

    def intercept_unary_unary(
        self,
        continuation: typing.Callable,
        client_call_details: grpc.ClientCallDetails,
        request: typing.Any,
    ):
        """
        Intercepts unary calls and adds auth metadata if available. On Unauthenticated, resets the token and refreshes
        and then retries with the new token
        """
        updated_call_details = self._call_details_with_auth_metadata(client_call_details)
        fut: grpc.Future = continuation(updated_call_details, request)
        e = fut.exception()
        if e:
            if not hasattr(e, "code"):
                raise e
            # When gRPC is UNAUTHENTICATED (401) or grpc is UNKNOWN, handle the authentication error and trigger the PKCE authenticator.
            if e.code() == grpc.StatusCode.UNAUTHENTICATED or e.code() == grpc.StatusCode.UNKNOWN:
                return self._handle_unauthenticated_error(fut, client_call_details, request)
        return fut

    def intercept_unary_stream(self, continuation, client_call_details, request):
        """
        Handles a stream call and adds authentication metadata if needed
        """
        updated_call_details = self._call_details_with_auth_metadata(client_call_details)
        c: grpc.Call = continuation(updated_call_details, request)
        # When gRPC is UNAUTHENTICATED (401), handle the authentication error and trigger the PKCE authenticator.
        if c.code() == grpc.StatusCode.UNAUTHENTICATED:
            return self._handle_unauthenticated_error(c, client_call_details, request)
        return c

    def _handle_unauthenticated_error(self, continuation, client_call_details, request):
        """
        Handling Unauthenticated Errors and Triggering the PKCE Flow
        """

        logging.info("Received authentication error, starting PKCE authentication flow")

        try:
            if isinstance(self._authenticator, Authenticator) and not isinstance(
                self._authenticator, PKCEAuthenticator
            ):
                logging.info("Current authenticator is 'None', switching to PKCEAuthenticator")

                from flytekit.clients.auth.authenticator import PKCEAuthenticator
                from flytekit.clients.auth_helper import get_session

                session = get_session(self._cfg)

                verify = None
                if self._cfg.insecure_skip_verify:
                    verify = False
                elif self._cfg.ca_cert_file_path:
                    verify = self._cfg.ca_cert_file_path

                self._authenticator = PKCEAuthenticator(
                    self._cfg.endpoint, self._cfg_store, scopes=self._cfg.scopes, verify=verify, session=session
                )

            self._authenticator.refresh_credentials()
            logging.info("Authentication flow completed successfully")

        except Exception as e:
            logging.error(f"Authentication failed: {str(e)}")
            raise

        updated_call_details = self._call_details_with_auth_metadata(client_call_details)
        return continuation(updated_call_details, request)
