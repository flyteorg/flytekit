from typing import List

from flytekit.clis.auth.auth import AuthorizationClient
from flytekit.loggers import auth_logger

# Default, well known-URI string used for fetching JSON metadata. See https://tools.ietf.org/html/rfc8414#section-3.
discovery_endpoint_path = "./.well-known/oauth-authorization-server"

# Lazy initialized authorization client singleton
_authorization_client = None


def get_client(
    redirect_endpoint: str, client_id: str, scopes: List[str], auth_endpoint: str, token_endpoint: str
) -> AuthorizationClient:
    global _authorization_client
    if _authorization_client is not None and not _authorization_client.expired:
        return _authorization_client

    _authorization_client = AuthorizationClient(
        redirect_uri=redirect_endpoint,
        client_id=client_id,
        scopes=scopes,
        auth_endpoint=auth_endpoint,
        token_endpoint=token_endpoint,
    )

    auth_logger.debug(f"Created oauth client with redirect {_authorization_client}")

    if not _authorization_client.has_valid_credentials:
        _authorization_client.start_authorization_flow()

    return _authorization_client
