import logging
import typing

import click
from google.api_core.exceptions import NotFound
from google.cloud import secretmanager

from flytekit.clients.auth.auth_client import AuthorizationClient
from flytekit.clients.auth.authenticator import Authenticator
from flytekit.clients.auth.exceptions import AccessTokenNotFoundError
from flytekit.clients.auth.keyring import KeyringStore


class GCPIdentityAwareProxyAuthenticator(Authenticator):
    """
    This Authenticator encapsulates the entire OAauth 2.0 flow with GCP Identity Aware Proxy.

    The auth flow is described in https://cloud.google.com/iap/docs/authentication-howto#signing_in_to_the_application

    Automatically opens a browser window for login.
    """

    def __init__(
        self,
        audience: str,
        client_id: str,
        client_secret: str,
        verify: typing.Optional[typing.Union[bool, str]] = None,
    ):
        """
        Initialize with default creds from KeyStore using the audience name.
        """
        super().__init__(audience, "proxy-authorization", KeyringStore.retrieve(audience), verify=verify)
        self._auth_client = None

        self.audience = audience
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_uri = "http://localhost:4444"

    def _initialize_auth_client(self):
        if not self._auth_client:
            self._auth_client = AuthorizationClient(
                endpoint=self.audience,
                # See step 3 in https://cloud.google.com/iap/docs/authentication-howto#signing_in_to_the_application
                auth_endpoint="https://accounts.google.com/o/oauth2/v2/auth",
                token_endpoint="https://oauth2.googleapis.com/token",
                # See step 3 in https://cloud.google.com/iap/docs/authentication-howto#signing_in_to_the_application
                scopes=["openid", "email"],
                client_id=self.client_id,
                redirect_uri=self.redirect_uri,
                verify=self._verify,
                # See step 3 in https://cloud.google.com/iap/docs/authentication-howto#signing_in_to_the_application
                request_auth_code_params={
                    "cred_ref": "true",
                    "access_type": "offline",
                },
                # See step 4 in https://cloud.google.com/iap/docs/authentication-howto#signing_in_to_the_application
                request_access_token_params={
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "audience": self.audience,
                    "redirect_uri": self.redirect_uri,
                },
                # See https://cloud.google.com/iap/docs/authentication-howto#refresh_token
                refresh_access_token_params={
                    "client_secret": self.client_secret,
                    "audience": self.audience,
                },
            )

    def refresh_credentials(self):
        """Refresh the IAP credentials. If no credentials are found, it will kick off a full OAuth 2.0 authorization flow."""
        self._initialize_auth_client()
        if self._creds:
            """We have an id token so lets try to refresh it"""
            try:
                self._creds = self._auth_client.refresh_access_token(self._creds)
                if self._creds:
                    KeyringStore.store(self._creds)
                return
            except AccessTokenNotFoundError:
                logging.warning("Failed to refresh token. Kicking off a full authorization flow.")
                KeyringStore.delete(self._endpoint)

        self._creds = self._auth_client.get_creds_from_remote()
        KeyringStore.store(self._creds)


def get_gcp_secret_manager_secret(project_id: str, secret_id: str, version: typing.Optional[str] = "latest"):
    """Retrieve secret from GCP secret manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    try:
        response = client.access_secret_version(name=name)
    except NotFound as e:
        raise click.BadParameter(e.message)
    payload = response.payload.data.decode("UTF-8")
    return payload


@click.command()
@click.option(
    "--desktop_client_id",
    type=str,
    default=None,
    required=True,
    help=(
        "Desktop type OAuth 2.0 client ID. Typically in the form of `<xyz>.apps.googleusercontent.com`. "
        "Create by following https://cloud.google.com/iap/docs/authentication-howto#setting_up_the_client_id"
    ),
)
@click.option(
    "--desktop_client_secret_gcp_secret_name",
    type=str,
    default=None,
    required=True,
    help=(
        "Name of a GCP secret manager secret containing the desktop type OAuth 2.0 client secret "
        "obtained together with desktop type OAuth 2.0 client ID."
    ),
)
@click.option(
    "--webapp_client_id",
    type=str,
    default=None,
    required=True,
    help=(
        "Webapp type OAuth 2.0 client ID. Typically in the form of `<xyz>.apps.googleusercontent.com`. "
        "Created when activating IAP for the Flyte deployment. "
        "https://cloud.google.com/iap/docs/enabling-kubernetes-howto#oauth-credentials"
    ),
)
@click.option(
    "--project",
    type=str,
    default=None,
    required=True,
    help="GCP project ID (in which `desktop_client_secret_gcp_secret_name` is saved).",
)
def flyte_iap_token(
    desktop_client_id: str, desktop_client_secret_gcp_secret_name: str, webapp_client_id: str, project: str
):
    """Generate an ID token for proxy-authentication/authorization with GCP Identity Aware Proxy."""
    desktop_client_secret = get_gcp_secret_manager_secret(project, desktop_client_secret_gcp_secret_name)

    iap_authenticator = GCPIdentityAwareProxyAuthenticator(
        audience=webapp_client_id,
        client_id=desktop_client_id,
        client_secret=desktop_client_secret,
    )
    try:
        iap_authenticator.refresh_credentials()
    except Exception as e:
        raise click.ClickException(f"Failed to obtain credentials for GCP Identity Aware Proxy (IAP): {e}")

    click.echo(iap_authenticator.get_credentials().id_token)


if __name__ == "__main__":
    flyte_iap_token()
