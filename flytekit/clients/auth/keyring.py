import typing
from dataclasses import dataclass

import keyring as _keyring


@dataclass
class Credentials(object):
    """
    Stores the credentials together
    """

    access_token: str
    refresh_token: typing.Optional[str] = None
    for_endpoint: typing.Optional[str] = None


class KeyringStore:
    """
    Methods to access Keyring Store.
    """

    _access_token_key = "access_token"
    _refresh_token_key = "refresh_token"

    @staticmethod
    def store(credentials: Credentials) -> Credentials:
        _keyring.set_password(
            credentials.for_endpoint,
            KeyringStore._refresh_token_key,
            credentials.refresh_token,
        )
        _keyring.set_password(
            credentials.for_endpoint,
            KeyringStore._access_token_key,
            credentials.access_token,
        )
        return credentials

    @staticmethod
    def retrieve(for_endpoint: str) -> typing.Optional[Credentials]:
        refresh_token = _keyring.get_password(for_endpoint, KeyringStore._refresh_token_key)
        access_token = _keyring.get_password(for_endpoint, KeyringStore._access_token_key)
        if not access_token:
            return None
        return Credentials(access_token, refresh_token, for_endpoint)

    @staticmethod
    def delete(for_endpoint: str):
        _keyring.delete_password(for_endpoint, KeyringStore._access_token_key)
        _keyring.delete_password(for_endpoint, KeyringStore._refresh_token_key)
