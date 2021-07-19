import requests as _requests

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins


class HttpPersistence(DataPersistence):
    PROTOCOL_HTTP = "http"
    PROTOCOL_HTTPS = "https"
    _HTTP_OK = 200
    _HTTP_FORBIDDEN = 403
    _HTTP_NOT_FOUND = 404

    def __init__(self, *args, **kwargs):
        super(HttpPersistence, self).__init__(name="http/https", *args, **kwargs)

    def exists(self, path: str):
        rsp = _requests.head(path)
        allowed_codes = {
            type(self)._HTTP_OK,
            type(self)._HTTP_NOT_FOUND,
            type(self)._HTTP_FORBIDDEN,
        }
        if rsp.status_code not in allowed_codes:
            raise _user_exceptions.FlyteValueException(
                rsp.status_code,
                "Data at {} could not be checked for existence. Expected one of: {}".format(path, allowed_codes),
            )
        return rsp.status_code == type(self)._HTTP_OK

    def download_directory(self, from_path: str, to_path: str):
        raise _user_exceptions.FlyteAssertion("Reading data recursively from HTTP endpoint is not currently supported.")

    def download(self, from_path: str, to_path: str):
        rsp = _requests.get(from_path)
        if rsp.status_code != type(self)._HTTP_OK:
            raise _user_exceptions.FlyteValueException(
                rsp.status_code,
                "Request for data @ {} failed. Expected status code {}".format(from_path, type(self)._HTTP_OK),
            )
        with open(to_path, "wb") as writer:
            writer.write(rsp.content)

    def upload(self, from_path: str, to_path: str):
        raise _user_exceptions.FlyteAssertion("Writing data to HTTP endpoint is not currently supported.")

    def upload_directory(self, from_path: str, to_path: str):
        raise _user_exceptions.FlyteAssertion("Writing data to HTTP endpoint is not currently supported.")

    def construct_path(self, add_protocol: bool, *paths) -> str:
        raise _user_exceptions.FlyteAssertion(
            "There are multiple ways of creating http links / paths, this is not supported by the persistence layer")


DataPersistencePlugins.register_plugin("http://", HttpPersistence())
DataPersistencePlugins.register_plugin("https://", HttpPersistence())