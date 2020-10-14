import requests as _requests

from flytekit.common.exceptions import user as _user_exceptions
from flytekit.interfaces.data import common as _common_data


class HttpFileProxy(_common_data.DataProxy):

    _HTTP_OK = 200
    _HTTP_FORBIDDEN = 403
    _HTTP_NOT_FOUND = 404

    def exists(self, path):
        """
        :param Text path: the path of the file
        :rtype bool: whether the file exists or not
        """
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

    def download_directory(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        raise _user_exceptions.FlyteAssertion("Reading data recursively from HTTP endpoint is not currently supported.")

    def download(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """

        rsp = _requests.get(from_path)
        if rsp.status_code != type(self)._HTTP_OK:
            raise _user_exceptions.FlyteValueException(
                rsp.status_code,
                "Request for data @ {} failed. Expected status code {}".format(from_path, type(self)._HTTP_OK),
            )
        with open(to_path, "wb") as writer:
            writer.write(rsp.content)

    def upload(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        raise _user_exceptions.FlyteAssertion("Writing data to HTTP endpoint is not currently supported.")

    def upload_directory(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        raise _user_exceptions.FlyteAssertion("Writing data to HTTP endpoint is not currently supported.")

    def get_random_path(self):
        """
        :rtype: Text
        """
        raise _user_exceptions.FlyteAssertion("Writing data to HTTP endpoint is not currently supported.")

    def get_random_directory(self):
        """
        :rtype: Text
        """
        raise _user_exceptions.FlyteAssertion("Writing data to HTTP endpoint is not currently supported.")
