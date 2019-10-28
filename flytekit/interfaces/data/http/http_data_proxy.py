from __future__ import absolute_import

import os as _os
import requests as _requests
from flytekit.interfaces.data import common as _common_data
from flytekit.common.exceptions import user as _user_exceptions


class HttpFileProxy(_common_data.DataProxy):

    def __init__(self, sandbox):
        """
        :param Text sandbox:
        """
        self._sandbox = sandbox

    def exists(self, path):
        """
        :param Text path: the path of the file
        :rtype bool: whether the file exists or not
        """
        return _os.path.exists(path)

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
        _HTTP_OK = 200
        _CONTENT_TYPE = 'binary/octet-stream'
        rsp = _requests.get(from_path)
        if rsp.status_code != _HTTP_OK:
            raise _user_exceptions.FlyteValueException(
                rsp.status_code,
                "Request for data @ {} failed. Expected status code {}".format(from_path, _HTTP_OK)
            )
        if rsp.headers.get('content-type') != _CONTENT_TYPE:
            raise _user_exceptions.FlyteValueException(
                rsp.headers['content-type'],
                "Data from {} expected to be received as '{}'.".format(from_path, _CONTENT_TYPE)
            )

        with open(to_path, 'wb') as writer:
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
