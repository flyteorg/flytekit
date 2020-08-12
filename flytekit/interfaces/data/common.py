from __future__ import absolute_import

import abc as _abc

import six as _six


class DataProxy(_six.with_metaclass(_abc.ABCMeta, object)):
    def exists(self, path):
        """
        :param path:
        :rtype: bool: whether the file exists or not
        """
        pass

    def download_directory(self, remote_path, local_path):
        """
        :param Text remote_path:
        :param Text local_path:
        """
        pass

    def download(self, remote_path, local_path):
        """
        :param Text remote_path:
        :param Text local_path:
        """
        pass

    def upload(self, file_path, to_path):
        """
        :param Text file_path:
        :param Text to_path:
        """
        pass

    def upload_directory(self, local_path, remote_path):
        """
        :param Text local_path:
        :param Text remote_path:
        """
        pass

    def get_random_path(self):
        """
        :rtype: Text
        """
        pass

    def get_random_directory(self):
        """
        :rtype: Text
        """
        pass
