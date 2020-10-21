import os as _os
import uuid as _uuid
from distutils import dir_util as _dir_util
from shutil import copyfile as _copyfile

from flytekit.interfaces import random as _flyte_random
from flytekit.interfaces.data import common as _common_data


def _make_local_path(path):
    if not _os.path.exists(path):
        try:
            _os.makedirs(path)
        except OSError:  # Guard against race condition
            if not _os.path.isdir(path):
                raise


def strip_file_header(path: str) -> str:
    if path.startswith("file://"):
        return path.replace("file://", "", 1)
    return path


class LocalFileProxy(_common_data.DataProxy):
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
        return _os.path.exists(strip_file_header(path))

    def download_directory(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        if from_path != to_path:
            _dir_util.copy_tree(strip_file_header(from_path), strip_file_header(to_path))

    def download(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        _copyfile(strip_file_header(from_path), strip_file_header(to_path))

    def upload(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        # Emulate s3's flat storage by automatically creating directory path
        _make_local_path(_os.path.dirname(strip_file_header(to_path)))
        # Write the object to a local file in the sandbox
        _copyfile(strip_file_header(from_path), strip_file_header(to_path))

    def upload_directory(self, from_path, to_path):
        """
        :param Text from_path:
        :param Text to_path:
        """
        self.download_directory(from_path, to_path)

    def get_random_path(self):
        """
        :rtype: Text
        """
        # Create a 128-bit random hash because the birthday attack principle shows that there is about a 50% chance of a
        # collision between objects when 2^(n/2) objects are created (where n is the number of bits in the hash).
        # Assuming Flyte eventually creates 1 trillion pieces of data (~2 ^ 40), the likelihood
        # of a collision is 10^-15 with 128-bit...or basically 0.
        return _os.path.join(self._sandbox, _uuid.UUID(int=_flyte_random.random.getrandbits(128)).hex)

    def get_random_directory(self):
        """
        :rtype: Text
        """
        return self.get_random_path() + "/"
