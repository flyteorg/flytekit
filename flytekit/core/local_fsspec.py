""" """

import os

from fsspec.implementations.local import LocalFileSystem


class FlyteLocalFileSystem(LocalFileSystem):  # noqa
    """
    This class doesn't do anything except override the separator so that it works on windows
    """

    sep = os.sep
