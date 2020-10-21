import os
import typing
from enum import Enum

from flytekit import logger

"""
Since there is no native equivalent of the int type for files and directories, we need to create one so that users
can express that their tasks take in or return a File.

There are a few possible types on the Python side that can be specified:

  * typing.IO
  Usually this takes the form of TextIO or BinaryIO. For now we only support these derived classes. This is what
  open() will generally return. For example,

    def get_read_file_handle() -> TextIO:
        fh = open(__file__, 'r')
        return fh

    def get_bin_read_file_handle() -> BinaryIO:
        fh = open(__file__, 'rb')
        return fh

  Note: issubclass(type(fh), typing.Text/BinaryIO) is False for some reason, nevertheless the type checker passes.

  If you specify either of these, as an input, Flyte will open a filehandle to the data, before the task runs, and pass
  that handle as the argument to your function. If you specify it as an output, Flyte will read() the data after the
  task completes, and write it to Flyte's configurable Blob store. On the backend, Flyte's type system for file and
  file-like objects include a str based "format" as part of the type. For TextIO and BinaryIO, the format will be
  "TextIO" and "BinaryIO".

  * os.PathLike
  This is just a path on the filesystem accessible from the Python process. This is a native Python abstract class.

    # NB: This is just normal Python, will not work in Flyte! Read below for more information.
    def path_task() -> os.PathLike:
        return os.path.abspath('/tmp/xyz.txt')

  If you specify a PathLike as an input, the task will receive a PathLike at task start, and you can open() it as
  normal. However, since we want to control when files are downloaded, Flyte provides its own PathLike object.

    # This is how you should write it instead.
    from flytekit import typing as flytekit_typing

    def t1(in1: flytekit_typing.FlyteFilePath) -> str:
        with open(in1, 'r') as fh:
            lines = fh.readlines()
            return "".join(lines)

  As mentioned above, since Flyte file types have a string embedded in it as part of the type, flytekit.typing
  includes a CSV variant as well.

    def t2() -> flytekit_typing.FlyteCSVFilePath:
        from random import sample
        sequence = [i for i in range(20)]
        subset = sample(sequence, 5)
        results = ",".join([str(x) for x in subset])
        with open("/tmp/blah.csv", "w") as fh:
            fh.write(results)
        return FlyteCSVFilePath("/tmp/blah.csv")
"""


class FlyteFileFormats(Enum):
    """
    When users are subclassing the FlyteFilePath type to create their own formats, they should not use these strings
    as these have special meaning. Also please use lower case.
    """

    BASE_FORMAT = ""
    TEXT_IO = "TextIO"
    BINARY_IO = "BinaryIO"
    CSV = "csv"


def noop():
    ...


class FlyteFilePath(os.PathLike):
    @classmethod
    def format(cls) -> str:
        """
        When subclassing this, please make sure you do not use the same string as any of the ones already declared
        in the FlyteFileFormats enum.
        """
        return FlyteFileFormats.BASE_FORMAT.value

    def __init__(self, path: str, downloader: typing.Callable = noop, remote_path=None):
        """
        :param path: The local path that users are expected to call open() on
        :param downloader: Optional function that can be passed that used to delay downloading of the actual fil
            until a user actually calls open().
        :param remote_path: If the user wants to return something and also specify where it should be uploaded to.
        """
        self._abspath = os.path.abspath(path)
        self._downloader = downloader
        self._downloaded = False
        self._remote_path = remote_path
        logger.debug(f"Path is: {self._abspath}")

    def __fspath__(self):
        # This is where a delayed downloading of the file will happen
        self._downloader()
        self._downloaded = True
        return self._abspath

    def __eq__(self, other):
        if self.format() != other.format():
            return False
        return self._abspath == other._abspath and self._remote_path == other._remote_path

    @property
    def downloaded(self) -> bool:
        return self._downloaded

    @property
    def remote_path(self) -> typing.Optional[str]:
        return self._remote_path

    def __repr__(self):
        return self._abspath

    def __str__(self):
        return self._abspath


class FlyteCSVFilePath(FlyteFilePath):
    @classmethod
    def format(cls) -> str:
        return FlyteFileFormats.CSV.value
