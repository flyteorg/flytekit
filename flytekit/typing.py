from __future__ import annotations

import os
import typing


def noop():
    ...


class FlyteFilePath(os.PathLike):
    """
    Since there is no native implementation of the int type for files and directories, we need to create one so that users
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

        def path_task() -> os.PathLike:
            return '/tmp/xyz.txt'

      If you specify a PathLike as an input, the task will receive a PathLike at task start, and you can open() it as
      normal. However, since we want to control when files are downloaded, Flyte provides its own PathLike object.

        from flytekit import typing as flytekit_typing

        def t1(in1: flytekit_typing.FlyteFile) -> str:
            with open(in1, 'r') as fh:
                lines = fh.readlines()
                return "".join(lines)

      As mentioned above, since Flyte file types have a string embedded in it as part of the type, you can add a
      format by specifying a string after the class like so.

        def t2() -> flytekit_typing.FlyteFile["csv"]:
            from random import sample
            sequence = [i for i in range(20)]
            subset = sample(sequence, 5)
            results = ",".join([str(x) for x in subset])
            with open("/tmp/blah.csv", "w") as fh:
                fh.write(results)
            return "/tmp/blah.csv"

    How are these files handled?
    ============================

    S3, http, https are all treated as remote - the behavior should be the same, they are never copied unless
    explicitly told to do so.

    Local paths always get uploaded, unless explicitly told not to do so.

    If you specify a path as a string, you get the default behavior, not possible to override. To override, you must use
    the FlyteFilePath class.

    More succinctly, regardless of whether it is input or output, these rules apply:
      - "s3://bucket/path" -> will never get uploaded
      - "https://a.b.com/path" -> will never get uploaded
      - "/tmp/blah" -> will always get uploaded

    To specify non-default behavior:

    * Copy the s3 path to a new location.
      FlyteFilePath("s3://bucket/path", remote_path=True)

    * Copy the s3 path to a specific location.
      FlyteFilePath("s3://bucket/path", remote_path="s3://other-bucket/path")

    * Copy local path to a specific location.
      FlyteFilePath("/tmp/blah", remote_path="s3://other-bucket/path")

    * Do not copy local path, this will copy the string into the literal. For example, let's say your docker image has a
      thousand files in it, and you want to tell the next task, which file to look at. (Dumb example, you shouldn't have that
      many files in your image.)
      FlyteFilePath("/tmp/blah", remote_path=False)

    * However, we have a shorthand.
      "file:///tmp/blah" is treated as "remote" and is by default not copied.
    """

    @classmethod
    def extension(cls) -> str:
        return ""

    def __class_getitem__(cls, item: typing.Type) -> typing.Type[FlyteFilePath]:
        if item is None:
            return cls
        item = str(item)
        item = item.strip().lstrip("~").lstrip(".")
        if item == "":
            return cls

        class _SpecificFormatClass(FlyteFilePath):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteFilePath

            @classmethod
            def extension(cls) -> str:
                return item

        return _SpecificFormatClass

    def __init__(self, path: str, downloader: typing.Callable = noop, remote_path=None):
        """
        :param path: The source path that users are expected to call open() on
        :param downloader: Optional function that can be passed that used to delay downloading of the actual fil
            until a user actually calls open().
        :param remote_path: If the user wants to return something and also specify where it should be uploaded to.
        """
        self._path = os.path.abspath(path)
        self._downloader = downloader
        self._downloaded = False
        self._remote_path = remote_path

        self._remote_source = None

    def __fspath__(self):
        # This is where a delayed downloading of the file will happen
        self._downloader()
        self._downloaded = True
        return self._path

    def __eq__(self, other):
        return (
            self._path == other._path
            and self._remote_path == other._remote_path
            and self.extension() == other.extension()
        )

    @property
    def downloaded(self) -> bool:
        return self._downloaded

    @property
    def remote_path(self) -> typing.Optional[str]:
        return self._remote_path

    @property
    def path(self) -> str:
        return self._path

    @property
    def remote_source(self) -> str:
        """
        If this is an input to a task, and the original path is s3://something, flytekit will download the
        file for the user. In case the user wants access to the original path, it will be here.
        """
        return self._remote_source

    def __repr__(self):
        return self._path

    def __str__(self):
        return self._path
