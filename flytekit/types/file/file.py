from __future__ import annotations

import os
import typing

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType


def noop():
    ...


T = typing.TypeVar("T")


class FlyteFile(os.PathLike, typing.Generic[T]):
    """
    Since there is no native Python implementation of files and directories for the Flyte Blob type, (like how int
    exists for Flyte's Integer type) we need to create one so that users can express that their tasks take
    in or return a file.

    There are a few possible types on the Python side that can be specified:

    * :class:`python:typing.IO`
      Usually this takes the form of TextIO or BinaryIO. For now we only support these derived classes. This is what
      open() will generally return. For example, ::

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
      "TextIO" and "BinaryIO". These IO types have a higher likelihood of being subject to change before an official,
      release. The PathLike types will not.

    * :class:`python:os.PathLike`
      This is just a path on the filesystem accessible from the Python process. This is a native Python abstract class.

      .. code-block:: python

          def path_task() -> os.PathLike:
              return '/tmp/xyz.txt'

      If you specify a PathLike as an input, the task will receive a PathLike at task start, and you can open() it as
      normal. However, since we want to control when files are downloaded, Flyte provides its own PathLike object::

        from flytekit import types as flytekit_typing

        def t1(in1: flytekit_typing.FlyteFile) -> str:
            with open(in1, 'r') as fh:
                lines = fh.readlines()
                return "".join(lines)

      As mentioned above, since Flyte file types have a string embedded in it as part of the type, you can add a
      format by specifying a string after the class like so. ::

        def t2() -> flytekit_typing.FlyteFile["csv"]:
            from random import sample
            sequence = [i for i in range(20)]
            subset = sample(sequence, 5)
            results = ",".join([str(x) for x in subset])
            with open("/tmp/blah.csv", "w") as fh:
                fh.write(results)
            return "/tmp/blah.csv"

    How are these files handled?

    S3, http, https are all treated as remote - the behavior should be the same, they are never copied unless
    explicitly told to do so.

    Local paths always get uploaded, unless explicitly told not to do so.

    If you specify a path as a string, you get the default behavior, not possible to override. To override, you must use
    the FlyteFile class.

    More succinctly, regardless of whether it is input or output, these rules apply:
      - ``"s3://bucket/path"`` -> will never get uploaded
      - ``"https://a.b.com/path"`` -> will never get uploaded
      - ``"/tmp/blah"`` -> will always get uploaded

    To specify non-default behavior:

    * Copy the s3 path to a new location.
      ``FlyteFilePath("s3://bucket/path", remote_path=True)``

    * Copy the s3 path to a specific location.
      ``FlyteFilePath("s3://bucket/path", remote_path="s3://other-bucket/path")``

    * Copy local path to a specific location.
      ``FlyteFilePath("/tmp/blah", remote_path="s3://other-bucket/path")``

    * Do not copy local path, this will copy the string into the literal. For example, let's say your docker image has a
      thousand files in it, and you want to tell the next task, which file to look at. (Bad example, you shouldn't have
      that many files in your image.)
      ``FlyteFilePath("/tmp/blah", remote_path=False)``

    * However, we have a shorthand.
      "file:///tmp/blah" is treated as "remote" and is by default not copied.
    """

    @classmethod
    def extension(cls) -> str:
        return ""

    def __class_getitem__(cls, item: typing.Type) -> typing.Type[FlyteFile]:
        if item is None:
            return cls
        item = str(item)
        item = item.strip().lstrip("~").lstrip(".")
        if item == "":
            return cls

        class _SpecificFormatClass(FlyteFile):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteFile

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
        self._path = path
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
        if isinstance(other, FlyteFile):
            return (
                self._path == other._path
                and self._remote_path == other._remote_path
                and self.extension() == other.extension()
            )
        else:
            return self._path == other

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


class FlyteFilePathTransformer(TypeTransformer[FlyteFile]):
    def __init__(self):
        super().__init__(name="FlyteFilePath", t=FlyteFile)

    @staticmethod
    def get_format(t: typing.Type[FlyteFile]) -> str:
        return t.extension()

    def _blob_type(self, format: str) -> _core_types.BlobType:
        return _core_types.BlobType(format=format, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE)

    def get_literal_type(self, t: typing.Type[FlyteFile]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(format=FlyteFilePathTransformer.get_format(t)))

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: typing.Union[FlyteFile, os.PathLike, str],
        python_type: typing.Type[FlyteFile],
        expected: LiteralType,
    ) -> Literal:
        remote_path = None
        should_upload = True

        if python_val is None:
            raise AssertionError("None value cannot be converted to a file.")
        if isinstance(python_val, FlyteFile):
            source_path = python_val.path
            if python_val.remote_path is False:
                # If the user specified the remote_path to be False, that means no matter what, do not upload
                should_upload = False
            else:
                # Otherwise, if not an "" use the user-specified remote path instead of the random one
                remote_path = python_val.remote_path or None
        else:
            if not (isinstance(python_val, os.PathLike) or isinstance(python_val, str)):
                raise AssertionError(f"Expected FlyteFile or os.PathLike object, received {type(python_val)}")
            source_path = python_val

        # For remote values, say https://raw.github.com/demo_data.csv, we will not upload to Flyte's store (S3/GCS)
        # and just return a literal with a uri equal to the path given
        if ctx.file_access.is_remote(source_path) or not should_upload:
            # TODO: Add copying functionality so that FlyteFile(path="s3://a", remote_path="s3://b") will copy.
            meta = BlobMetadata(type=self._blob_type(format=FlyteFilePathTransformer.get_format(python_type)))
            return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=source_path)))

        # For local paths, we will upload to the Flyte store (note that for local execution, the remote store is just
        # a subfolder), unless remote_path=False was given
        else:
            if remote_path is None:
                remote_path = ctx.file_access.get_random_remote_path(source_path)
            ctx.file_access.put_data(source_path, remote_path, is_multipart=False)
            meta = BlobMetadata(type=self._blob_type(format=FlyteFilePathTransformer.get_format(python_type)))
            return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path or source_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: typing.Type[FlyteFile]
    ) -> FlyteFile:

        uri = lv.scalar.blob.uri

        # This is a local file path, like /usr/local/my_file, don't mess with it. Certainly, downloading it doesn't
        # make any sense.
        if not ctx.file_access.is_remote(uri):
            return expected_python_type(uri)

        # For the remote case, return an FlyteFile object that can download
        local_path = ctx.file_access.get_random_local_path(uri)

        def _downloader():
            return ctx.file_access.get_data(uri, local_path, is_multipart=False)

        expected_format = FlyteFilePathTransformer.get_format(expected_python_type)
        ff = FlyteFile[expected_format](local_path, _downloader)
        ff._remote_source = uri

        return ff


TypeEngine.register(FlyteFilePathTransformer())
