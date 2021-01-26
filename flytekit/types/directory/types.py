from __future__ import annotations

import os
import typing
from pathlib import Path

from flytekit.annotated.context_manager import FlyteContext
from flytekit.annotated.type_engine import TypeEngine, TypeTransformer
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = typing.TypeVar("T")


class FlyteDirectory(os.PathLike, typing.Generic[T]):
    """
    WARNING: This class should not be used on very large datasets, as merely listing the dataset will cause
    the entire dataset to be downloaded. Listing on S3 and other backend object stores is not consistent
    and we should not need data to be downloaded to list.

    Please first read through the comments on the FlyteFile class as the implementation here is similar.

    One thing to note is that the os.PathLike type that comes with Python was used as a stand-in for FlyteFile.
    That is, if a task returns an os.PathLike, Flyte takes that to mean FlyteFile. There is no easy way to
    distinguish an os.PathLike where the user means a File and where the user means a Directory. As such, if you
    want to use a directory, you must declare all types as FlyteDirectory. You'll still be able to return a string
    literal though instead of a full-fledged FlyteDirectory object assuming the str is a directory.

    Use cases as inputs:
        def t1(in1: FlyteDirectory):
            ...

        def t1(in1: FlyteDirectory["svg"]):
            ...

    As outputs:

    The contents of this local directory will be uploaded to the Flyte store.
        return FlyteDirectory("/path/to/dir/")

        return FlyteDirectory["svg"]("/path/to/dir/", remote_path="s3://special/output/location")

    Similar to the FlyteFile example, if you give an already remote location, it will not be copied to Flyte's
    durable store, the uri will just be stored as is.

        return FlyteDirectory("s3://some/other/folder")

    Note if you write a path starting with http/s, if anything ever tries to read it (i.e. use the literal
    as an input, it'll fail because the http proxy doesn't know how to download whole directories.

    The format [] bit is still there because in Flyte, directories are stored as Blob Types also, just like files, and
    the Blob type has the format field. The difference in the type field is represented in the ``dimensionality``
    field in the ``BlobType``.
    """

    def __init__(self, path: str, downloader: typing.Callable = None, remote_directory=None):
        """
        :param path: The source path that users are expected to call open() on
        :param downloader: Optional function that can be passed that used to delay downloading of the actual fil
            until a user actually calls open().
        :param remote_directory: If the user wants to return something and also specify where it should be uploaded to.
        """

        def noop():
            ...

        self._path = path
        self._downloader = downloader or noop
        self._downloaded = False
        self._remote_directory = remote_directory
        self._remote_source = None

    def __fspath__(self):
        """
        This function should be called by os.listdir as well.
        """
        self._downloader()
        self._downloaded = True
        return self._path

    @classmethod
    def extension(cls) -> str:
        return ""

    def __class_getitem__(cls, item: typing.Type) -> typing.Type[FlyteDirectory]:
        if item is None:
            return cls
        item = str(item)
        item = item.strip().lstrip("~").lstrip(".")
        if item == "":
            return cls

        class _SpecificFormatDirectoryClass(FlyteDirectory):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteDirectory

            @classmethod
            def extension(cls) -> str:
                return item

        return _SpecificFormatDirectoryClass

    @property
    def downloaded(self) -> bool:
        return self._downloaded

    @property
    def remote_directory(self) -> typing.Optional[str]:
        return self._remote_directory

    @property
    def path(self) -> str:
        return self._path

    @property
    def remote_source(self) -> str:
        """
        If this is an input to a task, and the original path is s3://something, flytekit will download the
        directory for the user. In case the user wants access to the original path, it will be here.
        """
        return self._remote_source

    def __repr__(self):
        return self._path

    def __str__(self):
        return self._path


class FlyteDirToMultipartBlobTransformer(TypeTransformer[FlyteDirectory]):
    """
    This transformer handles conversion between the Python native FlyteDirectory class defined above, and the Flyte
    IDL literal/type of Multipart Blob. Please see the FlyteDirectory comments for additional information.
    """

    def __init__(self):
        super().__init__(name="FlyteDirectory", t=FlyteDirectory)

    @staticmethod
    def get_format(t: typing.Type[FlyteDirectory]) -> str:
        return t.extension()

    @staticmethod
    def _blob_type(format: str) -> _core_types.BlobType:
        return _core_types.BlobType(format=format, dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART)

    def get_literal_type(self, t: typing.Type[FlyteDirectory]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(format=FlyteDirToMultipartBlobTransformer.get_format(t)))

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: FlyteDirectory,
        python_type: typing.Type[FlyteDirectory],
        expected: LiteralType,
    ) -> Literal:

        remote_directory = None
        should_upload = True

        # There are two kinds of literals we handle, either an actual FlyteDirectory, or a string path to a directory.
        # Handle the FlyteDirectory case
        if isinstance(python_val, FlyteDirectory):
            source_path = python_val.path
            if python_val.remote_directory is False:
                # If the user specified the remote_path to be False, that means no matter what, do not upload
                should_upload = False
            else:
                # Otherwise, if not an "" use the user-specified remote path instead of the random one
                remote_directory = python_val.remote_directory or None

        # Handle the string case
        else:
            if not (isinstance(python_val, os.PathLike) or isinstance(python_val, str)):
                raise AssertionError(f"Expected FlyteDirectory or os.PathLike object, received {type(python_val)}")

            source_path = python_val
            # Only do this check if it's a local directory.
            if not ctx.file_access.is_remote(source_path):
                p = Path(source_path)
                if not p.is_dir():
                    raise AssertionError(f"Expected a directory. {source_path} is not a directory")

        # For remote values, say s3://some/extant/dir/, we will not upload to Flyte's store (S3/GCS)
        # and just return a literal with a uri equal to the path given
        if ctx.file_access.is_remote(source_path) or not should_upload:
            meta = BlobMetadata(type=self._blob_type(format=self.get_format(python_type)))
            return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=source_path)))

        # For local paths, we will upload to the Flyte store (note that for local execution, the remote store is just
        # a subfolder), unless remote_path=False was given
        else:
            if remote_directory is None:
                remote_directory = ctx.file_access.get_random_remote_directory()
            ctx.file_access.put_data(source_path, remote_directory, is_multipart=True)
            meta = BlobMetadata(type=self._blob_type(format=self.get_format(python_type)))
            return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_directory)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: typing.Type[FlyteDirectory]
    ) -> FlyteDirectory:

        uri = lv.scalar.blob.uri

        # This is a local file path, like /usr/local/my_file, don't mess with it. Certainly, downloading it doesn't
        # make any sense.
        if not ctx.file_access.is_remote(uri):
            return expected_python_type(uri)

        # For the remote case, return an FlyteDirectory object that can download
        local_folder = ctx.file_access.get_random_local_directory()

        def _downloader():
            return ctx.file_access.get_data(uri, local_folder, is_multipart=True)

        expected_format = self.get_format(expected_python_type)

        fd = FlyteDirectory[expected_format](local_folder, _downloader)
        fd._remote_source = uri

        return fd


TypeEngine.register(FlyteDirToMultipartBlobTransformer())
