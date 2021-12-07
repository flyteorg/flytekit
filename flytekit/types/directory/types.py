from __future__ import annotations

import os
import pathlib
import typing
from dataclasses import dataclass, field
from pathlib import Path

from dataclasses_json import config, dataclass_json
from marshmallow import fields

from flytekit.core.context_manager import FlyteContext
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import Blob, BlobMetadata, Literal, Scalar
from flytekit.models.types import LiteralType

T = typing.TypeVar("T")


def noop():
    ...


@dataclass_json
@dataclass
class FlyteDirectory(os.PathLike, typing.Generic[T]):
    path: typing.Union[str, os.PathLike] = field(default=None, metadata=config(mm_field=fields.String()))
    """
    .. warning::

        This class should not be used on very large datasets, as merely listing the dataset will cause
        the entire dataset to be downloaded. Listing on S3 and other backend object stores is not consistent
        and we should not need data to be downloaded to list.

    Please first read through the comments on the :py:class:`flytekit.types.file.FlyteFile` class as the
    implementation here is similar.

    One thing to note is that the ``os.PathLike`` type that comes with Python was used as a stand-in for ``FlyteFile``.
    That is, if a task's output signature is an ``os.PathLike``, Flyte takes that to mean ``FlyteFile``. There is no
    easy way to distinguish an ``os.PathLike`` where the user means a File and where the user means a Directory. As
    such, if you want to use a directory, you must declare all types as ``FlyteDirectory``. You'll still be able to
    return a string literal though instead of a full-fledged ``FlyteDirectory`` object assuming the str is a directory.

    **Converting from a Flyte literal value to a Python instance of FlyteDirectory**

    +-----------------------------+------------------------------------------------------------------------------------+
    | Type of Flyte IDL Literal   |    FlyteDirectory                                                                  |
    +=============+===============+====================================================================================+
    | Multipart   | uri matches   | FlyteDirectory object stores the original string                                   |
    | Blob        | http(s)/s3/gs | path, but points to a local file instead.                                          |
    |             |               |                                                                                    |
    |             |               | * [fn] downloader: function that writes to path when open'ed.                      |
    |             |               | * [fn] download: will trigger download                                             |
    |             |               | * path: randomly generated local path that will not exist until downloaded         |
    |             |               | * remote_path: None                                                                |
    |             |               | * remote_source: original http/s3/gs path                                          |
    |             |               |                                                                                    |
    |             +---------------+------------------------------------------------------------------------------------+
    |             | uri matches   | FlyteDirectory object just wraps the string                                        |
    |             | /local/path   |                                                                                    |
    |             |               | * [fn] downloader: noop function                                                   |
    |             |               | * [fn] download: raises exception                                                  |
    |             |               | * path: just the given path                                                        |
    |             |               | * remote_path: None                                                                |
    |             |               | * remote_source: None                                                              |
    +-------------+---------------+------------------------------------------------------------------------------------+

    -----------

    **Converting from a Python value (FlyteDirectory, str, or pathlib.Path) to a Flyte literal**

    +-----------------------------------+------------------------------------------------------------------------------+
    | Type of Python value              | FlyteDirectory                                                               |
    +===================+===============+==============================================================================+
    | str or            | path matches  | Blob object is returned with uri set to the given path.                      |
    | pathlib.Path or   | http(s)/s3/gs | Nothing is uploaded.                                                         |
    | FlyteDirectory    +---------------+------------------------------------------------------------------------------+
    |                   | path matches  | Contents of file are uploaded to the Flyte blob store (S3, GCS, etc.), in    |
    |                   | /local/path   | a bucket determined by the raw_output_data_prefix setting. If                |
    |                   |               | remote_path is given, then that is used instead of the random path. Blob     |
    |                   |               | object is returned with uri pointing to the blob store location.             |
    |                   |               |                                                                              |
    +-------------------+---------------+------------------------------------------------------------------------------+

    As inputs ::

        def t1(in1: FlyteDirectory):
            ...

        def t1(in1: FlyteDirectory["svg"]):
            ...

    As outputs:

    The contents of this local directory will be uploaded to the Flyte store. ::

        return FlyteDirectory("/path/to/dir/")

        return FlyteDirectory["svg"]("/path/to/dir/", remote_path="s3://special/output/location")

    Similar to the FlyteFile example, if you give an already remote location, it will not be copied to Flyte's
    durable store, the uri will just be stored as is. ::

        return FlyteDirectory("s3://some/other/folder")

    Note if you write a path starting with http/s, if anything ever tries to read it (i.e. use the literal
    as an input, it'll fail because the http proxy doesn't know how to download whole directories.

    The format [] bit is still there because in Flyte, directories are stored as Blob Types also, just like files, and
    the Blob type has the format field. The difference in the type field is represented in the ``dimensionality``
    field in the ``BlobType``.
    """

    def __init__(self, path: typing.Union[str, os.PathLike], downloader: typing.Callable = None, remote_directory=None):
        """
        :param path: The source path that users are expected to call open() on
        :param downloader: Optional function that can be passed that used to delay downloading of the actual fil
            until a user actually calls open().
        :param remote_directory: If the user wants to return something and also specify where it should be uploaded to.
        """
        # Make this field public, so that the dataclass transformer can set a value for it
        # https://github.com/flyteorg/flytekit/blob/bcc8541bd6227b532f8462563fe8aac902242b21/flytekit/core/type_engine.py#L298
        self.path = path
        self._downloader = downloader or noop
        self._downloaded = False
        self._remote_directory = remote_directory
        self._remote_source = None

    def __fspath__(self):
        """
        This function should be called by os.listdir as well.
        """
        if not self._downloaded:
            self._downloader()
            self._downloaded = True
        return self.path

    @classmethod
    def extension(cls) -> str:
        return ""

    def __class_getitem__(cls, item: typing.Union[typing.Type, str]) -> typing.Type[FlyteDirectory]:
        if item is None:
            return cls
        item_string = str(item)
        item_string = item_string.strip().lstrip("~").lstrip(".")
        if item_string == "":
            return cls

        class _SpecificFormatDirectoryClass(FlyteDirectory):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteDirectory

            @classmethod
            def extension(cls) -> str:
                return item_string

        return _SpecificFormatDirectoryClass

    @property
    def downloaded(self) -> bool:
        return self._downloaded

    @property
    def remote_directory(self) -> typing.Optional[str]:
        return self._remote_directory

    @property
    def remote_source(self) -> str:
        """
        If this is an input to a task, and the original path is s3://something, flytekit will download the
        directory for the user. In case the user wants access to the original path, it will be here.
        """
        return typing.cast(str, self._remote_source)

    def download(self) -> str:
        return self.__fspath__()

    def __repr__(self):
        return self.path

    def __str__(self):
        return self.path


class FlyteDirToMultipartBlobTransformer(TypeTransformer[FlyteDirectory]):
    """
    This transformer handles conversion between the Python native FlyteDirectory class defined above, and the Flyte
    IDL literal/type of Multipart Blob. Please see the FlyteDirectory comments for additional information.

    .. caution:

       The transformer will not check if the given path is actually a directory. This is because the path could be
       a remote reference.

    """

    def __init__(self):
        super().__init__(name="FlyteDirectory", t=FlyteDirectory)

    @staticmethod
    def get_format(t: typing.Type[FlyteDirectory]) -> str:
        return t.extension()

    @staticmethod
    def _blob_type(format: str) -> _core_types.BlobType:
        return _core_types.BlobType(format=format, dimensionality=_core_types.BlobType.BlobDimensionality.MULTIPART)

    def assert_type(self, t: typing.Type[FlyteDirectory], v: typing.Union[FlyteDirectory, os.PathLike, str]):
        if isinstance(v, FlyteDirectory) or isinstance(v, str) or isinstance(v, os.PathLike):
            """
            NOTE: we do not do a isdir check because the given path could be remote reference
            """
            return
        raise TypeError(
            f"No automatic conversion from {type(v)} declared type {t} to FlyteDirectory found."
            f" Use (FlyteDirectory, str, os.PathLike)"
        )

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
        meta = BlobMetadata(type=self._blob_type(format=self.get_format(python_type)))

        # There are two kinds of literals we handle, either an actual FlyteDirectory, or a string path to a directory.
        # Handle the FlyteDirectory case
        if isinstance(python_val, FlyteDirectory):
            # If the object has a remote source, then we just convert it back.
            if python_val._remote_source is not None:
                return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=python_val._remote_source)))

            source_path = python_val.path
            # If the user specified the remote_directory to be False, that means no matter what, do not upload. Also if the
            # path given is already a remote path, say https://www.google.com, the concept of uploading to the Flyte
            # blob store doesn't make sense.
            if python_val.remote_directory is False or ctx.file_access.is_remote(source_path):
                should_upload = False

            # Set the remote destination if one was given instead of triggering a random one below
            remote_directory = python_val.remote_directory or None

        # Handle the string case
        elif isinstance(python_val, pathlib.Path) or isinstance(python_val, str):
            source_path = str(python_val)

            if ctx.file_access.is_remote(source_path):
                should_upload = False
            else:
                p = Path(source_path)
                if not p.is_dir():
                    raise ValueError(f"Expected a directory. {source_path} is not a directory")
        else:
            raise AssertionError(f"Expected FlyteDirectory or os.PathLike object, received {type(python_val)}")

        # If we're uploading something, that means that the uri should always point to the upload destination.
        if should_upload:
            if remote_directory is None:
                remote_directory = ctx.file_access.get_random_remote_directory()
            ctx.file_access.put_data(source_path, remote_directory, is_multipart=True)
            return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_directory)))

        # If not uploading, then we can only take the original source path as the uri.
        else:
            return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=source_path)))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: typing.Type[FlyteDirectory]
    ) -> FlyteDirectory:

        uri = lv.scalar.blob.uri

        # This is a local file path, like /usr/local/my_dir, don't mess with it. Certainly, downloading it doesn't
        # make any sense.
        if not ctx.file_access.is_remote(uri):
            return expected_python_type(uri)

        # For the remote case, return an FlyteDirectory object that can download
        local_folder = ctx.file_access.get_random_local_directory()

        def _downloader():
            return ctx.file_access.get_data(uri, local_folder, is_multipart=True)

        expected_format = self.get_format(expected_python_type)

        fd = FlyteDirectory.__class_getitem__(expected_format)(local_folder, _downloader)
        fd._remote_source = uri

        return fd

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[FlyteDirectory[typing.Any]]:
        if (
            literal_type.blob is not None
            and literal_type.blob.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART
        ):
            return FlyteDirectory.__class_getitem__(literal_type.blob.format)
        raise ValueError(f"Transformer {self} cannot reverse {literal_type}")


TypeEngine.register(FlyteDirToMultipartBlobTransformer())
