from __future__ import annotations

import typing

from flyteidl.core import types_pb2 as _types_pb2

from flytekit.models import common as _common


class EnumType(_common.FlyteIdlEntity):
    """
    Models _types_pb2.EnumType
    """

    def __init__(self, values: typing.List[str]):
        self._values = values

    @property
    def values(self) -> typing.List[str]:
        return self._values

    def to_flyte_idl(self) -> _types_pb2.EnumType:
        return _types_pb2.EnumType(
            values=self._values if self._values else [],
        )

    @classmethod
    def from_flyte_idl(cls, proto: _types_pb2.EnumType):
        return cls(values=proto.values)


class BlobType(_common.FlyteIdlEntity):
    """
    This type represents offloaded data and is typically used for things like files.
    """

    class BlobDimensionality(object):
        SINGLE = _types_pb2.BlobType.SINGLE
        MULTIPART = _types_pb2.BlobType.MULTIPART

    def __init__(self, format, dimensionality, file_extension="", enable_legacy_filename=False):
        """
        :param Text format: A string describing the format of the underlying blob data.
        :param int dimensionality: An integer from BlobType.BlobDimensionality enum
        :param Text file_extension: The file extension (e.g. "csv", "parquet") to use
            during copilot download, e.g. "csv", "parquet". Empty by default.
        :param bool enable_legacy_filename: When True and file_extension is set, the copilot
            download phase writes the blob to both the extended path and the base path.
        """
        self._format = format
        self._dimensionality = dimensionality
        self._file_extension = file_extension
        self._enable_legacy_filename = enable_legacy_filename

    @property
    def format(self):
        """
        A string describing the format of the underlying blob data.
        :rtype: Text
        """
        return self._format

    @property
    def dimensionality(self):
        """
        An integer from BlobType.BlobDimensionality enum
        :rtype: int
        """
        return self._dimensionality

    @property
    def file_extension(self):
        """
        The file extension (e.g. "csv", "parquet") to use during copilot download.
        Default is "", which means no extension is appended.
        :rtype: Text
        """
        return self._file_extension

    @property
    def enable_legacy_filename(self):
        """
        When True and file_extension is set, the copilot download writes the blob to
        both the full path (with extension) and the old path (without extension).
        :rtype: bool
        """
        return self._enable_legacy_filename

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.types_pb2.BlobType
        """
        return _types_pb2.BlobType(
            format=self.format,
            dimensionality=self.dimensionality,
            file_extension=self._file_extension,
            enable_legacy_filename=self._enable_legacy_filename,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.types_pb2.BlobType proto:
        :rtype: BlobType
        """
        return cls(
            format=proto.format,
            dimensionality=proto.dimensionality,
            file_extension=proto.file_extension,
            enable_legacy_filename=proto.enable_legacy_filename,
        )
