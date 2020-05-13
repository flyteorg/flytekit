from __future__ import absolute_import

from flyteidl.plugins import raw_container_pb2 as _raw

from flytekit.models import common as _common


class CoPilot(_common.FlyteIdlEntity):
    METADATA_FORMAT_PROTO = _raw.CoPilot.METADATA_FORMAT_PROTO
    METADATA_FORMAT_JSON = _raw.CoPilot.METADATA_FORMAT_JSON
    METADATA_FORMAT_YAML = _raw.CoPilot.METADATA_FORMAT_YAML
    _METADATA_FORMATS = frozenset([METADATA_FORMAT_JSON, METADATA_FORMAT_PROTO, METADATA_FORMAT_YAML])

    def __init__(self, input_path: str, output_path: str, metadata_format: str):
        if metadata_format not in self._METADATA_FORMATS:
            raise ValueError("Metadata format {} not supported. Should be one of {}".format(metadata_format, self._METADATA_FORMATS))
        self._input_path = input_path
        self._output_path = output_path
        self._metadata_format = metadata_format

    @property
    def input_path(self) -> str:
        return self._input_path

    @property
    def output_path(self) -> str:
        return self._output_path

    @property
    def metadata_format(self) -> str:
        return self._metadata_format

    def to_flyte_idl(self) -> _raw.CoPilot:
        return _raw.CoPilot(
            input_path=self._input_path,
            output_path=self._output_path,
            metadata_format=self._metadata_format,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _raw.CoPilot):
        """
        :param _presto.PrestoQuery pb2_object:
        :return: PrestoQuery
        """
        return cls(
            input_path=pb2_object.input_path,
            output_path=pb2_object.output_path,
            metadata_format=pb2_object.metadata_format,
        )