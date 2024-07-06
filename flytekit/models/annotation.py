import json
from typing import Any, Dict

import flyteidl_rust as flyteidl
from google.protobuf import json_format as _json_format


class TypeAnnotation:
    """Python class representation of the flyteidl TypeAnnotation message."""

    def __init__(self, annotations: Dict[str, Any]):
        self._annotations = annotations

    @property
    def annotations(self) -> Dict[str, Any]:
        """
        :rtype: dict[str, Any]
        """
        return self._annotations

    def to_flyte_idl(self) -> flyteidl.core.TypeAnnotation:
        """
        :rtype: flyteidl.core.types_pb2.TypeAnnotation
        """
        if self._annotations is not None:
            annotations = _json_format.Parse(json.dumps(self.annotations), flyteidl.protobuf.Struct())
        else:
            annotations = None

        return flyteidl.core.TypeAnnotation(
            annotations=annotations,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.types_pb2.TypeAnnotation proto:
        :rtype: TypeAnnotation
        """

        return cls(annotations=_json_format.MessageToDict(proto.annotations))

    def __eq__(self, x: object) -> bool:
        if not isinstance(x, self.__class__):
            return False
        return self.annotations == x.annotations
