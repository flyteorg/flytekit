import json as _json

from flyteidl.core import types_pb2 as _types_pb2
from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit.models import common as _common
from flytekit.models.core import types as _core_types


class SimpleType(object):
    NONE = _types_pb2.NONE
    INTEGER = _types_pb2.INTEGER
    FLOAT = _types_pb2.FLOAT
    STRING = _types_pb2.STRING
    BOOLEAN = _types_pb2.BOOLEAN
    DATETIME = _types_pb2.DATETIME
    DURATION = _types_pb2.DURATION
    BINARY = _types_pb2.BINARY
    ERROR = _types_pb2.ERROR
    STRUCT = _types_pb2.STRUCT


class SchemaType(_common.FlyteIdlEntity):
    class SchemaColumn(_common.FlyteIdlEntity):
        class SchemaColumnType(object):
            INTEGER = _types_pb2.SchemaType.SchemaColumn.INTEGER
            FLOAT = _types_pb2.SchemaType.SchemaColumn.FLOAT
            STRING = _types_pb2.SchemaType.SchemaColumn.STRING
            DATETIME = _types_pb2.SchemaType.SchemaColumn.DATETIME
            DURATION = _types_pb2.SchemaType.SchemaColumn.DURATION
            BOOLEAN = _types_pb2.SchemaType.SchemaColumn.BOOLEAN

        def __init__(self, name, type):
            """
            :param Text name: Name for the column
            :param int type: Enum type from SchemaType.SchemaColumn.SchemaColumnType representing the type of the column
            """
            self._name = name
            self._type = type

        @property
        def name(self):
            """
            Name for the column
            :rtype: Text
            """
            return self._name

        @property
        def type(self):
            """
            Enum type from SchemaType.SchemaColumn.SchemaColumnType representing the type of the column
            :rtype: int
            """
            return self._type

        def to_flyte_idl(self):
            """
            :rtype: flyteidl.core.types_pb2.SchemaType.SchemaColumn
            """
            return _types_pb2.SchemaType.SchemaColumn(name=self.name, type=self.type)

        @classmethod
        def from_flyte_idl(cls, proto):
            """
            :param flyteidl.core.types_pb2.SchemaType.SchemaColumn proto:
            :rtype: SchemaType.SchemaColumn
            """
            return cls(name=proto.name, type=proto.type)

    def __init__(self, columns):
        """
        :param list[SchemaType.SchemaColumn] columns: A list of columns defining the underlying data frame.
        """
        self._columns = columns

    @property
    def columns(self):
        """
        A list of columns defining the underlying data frame.
        :rtype: list[SchemaType.SchemaColumn]
        """
        return self._columns

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.types_pb2.SchemaType
        """
        return _types_pb2.SchemaType(columns=[c.to_flyte_idl() for c in self.columns])

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.types_pb2.SchemaType proto:
        :rtype: SchemaType
        """
        return cls(columns=[SchemaType.SchemaColumn.from_flyte_idl(c) for c in proto.columns])


class LiteralType(_common.FlyteIdlEntity):
    def __init__(
        self, simple=None, schema=None, collection_type=None, map_value_type=None, blob=None, metadata=None,
    ):
        """
        Only one of the kwargs may be set.
        :param int simple: Enum type from SimpleType
        :param SchemaType schema: Type definition for a dataframe-like object.
        :param LiteralType collection_type: For list-like objects, this is the type of each entry in the list.
        :param LiteralType map_value_type: For map objects, this is the type of the value.  The key must always be a
            string.
        :param flytekit.models.core.types.BlobType blob: For blob objects, this describes the type.
        :param dict[Text, T] metadata: Additional data describing the type
        """
        self._simple = simple
        self._schema = schema
        self._collection_type = collection_type
        self._map_value_type = map_value_type
        self._blob = blob
        self._metadata = metadata

    @property
    def simple(self):
        """
        Enum type from SimpleType
        :rtype: int
        """
        return self._simple

    @property
    def schema(self):
        """
        Type definition for a dataframe-like object.
        :rtype: SchemaType
        """
        return self._schema

    @property
    def collection_type(self):
        """
        Enum type from SimpleType or SchemaType
        :rtype: LiteralType
        """
        return self._collection_type

    @property
    def map_value_type(self):
        """
        Enum type from SimpleType
        :rtype: LiteralType
        """
        return self._map_value_type

    @property
    def blob(self):
        """
        :rtype: flytekit.models.core.types.BlobType
        """
        return self._blob

    @property
    def metadata(self):
        """
        :rtype: dict[Text, T]
        """
        return self._metadata

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.types_pb2.LiteralType
        """
        if self.metadata is not None:
            metadata = _json_format.Parse(_json.dumps(self.metadata), _struct.Struct())
        else:
            metadata = None
        t = _types_pb2.LiteralType(
            simple=self.simple if self.simple is not None else None,
            schema=self.schema.to_flyte_idl() if self.schema is not None else None,
            collection_type=self.collection_type.to_flyte_idl() if self.collection_type is not None else None,
            map_value_type=self.map_value_type.to_flyte_idl() if self.map_value_type is not None else None,
            blob=self.blob.to_flyte_idl() if self.blob is not None else None,
            metadata=metadata,
        )
        return t

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.types_pb2.LiteralType proto:
        :rtype: LiteralType
        """
        collection_type = None
        map_value_type = None
        if proto.HasField("collection_type"):
            collection_type = LiteralType.from_flyte_idl(proto.collection_type)
        if proto.HasField("map_value_type"):
            map_value_type = LiteralType.from_flyte_idl(proto.map_value_type)
        return cls(
            simple=proto.simple if proto.HasField("simple") else None,
            schema=SchemaType.from_flyte_idl(proto.schema) if proto.HasField("schema") else None,
            collection_type=collection_type,
            map_value_type=map_value_type,
            blob=_core_types.BlobType.from_flyte_idl(proto.blob) if proto.HasField("blob") else None,
            metadata=_json_format.MessageToDict(proto.metadata) or None,
        )


class OutputReference(_common.FlyteIdlEntity):
    def __init__(self, node_id, var):
        """
        A reference to an output produced by a node. The type can be retrieved -and validated- from
            the underlying interface of the node.

        :param Text node_id: Node id must exist at the graph layer.
        :param Text var: Variable name must refer to an output variable for the node.
        """
        self._node_id = node_id
        self._var = var

    @property
    def node_id(self):
        """
        Node id must exist at the graph layer.
        :rtype: Text
        """
        return self._node_id

    @property
    def var(self):
        """
        Variable name must refer to an output variable for the node.
        :rtype: Text
        """
        return self._var

    @var.setter
    def var(self, var_name):
        self._var = var_name

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.types.OutputReference
        """
        return _types_pb2.OutputReference(node_id=self.node_id, var=self.var)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.types.OutputReference pb2_object:
        :rtype: OutputReference
        """
        return cls(node_id=pb2_object.node_id, var=pb2_object.var)
