from datetime import datetime as _datetime
from datetime import timezone as _timezone
from typing import Dict, Optional

import flyteidl_rust as flyteidl
from flyteidl.core import literals_pb2 as _literals_pb2
from google.protobuf.struct_pb2 import Struct

from flytekit.exceptions import user as _user_exceptions
from flytekit.models import common as _common
from flytekit.models import utils
from flytekit.models.core import types as _core_types
from flytekit.models.types import Error, StructuredDatasetType
from flytekit.models.types import LiteralType as _LiteralType
from flytekit.models.types import OutputReference as _OutputReference
from flytekit.models.types import SchemaType as _SchemaType


class RetryStrategy(_common.FlyteIdlEntity):
    def __init__(self, retries):
        """
        :param int retries: Number of retries to attempt on recoverable failures.  If retries is 0, then
            only one attempt will be made.
        """
        self._retries = retries

    @property
    def retries(self):
        """
        Number of retries to attempt on recoverable failures.  If retries is 0, then only one attempt will be made.
        :rtype: int
        """
        return self._retries

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.RetryStrategy
        """
        return flyteidl.core.RetryStrategy(retries=self.retries)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.RetryStrategy pb2_object:
        :rtype: RetryStrategy
        """
        return cls(retries=pb2_object.retries)


class Primitive(_common.FlyteIdlEntity):
    def __init__(
        self,
        integer=None,
        float_value=None,
        string_value=None,
        boolean=None,
        datetime=None,
        duration=None,
    ):
        """
        This object proxies the primitives supported by the Flyte IDL system.  Only one value can be set.
        :param int integer: [Optional]
        :param float float_value: [Optional]
        :param Text string_value: [Optional]
        :param bool boolean: [Optional]
        :param datetime.timestamp datetime: [Optional]
        :param datetime.timedelta duration: [Optional]
        """
        self._integer = integer
        self._float_value = float_value
        self._string_value = string_value
        self._boolean = boolean
        if datetime is None:
            self._datetime = None
        elif isinstance(datetime, _datetime):
            self._datetime = datetime
        else:  # TODO Check for timestamp type?
            self._datetime = _datetime.utcfromtimestamp(datetime.seconds)
        self._duration = duration

    @property
    def integer(self):
        """
        :rtype: int
        """
        return self._integer

    @property
    def float_value(self):
        """
        :rtype: float
        """
        return self._float_value

    @property
    def string_value(self):
        """
        :rtype: Text
        """
        return self._string_value

    @property
    def boolean(self):
        """
        :rtype: bool
        """
        return self._boolean

    @property
    def datetime(self):
        """
        :rtype: datetime.datetime
        """
        if self._datetime is None or self._datetime.tzinfo is not None:
            return self._datetime
        return self._datetime.replace(tzinfo=_timezone.utc)

    @property
    def duration(self):
        """
        :rtype: datetime.timedelta
        """
        return self._duration

    @property
    def value(self):
        """
        This returns whichever field is set.
        :rtype: T
        """
        for value in [
            self.integer,
            self.float_value,
            self.string_value,
            self.boolean,
            self.datetime,
            self.duration,
        ]:
            if value is not None:
                return value

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Primitive
        """
        value = None
        if self.string_value:
            value = flyteidl.primitive.Value.StringValue(self.string_value)
        elif self.integer:
            value = flyteidl.primitive.Value.Integer(self.integer)
        elif self.float_value:
            value = flyteidl.primitive.Value.FloatValue(self.float_value)
        elif self.boolean:
            value = flyteidl.primitive.Value.Boolean(self.string_value)
        elif self.datetime:
            # Convert to UTC and remove timezone so protobuf behaves.
            value = flyteidl.primitive.Value.Datetime(
                flyteidl.protobuf.Timestamp(seconds=self.datetime.second, nanos=0)
            )
        if self.duration is not None:
            value = flyteidl.primitive.Value.Duration(self.duration)
        return flyteidl.core.Primitive(value)

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.literals_pb2.Primitive proto:
        :rtype: Primitive
        """
        return cls(
            integer=proto.value[0] if isinstance(proto.value, flyteidl.primitive.Value.Integer) else None,
            float_value=proto.value[0] if isinstance(proto.value, flyteidl.primitive.Value.FloatValue) else None,
            string_value=proto.value[0] if isinstance(proto.value, flyteidl.primitive.Value.StringValue) else None,
            boolean=proto.value[0] if isinstance(proto.value, flyteidl.primitive.Value.Boolean) else None,
            datetime=utils.convert_to_datetime(seconds=proto.value[0].seconds, nanos=proto.value[0].nanos)
            if isinstance(proto.value, flyteidl.primitive.Value.Datetime)
            else None,
            duration=utils.convert_to_datetime(proto.value[0])  # TODO
            if isinstance(proto.value, flyteidl.primitive.Value.Duration)
            else None,
        )


class Binary(_common.FlyteIdlEntity):
    def __init__(self, value, tag):
        """
        :param bytes value:
        :param Text tag:
        """
        self._value = value
        self._tag = tag

    @property
    def value(self):
        """
        :rtype: bytes
        """
        return self._value

    @property
    def tag(self):
        """
        :rtype: Text
        """
        return self._tag

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Binary
        """
        return flyteidl.core.Binary(value=self.value, tag=self.tag)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.Binary pb2_object:
        :rtype: Binary
        """
        return cls(value=pb2_object.value, tag=pb2_object.tag)


class BlobMetadata(_common.FlyteIdlEntity):
    """
    This is metadata for the Blob literal.
    """

    def __init__(self, type):
        """
        :param flytekit.models.core.types.BlobType type: The type of the underlying blob
        """
        self._type = type

    @property
    def type(self):
        """
        :rtype: flytekit.models.core.types.BlobType
        """
        return self._type

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.BlobMetadata
        """
        return flyteidl.core.BlobMetadata(type=self.type.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.literals_pb2.BlobMetadata proto:
        :rtype: BlobMetadata
        """
        return cls(type=_core_types.BlobType.from_flyte_idl(proto.type))


class Blob(_common.FlyteIdlEntity):
    def __init__(self, metadata, uri):
        """
        This literal model is used to represent binary data offloaded to some storage location which is
        identifiable with a unique string. See :py:class:`flytekit.FlyteFile` as an example.

        :param BlobMetadata metadata:
        :param Text uri: The location of this blob
        """
        self._metadata = metadata
        self._uri = uri

    @property
    def uri(self):
        """
        :rtype: Text
        """
        return self._uri

    @property
    def metadata(self):
        """
        :rtype: BlobMetadata
        """
        return self._metadata

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Blob
        """
        return flyteidl.core.Blob(metadata=self.metadata.to_flyte_idl(), uri=self.uri)

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.literals_pb2.Blob proto:
        :rtype: Blob
        """
        return cls(metadata=BlobMetadata.from_flyte_idl(proto.metadata), uri=proto.uri)


class Void(_common.FlyteIdlEntity):
    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Void
        """
        return flyteidl.core.Void()

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core.literals_pb2.Void proto:
        :rtype: Void
        """
        return cls()


class BindingDataMap(_common.FlyteIdlEntity):
    def __init__(self, bindings):
        """
        A map of BindingData items.  Can be a recursive structure

        :param dict[string, BindingData] bindings: Map of strings to Bindings
        """
        self._bindings = bindings

    @property
    def bindings(self):
        """
        Map of strings to Bindings
        :rtype: dict[string, BindingData]
        """
        return self._bindings

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.BindingDataMap
        """
        return _literals_pb2.BindingDataMap(bindings={k: v.to_flyte_idl() for (k, v) in self.bindings.items()})

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.BindingDataMap pb2_object:
        :rtype: flytekit.models.literals.BindingDataMap
        """

        return cls({k: BindingData.from_flyte_idl(v) for (k, v) in pb2_object.bindings.items()})


class BindingDataCollection(_common.FlyteIdlEntity):
    def __init__(self, bindings):
        """
        A list of BindingData items.

        :param list[BindingData] bindings:
        """
        self._bindings = bindings

    @property
    def bindings(self):
        """
        :rtype: list[BindingData]
        """
        return self._bindings

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.BindingDataCollection
        """
        return flyteidl.core.BindingDataCollection(bindings=[b.to_flyte_idl() for b in self.bindings])

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.BindingDataCollection pb2_object:
        :rtype: flytekit.models.literals.BindingDataCollection
        """
        return cls([BindingData.from_flyte_idl(b) for b in pb2_object.bindings])


class BindingData(_common.FlyteIdlEntity):
    def __init__(self, scalar=None, collection=None, promise=None, map=None):
        """
        Specifies either a simple value or a reference to another output. Only one of the input arguments may be
        specified.

        :param Scalar scalar: [Optional] A simple scalar value.
        :param BindingDataCollection collection: [Optional] A collection of binding data. This allows nesting of
            binding data to any number of levels.
        :param flytekit.models.types.OutputReference promise: [Optional] References an output promised by another node.
        :param BindingDataMap map: [Optional] A map of bindings. The key is always a string.
        """
        self._scalar = scalar
        self._collection = collection
        self._promise = promise
        self._map = map

    @property
    def scalar(self):
        """
        A simple scalar value.
        :rtype: Scalar
        """
        return self._scalar

    @property
    def collection(self):
        """
        [Optional] A collection of binding data. This allows nesting of binding data to any number of levels.
        :rtype: BindingDataCollection
        """
        return self._collection

    @property
    def promise(self):
        """
        [Optional] References an output promised by another node.
        :rtype: flytekit.models.types.OutputReference
        """
        return self._promise

    @property
    def map(self):
        """
        [Optional] A map of bindings. The key is always a string.
        :rtype: BindingDataMap
        """
        return self._map

    @property
    def value(self):
        """
        Returns whichever value is set
        :rtype: T
        """
        return self.scalar or self.collection or self.promise or self.map

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.BindingData
        """
        value = None
        if self.scalar:
            value = flyteidl.binding_data.Value.Scalar(self.scalar.to_flyte_idl())
        if self.collection:
            value = flyteidl.binding_data.Value.Collection(self.collection.to_flyte_idl())
        if self.promise:
            value = flyteidl.binding_data.Value.Promise(self.promise.to_flyte_idl())
        if self.map:
            value = flyteidl.binding_data.Value.Map(self.map.to_flyte_idl())
        return flyteidl.core.BindingData(value=value)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.BindingData pb2_object:
        :return: BindingData
        """

        return cls(
            scalar=Scalar.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object, flyteidl.binding_data.Value.Scalar)
            else None,
            collection=BindingDataCollection.from_flyte_idl(pb2_object.collection)
            if isinstance(pb2_object, flyteidl.binding_data.Value.Collection)
            else None,
            promise=_OutputReference.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object, flyteidl.binding_data.Value.Promise)
            else None,
            map=BindingDataMap.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object, flyteidl.binding_data.Value.Map)
            else None,
        )

    def to_literal_model(self):
        """
        Converts current binding data into a Literal asserting that there are no promises in the bindings.
        :rtype: Literal
        """
        if self.promise:
            raise _user_exceptions.FlyteValueException(
                self.promise,
                "Cannot convert BindingData to a Literal because " "it has a promise.",
            )
        elif self.scalar:
            return Literal(scalar=self.scalar)
        elif self.collection:
            return Literal(
                collection=LiteralCollection(
                    literals=[binding.to_literal_model() for binding in self.collection.bindings]
                )
            )
        elif self.map:
            return Literal(
                map=LiteralMap(literals={k: binding.to_literal_model() for k, binding in self.map.bindings.items()})
            )


class Binding(_common.FlyteIdlEntity):
    def __init__(self, var, binding):
        """
        An input/output binding of a variable to either static value or a node output.

        :param Text var: A variable name, must match an input or output variable of the node.
        :param BindingData binding: Data to use to bind this variable.
        """
        self._var = var
        self._binding = binding

    @property
    def var(self):
        """
        A variable name, must match an input or output variable of the node.
        :rtype: Text
        """
        return self._var

    @property
    def binding(self):
        """
        Data to use to bind this variable.
        :rtype: BindingData
        """
        return self._binding

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Binding
        """
        return flyteidl.core.Binding(var=self.var, binding=self.binding.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.Binding pb2_object:
        :rtype: flytekit.core.models.literals.Binding
        """
        return cls(pb2_object.var, BindingData.from_flyte_idl(pb2_object.binding))


class Schema(_common.FlyteIdlEntity):
    def __init__(self, uri, type):
        """
        A strongly typed schema that defines the interface of data retrieved from the underlying storage medium.

        :param Text uri:
        :param flytekit.models.types.SchemaType type:
        """
        self._uri = uri
        self._type = type

    @property
    def uri(self):
        """
        :rtype: Text
        """
        return self._uri

    @property
    def type(self):
        """
        :rtype: flytekit.models.types.SchemaType
        """
        return self._type

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Schema
        """
        return flyteidl.core.Schema(uri=self.uri, type=self.type.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.Schema pb2_object:
        :rtype: Schema
        """
        return cls(uri=pb2_object.uri, type=_SchemaType.from_flyte_idl(pb2_object.type))


class Union(_common.FlyteIdlEntity):
    def __init__(self, value, stored_type):
        """
        The runtime representation of a tagged union value. See `UnionType` for more details.

        :param flytekit.models.literals.Literal value:
        :param flytekit.models.types.LiteralType stored_type:
        """
        self._value = value
        self._type = stored_type

    @property
    def value(self):
        """
        :rtype: flytekit.models.literals.Literal
        """
        return self._value

    @property
    def stored_type(self):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return self._type

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Union
        """
        return flyteidl.core.Union(value=self.value.to_flyte_idl(), type=self._type.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.Schema pb2_object:
        :rtype: Schema
        """
        return cls(
            value=Literal.from_flyte_idl(pb2_object.value), stored_type=_LiteralType.from_flyte_idl(pb2_object.type)
        )


class StructuredDatasetMetadata(_common.FlyteIdlEntity):
    def __init__(self, structured_dataset_type: Optional[StructuredDatasetType] = None):
        self._structured_dataset_type = structured_dataset_type

    @property
    def structured_dataset_type(self) -> StructuredDatasetType:
        return self._structured_dataset_type

    def to_flyte_idl(self) -> flyteidl.core.StructuredDatasetMetadata:
        return flyteidl.core.StructuredDatasetMetadata(
            structured_dataset_type=self.structured_dataset_type.to_flyte_idl()
            if self._structured_dataset_type
            else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: flyteidl.core.StructuredDatasetMetadata) -> "StructuredDatasetMetadata":
        return cls(
            structured_dataset_type=StructuredDatasetType.from_flyte_idl(pb2_object.structured_dataset_type),
        )


class StructuredDataset(_common.FlyteIdlEntity):
    def __init__(self, uri: str, metadata: Optional[StructuredDatasetMetadata] = None):
        """
        A strongly typed schema that defines the interface of data retrieved from the underlying storage medium.
        """
        self._uri = uri
        self._metadata = metadata

    @property
    def uri(self) -> str:
        return self._uri

    @property
    def metadata(self) -> Optional[StructuredDatasetMetadata]:
        return self._metadata

    def to_flyte_idl(self) -> flyteidl.core.StructuredDataset:
        return flyteidl.core.StructuredDataset(
            uri=self.uri, metadata=self.metadata.to_flyte_idl() if self.metadata else None
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: flyteidl.core.StructuredDataset) -> "StructuredDataset":
        return cls(uri=pb2_object.uri, metadata=StructuredDatasetMetadata.from_flyte_idl(pb2_object.metadata))


class LiteralCollection(_common.FlyteIdlEntity):
    def __init__(self, literals):
        """
        :param list[Literal] literals: underlying list of literals in this collection.
        """
        self._literals = literals

    @property
    def literals(self):
        """
        :rtype: list[Literal]
        """
        return self._literals

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.LiteralCollection
        """
        return flyteidl.core.LiteralCollection(literals=[l.to_flyte_idl() for l in self.literals])

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.LiteralCollection pb2_object:
        :rtype: LiteralCollection
        """
        return cls([Literal.from_flyte_idl(l) for l in pb2_object.literals])


class LiteralMap(_common.FlyteIdlEntity):
    def __init__(self, literals):
        """
        :param dict[Text, Literal] literals: A dictionary mapping Text key names to Literal objects.
        """
        self._literals = literals

    @property
    def literals(self):
        """
        A dictionary mapping Text key names to Literal objects.
        :rtype: dict[Text, Literal]
        """
        return self._literals

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.LiteralMap
        """
        return flyteidl.core.LiteralMap(literals={k: v.to_flyte_idl() for k, v in self.literals.items()})

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.LiteralMap pb2_object:
        :rtype: LiteralMap
        """
        return cls({k: Literal.from_flyte_idl(v) for k, v in pb2_object.literals.items()})


class Scalar(_common.FlyteIdlEntity):
    def __init__(
        self,
        primitive: Primitive = None,
        blob: Blob = None,
        binary: Binary = None,
        schema: Schema = None,
        union: Union = None,
        none_type: Void = None,
        error: Error = None,
        generic: Struct = None,
        structured_dataset: StructuredDataset = None,
    ):
        """
        Scalar wrapper around Flyte types.  Only one can be specified.

        :param Primitive primitive:
        :param Blob blob:
        :param Binary binary:
        :param Schema schema:
        :param Void none_type:
        :param Error error:
        :param google.protobuf.struct_pb2.Struct generic:
        :param StructuredDataset structured_dataset:
        """

        self._primitive = primitive
        self._blob = blob
        self._binary = binary
        self._schema = schema
        self._union = union
        self._none_type = none_type
        self._error = error
        self._generic = generic
        self._structured_dataset = structured_dataset

    @property
    def primitive(self):
        """
        :rtype: Primitive
        """
        return self._primitive

    @property
    def blob(self):
        """
        :rtype: Blob
        """
        return self._blob

    @property
    def binary(self):
        """
        :rtype: Binary
        """
        return self._binary

    @property
    def schema(self):
        """
        :rtype: Schema
        """
        return self._schema

    @property
    def union(self):
        """
        :rtype: Union
        """
        return self._union

    @property
    def none_type(self):
        """
        :rtype: Void
        """
        return self._none_type

    @property
    def error(self):
        """
        :rtype: Error
        """
        return self._error

    @property
    def generic(self):
        """
        :rtype: google.protobuf.struct_pb2.Struct
        """
        return self._generic

    @property
    def structured_dataset(self) -> StructuredDataset:
        return self._structured_dataset

    @property
    def value(self):
        """
        Returns whichever value is set
        :rtype: T
        """
        return (
            self.primitive
            or self.blob
            or self.binary
            or self.schema
            or self.union
            or self.none_type
            or self.error
            or self.generic
            or self.structured_dataset
        )

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Scalar
        """

        value = None
        if self.primitive is not None:
            value = flyteidl.scalar.Value.Primitive(self.primitive.to_flyte_idl())
        elif self.blob is not None:
            value = flyteidl.scalar.Value.Blob(self.blob.to_flyte_idl())
        elif self.binary is not None:
            value = flyteidl.scalar.Value.Binary(self.binary.to_flyte_idl())
        elif self.schema is not None:
            value = flyteidl.scalar.Value.Schema(self.schema.to_flyte_idl())
        elif self.union is not None:
            value = flyteidl.scalar.Value.Union(self.union.to_flyte_idl())
        elif self.none_type is not None:
            value = flyteidl.scalar.Value.NoneType(self.none_type.to_flyte_idl())
        elif self.error is not None:
            value = flyteidl.scalar.Value.Error(self.error.to_flyte_idl())
        elif self.generic is not None:
            value = flyteidl.scalar.Value.Generic(self.generic.to_flyte_idl())
        elif self.structured_dataset is not None:
            value = flyteidl.scalar.Value.StructuredDataset(self.structured_dataset.to_flyte_idl())
        return flyteidl.core.Scalar(value)

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.Scalar pb2_object:
        :rtype: flytekit.models.literals.Scalar
        """
        return cls(
            # TODO:
            primitive=Primitive.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.Primitive)
            else None,
            blob=Blob.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.Blob)
            else None,
            binary=Binary.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.Binary)
            else None,
            schema=Schema.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.Schema)
            else None,
            union=Union.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.Union)
            else None,
            none_type=Void.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.NoneType)
            else None,
            error=pb2_object.value[0] if isinstance(pb2_object.value, flyteidl.scalar.Value.Error) else None,
            generic=pb2_object.value[0] if isinstance(pb2_object.value, flyteidl.scalar.Value.Generic) else None,
            structured_dataset=StructuredDataset.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.scalar.Value.StructuredDataset)
            else None,
        )


class Literal(_common.FlyteIdlEntity):
    def __init__(
        self,
        scalar: Optional[Scalar] = None,
        collection: Optional[LiteralCollection] = None,
        map: Optional[LiteralMap] = None,
        hash: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        """
        This IDL message represents a literal value in the Flyte ecosystem.

        :param Scalar scalar:
        :param LiteralCollection collection:
        :param LiteralMap map:
        """
        self._scalar = scalar
        self._collection = collection
        self._map = map
        self._hash = hash
        self._metadata = metadata

    @property
    def scalar(self):
        """
        If not None, this value holds a scalar value which can be further unpacked.
        :rtype: Scalar
        """
        return self._scalar

    @property
    def collection(self):
        """
        If not None, this value holds a collection of Literal values which can be further unpacked.
        :rtype: LiteralCollection
        """
        return self._collection

    @property
    def map(self):
        """
        If not None, this value holds a map of Literal values which can be further unpacked.
        :rtype: LiteralMap
        """
        return self._map

    @property
    def value(self):
        """
        Returns one of the scalar, collection, or map properties based on which one is set.
        :rtype: T
        """
        return self.scalar or self.collection or self.map

    @property
    def hash(self):
        """
        If not None, this value holds a hash that represents the literal for caching purposes.
        :rtype: str
        """
        return self._hash

    @hash.setter
    def hash(self, value):
        self._hash = value

    @property
    def metadata(self) -> Optional[Dict[str, str]]:
        """
        This value holds metadata about the literal.
        """
        return self._metadata

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.literals_pb2.Literal
        """
        value = None
        if self.scalar:
            value = flyteidl.literal.Value.Scalar(self.scalar.to_flyte_idl())
        elif self.collection:
            value = flyteidl.literal.Value.Collection(self.collection.to_flyte_idl())
        elif self.map:
            value = flyteidl.literal.Value.Map(self.map.to_flyte_idl())
        return flyteidl.core.Literal(
            value=value,
            hash=self.hash or "",
            metadata=self.metadata or {},
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param flyteidl.core.literals_pb2.Literal pb2_object:
        :rtype: Literal
        """
        collection = None
        if isinstance(pb2_object.value, flyteidl.literal.Value.Collection):
            collection = LiteralCollection.from_flyte_idl(pb2_object.value[0])

        return cls(
            scalar=Scalar.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.literal.Value.Scalar)
            else None,
            collection=collection,
            map=LiteralMap.from_flyte_idl(pb2_object.value[0])
            if isinstance(pb2_object.value, flyteidl.literal.Value.Map)
            else None,
            hash=pb2_object.hash if pb2_object.hash else None,
            metadata={k: v for k, v in pb2_object.metadata.items()} if pb2_object.metadata else None,
        )

    def set_metadata(self, metadata: Dict[str, str]):
        """
        Note: This is a mutation on the literal
        :param Dict[str, str] metadata: Metadata to be added
        """
        self._metadata = metadata
