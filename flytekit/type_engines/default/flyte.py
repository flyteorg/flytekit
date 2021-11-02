import importlib as _importer
from typing import Type

from flytekit.common.exceptions import system as _system_exceptions
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.common.types import blobs as _blobs
from flytekit.common.types import containers as _container_types
from flytekit.common.types import helpers as _helpers
from flytekit.common.types import primitives as _primitive_types
from flytekit.common.types import proto as _proto
from flytekit.common.types import schema as _schema
from flytekit.models import types as _literal_type_models
from flytekit.models.core import types as _core_types


def _load_type_from_tag(tag: str) -> Type:
    """
    Loads python type from tag
    """

    if "." not in tag:
        raise _user_exceptions.FlyteValueException(
            tag,
            "Protobuf tag must include at least one '.' to delineate package and object name.",
        )

    module, name = tag.rsplit(".", 1)
    try:
        pb_module = _importer.import_module(module)
    except ImportError:
        raise _user_exceptions.FlyteAssertion(
            "Could not resolve the protobuf definition @ {}.  Is the protobuf library installed?".format(module)
        )

    if not hasattr(pb_module, name):
        raise _user_exceptions.FlyteAssertion("Could not find the protobuf named: {} @ {}.".format(name, module))

    return getattr(pb_module, name)


def _proto_sdk_type_from_tag(tag):
    """
    :param Text tag:
    :rtype: _proto.Protobuf
    """
    return _proto.create_protobuf(_load_type_from_tag(tag))


def _generic_proto_sdk_type_from_tag(tag: str) -> Type[_proto.GenericProtobuf]:
    """
    :param Text tag:
    :rtype: _proto.GenericProtobuf
    """

    return _proto.create_generic(_load_type_from_tag(tag))


class FlyteDefaultTypeEngine(object):
    _SIMPLE_TYPE_LOOKUP_TABLE = {
        _literal_type_models.SimpleType.INTEGER: _primitive_types.Integer,
        _literal_type_models.SimpleType.FLOAT: _primitive_types.Float,
        _literal_type_models.SimpleType.BOOLEAN: _primitive_types.Boolean,
        _literal_type_models.SimpleType.DATETIME: _primitive_types.Datetime,
        _literal_type_models.SimpleType.DURATION: _primitive_types.Timedelta,
        _literal_type_models.SimpleType.NONE: _base_sdk_types.Void,
        _literal_type_models.SimpleType.STRING: _primitive_types.String,
        _literal_type_models.SimpleType.STRUCT: _primitive_types.Generic,
    }

    def python_std_to_sdk_type(self, t):
        """
        :param T t: User input.  Should be of the form: Types.Integer, [Types.Integer], {Types.String:
        Types.Integer}, etc.
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        if isinstance(t, list):
            if len(t) != 1:
                raise _user_exceptions.FlyteAssertion(
                    "When specifying a list type, there must be exactly one element in "
                    "the list describing the contained type."
                )
            return _container_types.List(_helpers.python_std_to_sdk_type(t[0]))
        elif isinstance(t, dict):
            raise _user_exceptions.FlyteAssertion("Map types are not yet implemented.")
        elif isinstance(t, _base_sdk_types.FlyteSdkType):
            return t
        else:
            raise _user_exceptions.FlyteTypeException(
                type(t),
                _base_sdk_types.FlyteSdkType,
                additional_msg="Should be of form similar to: Types.Integer, [Types.Integer], {Types.String: "
                "Types.Integer}",
                received_value=t,
            )

    def get_sdk_type_from_literal_type(self, literal_type):
        """
        :param flytekit.models.types.LiteralType literal_type:
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        if literal_type.collection_type is not None:
            return _container_types.List(_helpers.get_sdk_type_from_literal_type(literal_type.collection_type))
        elif literal_type.map_value_type is not None:
            raise NotImplementedError("TODO: Implement map")
        elif literal_type.schema is not None:
            return _schema.schema_instantiator_from_proto(literal_type.schema)
        elif literal_type.blob is not None:
            return self._get_blob_impl_from_type(literal_type.blob)
        elif literal_type.simple is not None:
            if (
                literal_type.simple == _literal_type_models.SimpleType.BINARY
                and _proto.Protobuf.PB_FIELD_KEY in literal_type.metadata
            ):
                return _proto_sdk_type_from_tag(literal_type.metadata[_proto.Protobuf.PB_FIELD_KEY])
            if (
                literal_type.simple == _literal_type_models.SimpleType.STRUCT
                and literal_type.metadata
                and _proto.Protobuf.PB_FIELD_KEY in literal_type.metadata
            ):
                return _generic_proto_sdk_type_from_tag(literal_type.metadata[_proto.Protobuf.PB_FIELD_KEY])
            sdk_type = self._SIMPLE_TYPE_LOOKUP_TABLE.get(literal_type.simple)
            if sdk_type is None:
                raise NotImplementedError(
                    "We haven't implemented this type yet:  Simple type={}".format(literal_type.simple)
                )
            return sdk_type
        else:
            raise _system_exceptions.FlyteSystemAssertion(
                "An unrecognized literal type was received: {}".format(literal_type)
            )

    def infer_sdk_type_from_literal(self, literal):  # noqa
        """
        :param flytekit.models.literals.Literal literal:
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        if literal.collection is not None:
            if len(literal.collection.literals) > 0:
                sdk_type = _container_types.List(_helpers.infer_sdk_type_from_literal(literal.collection.literals[0]))
            else:
                sdk_type = _container_types.List(_base_sdk_types.Void)
        elif literal.map is not None:
            raise NotImplementedError("TODO: Implement map")
        elif literal.scalar.blob is not None:
            sdk_type = self._get_blob_impl_from_type(literal.scalar.blob.metadata.type)
        elif literal.scalar.none_type is not None:
            sdk_type = _base_sdk_types.Void
        elif literal.scalar.schema is not None:
            sdk_type = _schema.schema_instantiator_from_proto(literal.scalar.schema.type)
        elif literal.scalar.error is not None:
            raise NotImplementedError("TODO: Implement error from literal map")
        elif literal.scalar.generic is not None:
            sdk_type = _primitive_types.Generic
        elif literal.scalar.binary is not None:
            if literal.scalar.binary.tag.startswith(_proto.Protobuf.TAG_PREFIX):
                sdk_type = _proto_sdk_type_from_tag(literal.scalar.binary.tag[len(_proto.Protobuf.TAG_PREFIX) :])
            else:
                raise NotImplementedError("TODO: Binary is only supported for protobuf types currently")
        elif literal.scalar.primitive.boolean is not None:
            sdk_type = _primitive_types.Boolean
        elif literal.scalar.primitive.datetime is not None:
            sdk_type = _primitive_types.Datetime
        elif literal.scalar.primitive.duration is not None:
            sdk_type = _primitive_types.Timedelta
        elif literal.scalar.primitive.float_value is not None:
            sdk_type = _primitive_types.Float
        elif literal.scalar.primitive.integer is not None:
            sdk_type = _primitive_types.Integer
        elif literal.scalar.primitive.string_value is not None:
            sdk_type = _primitive_types.String
        else:
            raise _system_exceptions.FlyteSystemAssertion("Received unknown literal: {}".format(literal))
        return sdk_type

    def _get_blob_impl_from_type(self, blob_type):
        """
        :param flytekit.models.core.types.BlobType blob_type:
        :rtype: flytekit.common.types.base_sdk_types.FlyteSdkType
        """
        if blob_type.dimensionality == _core_types.BlobType.BlobDimensionality.SINGLE:
            if blob_type.format == "csv":
                return _blobs.CSV
            else:
                return _blobs.Blob
        elif blob_type.dimensionality == _core_types.BlobType.BlobDimensionality.MULTIPART:
            if blob_type.format == "csv":
                return _blobs.MultiPartCSV
            else:
                return _blobs.MultiPartBlob
        raise _system_exceptions.FlyteSystemAssertion(
            "Flyte's base type engine does not support this type of blob. Value: {}".format(blob_type)
        )
