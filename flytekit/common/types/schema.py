from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.common.types.impl import schema as _schema_impl
from flytekit.models import literals as _literals
from flytekit.models import types as _idl_types


class SchemaInstantiator(_base_sdk_types.InstantiableType):
    def create_at_known_location(cls, location):
        """
        :param Text location:
        :rtype: flytekit.common.types.impl.schema.Schema
        """
        return _schema_impl.Schema.create_at_known_location(location, mode="wb", schema_type=cls.schema_type)

    def fetch(cls, remote_path, local_path=None):
        """
        :param Text remote_path:
        :param Text local_path: [Optional] If specified, the Schema is copied to this location.  If specified,
            this location is NOT managed and the schema will not be cleaned up upon exit.
        :rtype: flytekit.common.types.impl.schema.Schema
        """
        return _schema_impl.Schema.fetch(remote_path, mode="rb", local_path=local_path, schema_type=cls.schema_type)

    def create(cls):
        """
        :rtype: flytekit.common.types.impl.schema.Schema
        """
        return _schema_impl.Schema.create_at_any_location(mode="wb", schema_type=cls.schema_type)

    def create_from_hive_query(
        cls, select_query, stage_query=None, schema_to_table_name_map=None, known_location=None,
    ):
        """
        Returns a query that can be submitted to Hive and produce the desired output.  It also returns a properly-typed
        schema object.

        :param Text select_query: Query for selecting data from Hive
        :param Text stage_query: Query for building temporary tables on Hive.
            Runs before the select query. Temporary tables are supported but CTEs are not supported.
        :param Dict[Text, Text] schema_to_table_name_map: A map of column names in the schema to the column names
            returned from the select query
        :param Text known_location: create the schema object at a known s3 location.
        :rtype: flytekit.common.types.impl.schema.Schema, Text
        """
        return _schema_impl.Schema.create_from_hive_query(
            select_query=select_query,
            stage_query=stage_query,
            schema_to_table_name_map=schema_to_table_name_map,
            known_location=known_location,
            schema_type=cls.schema_type,
        )

    def __call__(cls, *args, **kwargs):
        """
        TODO: Is there a better way to deal with this?

        :rtype: flytekit.common.types.impl.schema.Schema
        """
        if not args and not kwargs:
            return _schema_impl.Schema.create_at_any_location(mode="wb", schema_type=cls.schema_type)
        else:
            return super(SchemaInstantiator, cls).__call__(*args, **kwargs)

    @property
    def schema_type(cls):
        """
        :rtype: _schema_impl.SchemaType
        """
        return cls._schema_type

    @property
    def columns(cls):
        """
        :rtype: dict[Text, flytekit.common.types.base_sdk_types.FlyteSdkType]
        """
        return cls.schema_type.sdk_columns


class Schema(_base_sdk_types.FlyteSdkValue, metaclass=SchemaInstantiator):
    @classmethod
    def from_string(cls, string_value):
        """
        :param Text string_value:
        :rtype: Schema
        """
        if not string_value:
            _user_exceptions.FlyteValueException(string_value, "Cannot create a Schema from an empty path")
        return cls(_schema_impl.Schema.from_string(string_value, schema_type=cls.schema_type))

    @classmethod
    def is_castable_from(cls, other):
        """
        :param flytekit.common.types.base_literal_types.FlyteSdkType other:
        :rtype: bool
        """
        return cls == other

    @classmethod
    def from_python_std(cls, t_value):
        """
        :param T t_value: It is up to each individual object as to whether or not this value can be cast.
        :rtype: FlyteSdkValue
        :raises: flytekit.common.exceptions.user.FlyteTypeException
        """
        if t_value is None:
            return _base_sdk_types.Void()
        elif isinstance(t_value, _schema_impl.Schema):
            schema = t_value.cast_to(cls.schema_type)
        else:
            schema = _schema_impl.Schema.from_python_std(t_value, schema_type=cls.schema_type)
        return cls(schema)

    @classmethod
    def to_flyte_literal_type(cls):
        """
        :rtype: flytekit.models.types.LiteralType
        """
        return _idl_types.LiteralType(schema=cls.schema_type)

    @classmethod
    def promote_from_model(cls, literal_model):
        """
        Creates an object of this type from the model primitive defining it.
        :param flytekit.models.literals.Literal literal_model:
        :rtype: Schema
        """
        return cls(_schema_impl.Schema.promote_from_model(literal_model.scalar.schema))

    @classmethod
    def short_class_string(cls):
        """
        :rtype: Text
        """
        return repr(cls.schema_type)

    def __init__(self, value):
        """
        :param flytekit.common.types.impl.schema.Schema value: Schema value to wrap
        """
        super(Schema, self).__init__(scalar=_literals.Scalar(schema=value))

    def to_python_std(self):
        """
        :rtype: flytekit.common.types.impl.schema.Schema
        """
        return self.scalar.schema

    def short_string(self):
        """
        :rtype: Text
        """
        return "{}".format(self.scalar.schema,)


def schema_instantiator(columns=None):
    """
    :param list[(Text, flytekit.common.types.base_sdk_types.FlyteSdkType)] columns: [Optional] Description of the
        columns in the underlying schema.  Should be tuples with the first element being the name.
    :rtype: SchemaInstantiator
    """
    if columns is not None and len(columns) == 0:
        raise _user_exceptions.FlyteValueException(
            columns,
            "When specifying a Schema type with a known set of columns, a non-empty list must be provided as " "inputs",
        )

    class _Schema(Schema, metaclass=SchemaInstantiator):
        _schema_type = _schema_impl.SchemaType(columns=columns)

    return _Schema


def schema_instantiator_from_proto(schema_type):
    """
    :param flytekit.models.types.SchemaType schema_type:
    :rtype: SchemaInstantiator
    """

    class _Schema(Schema, metaclass=SchemaInstantiator):
        _schema_type = _schema_impl.SchemaType.promote_from_model(schema_type)

    return _Schema
