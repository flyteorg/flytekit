import collections as _collections
import os as _os
import uuid as _uuid

import six as _six

from flytekit.common import sdk_bases as _sdk_bases
from flytekit.common import utils as _utils
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.exceptions import user as _user_exceptions
from flytekit.common.types import base_sdk_types as _base_sdk_types
from flytekit.common.types import helpers as _helpers
from flytekit.common.types import primitives as _primitives
from flytekit.common.types.impl import blobs as _blob_impl
from flytekit.configuration import sdk as _sdk_config
from flytekit.interfaces.data import data_proxy as _data_proxy
from flytekit.models import literals as _literal_models
from flytekit.models import types as _type_models
from flytekit.plugins import numpy as _np
from flytekit.plugins import pandas as _pd

# Note: For now, this is only for basic type-checking.  We need not differentiate between TINYINT, BIGINT,
# and INT or DOUBLE and FLOAT, VARCHAR and STRING, etc. as we will unpack into appropriate Python
# objects anyway.  If we work on managed tables, these more specific type specifications might become necessary.
_SUPPORTED_LITERAL_TYPE_TO_PANDAS_TYPES = None


def get_supported_literal_types_to_pandas_types():
    global _SUPPORTED_LITERAL_TYPE_TO_PANDAS_TYPES
    if _SUPPORTED_LITERAL_TYPE_TO_PANDAS_TYPES is None:
        _SUPPORTED_LITERAL_TYPE_TO_PANDAS_TYPES = {
            _primitives.Integer.to_flyte_literal_type(): {_np.int32, _np.int64, _np.uint32, _np.uint64},
            _primitives.Float.to_flyte_literal_type(): {_np.float32, _np.float64},
            _primitives.Boolean.to_flyte_literal_type(): {_np.bool},
            _primitives.Datetime.to_flyte_literal_type(): {_np.datetime64},
            _primitives.Timedelta.to_flyte_literal_type(): {_np.timedelta64},
            _primitives.String.to_flyte_literal_type(): {_np.object_, _np.str_, _np.string_},
        }
    return _SUPPORTED_LITERAL_TYPE_TO_PANDAS_TYPES


_ALLOWED_PARTITION_TYPES = {str, int}

# Hive currently has limitations where column headers are not stored when writing to an overwrite directory.  There is
# an open proposal (https://issues.apache.org/jira/browse/HIVE-12860) to improve this.  Until then, we have this
# work-around where we create an external table with the appropriate schema and write the data to our desired
# location.  The issue here is that the table information in the meta-store might not get cleaned up during a partial
# failure.
_HIVE_QUERY_FORMATTER = """
    {stage_query_str}

    CREATE TEMPORARY TABLE {table}_tmp AS {query_str};
    CREATE EXTERNAL TABLE {table} LIKE {table}_tmp STORED AS PARQUET;
    ALTER TABLE {table} SET LOCATION '{url}';

    INSERT OVERWRITE TABLE {table}
        SELECT
            {columnar_query}
        FROM {table}_tmp;
    DROP TABLE {table};
    """

# Once https://issues.apache.org/jira/browse/HIVE-12860 is resolved.  We will prefer the following syntax because it
# guarantees cleanup on partial failures.
_HIVE_QUERY_FORMATTER_V2 = """
    CREATE TEMPORARY TABLE {table} AS {query_str};

    INSERT OVERWRITE DIRECTORY '{url}' STORED AS PARQUET
        SELECT {columnar_query}
        FROM {table};
    """

# Set location in both parts of this query so in case of a partial failure, we will always have some data backing a
# partition.
_WRITE_HIVE_PARTITION_QUERY_FORMATTER = """
    ALTER TABLE {write_table} ADD IF NOT EXISTS {partition_string} LOCATION '{url}';

    ALTER TABLE {write_table} {partition_string} SET LOCATION '{url}';
    """


def _format_insert_partition_query(table_name, partition_string, remote_location):
    table_pieces = table_name.split(".")
    if len(table_pieces) > 1:
        # Hive shell commands don't allow us to alter tables and select databases in the table specification.  So
        # we split the table name and use the 'use' command to choose the correct database.
        prefix = "use {};\n".format(table_pieces[0])
        table_name = ".".join(table_pieces[1:])
    else:
        prefix = ""

    return prefix + _WRITE_HIVE_PARTITION_QUERY_FORMATTER.format(
        write_table=table_name, partition_string=partition_string, url=remote_location
    )


class _SchemaIO(object):
    def __init__(self, schema_instance, local_dir, mode):
        """
        :param Schema schema_instance:
        :param flytekit.common.utils.Directory local_dir:
        :param Text mode:
        """
        self._schema = schema_instance
        self._local_dir = local_dir
        self._chunks = []
        self._index = 0
        self._mode = mode

    def _access_guard(self):
        if not self._schema:
            raise _user_exceptions.FlyteAssertion(
                "Schema IO object has already been closed.  Cannot access chunk_count property."
            )

    @_exception_scopes.system_entry_point
    def iter_chunks(self, *args, **kwargs):
        raise _user_exceptions.FlyteAssertion("{} is write only.".format(self._schema))

    @_exception_scopes.system_entry_point
    def read(self, *args, **kwargs):
        raise _user_exceptions.FlyteAssertion("{} is write only.".format(self._schema))

    @_exception_scopes.system_entry_point
    def write(self, *args, **kwargs):
        raise _user_exceptions.FlyteAssertion("{} is read only.".format(self._schema))

    @_exception_scopes.system_entry_point
    def close(self):
        self._schema = None
        self._local_dir = None
        self._chunks = None
        self._index = 0

    @property
    @_exception_scopes.system_entry_point
    def chunk_count(self):
        self._access_guard()
        return len(self._chunks)

    @_exception_scopes.system_entry_point
    def seek(self, index):
        self._access_guard()
        if index < 0 or index > self.chunk_count:
            raise _user_exceptions.FlyteValueException(
                index,
                "Attempting to seek to a chunk that is out of range. Allowed range is [0, {}]".format(self.chunk_count),
            )
        self._index = index

    @_exception_scopes.system_entry_point
    def tell(self):
        return self._index

    def __repr__(self):
        return "{mode} IO Object for {type} @ {location}".format(
            type=self._schema.type, location=self._schema.remote_prefix, mode=self._mode
        )


class _SchemaReader(_SchemaIO):
    def __init__(self, schema_instance, local_dir):
        """
        :param Schema schema_instance:
        :param flytekit.common.utils.Directory local_dir:
        """
        super(_SchemaReader, self).__init__(schema_instance, local_dir, "Read-Only")
        self.reset_chunks()

    @_exception_scopes.system_entry_point
    def reset_chunks(self):
        self._chunks = sorted(self._local_dir.list_dir())

    @_exception_scopes.system_entry_point
    def iter_chunks(self, columns=None, **kwargs):
        self._access_guard()
        while self._index < len(self._chunks):
            chunk = self.read(columns=columns, concat=False, **kwargs)
            if chunk is not None:
                yield chunk

    @staticmethod
    def _read_parquet_with_type_promotion_override(chunk, columns, parquet_engine):
        """
        This wrapper function of pd.read_parquet() is a hack intended to fix the type promotion problem
        when using fastparquet as the underlying parquet engine.

        When using fastparquet, boolean columns containing None values will be promoted to float16 columns.
        This becomes problematic when users want to write the dataframe back into parquet
        file because float16 (halffloat) is not a supported type in parquet spec. In this function, we detect
        such columns and do override the type promotion.
        """
        df = None

        if parquet_engine == "fastparquet":
            import fastparquet.thrift_structures as _ts
            from fastparquet import ParquetFile as _ParquetFile

            # https://github.com/dask/fastparquet/issues/414#issuecomment-478983811
            df = _pd.read_parquet(chunk, columns=columns, engine=parquet_engine, index=False)
            df_column_types = df.dtypes
            pf = _ParquetFile(chunk)
            schema_column_dtypes = {l.name: l.type for l in list(pf.schema.schema_elements)}

            for idx in df_column_types[df_column_types == "float16"].index.tolist():
                # A hacky way to get the string representations of the column types of a parquet schema
                # Reference:
                # https://github.com/dask/fastparquet/blob/f4ecc67f50e7bf98b2d0099c9589c615ea4b06aa/fastparquet/schema.py
                if _ts.parquet_thrift.Type._VALUES_TO_NAMES[schema_column_dtypes[idx]] == "BOOLEAN":
                    df[idx] = df[idx].astype("object")
                    df[idx].replace({0: False, 1: True, _pd.np.nan: None}, inplace=True)

        else:
            df = _pd.read_parquet(chunk, columns=columns, engine=parquet_engine)

        return df

    @_exception_scopes.system_entry_point
    def read(self, columns=None, concat=False, truncate_extra_columns=True, **kwargs):
        """
        When this function is called, one chunk will be read and received as a Pandas data frame.  Once all chunks
        have been read, this function will return None.

        :param list[Text] columns: A list of columns to read.  They must be a subset of the columns
            defined for the Schema object.  If specified, truncate_extra_columns must be True.
        :param bool concat:  If true, the entire object will be returned in one large data frame.
        :param bool truncate_extra_columns: If true, only columns from the underlying parquet file will be read if
            they are specified as columns in the schema object (except for empty schemas which will read all columns
            regardless). If false, if there are additional columns in the underlying parquet file, they will also be
            read.
        :rtype: pandas.DataFrame
        """
        if columns is not None and truncate_extra_columns is False:
            raise _user_exceptions.FlyteAssertion(
                "When reading a schema object, it is not possible to both specify a set of columns to read and "
                "additionally not truncate_extra_columns.  Either columns must not be specified or "
                "truncate_extra_columns must be set to True (or not specified)."
            )

        self._access_guard()

        parquet_engine = _sdk_config.PARQUET_ENGINE.get()
        if parquet_engine not in {"fastparquet", "pyarrow"}:
            raise _user_exceptions.FlyteAssertion(
                "environment variable parquet_engine must be one of 'pyarrow', 'fastparquet', or be unset"
            )

        df_out = None
        if not columns:
            columns = list(self._schema.type.sdk_columns.keys())

        if len(columns) == 0 or truncate_extra_columns is False:
            columns = None

        if concat:
            frames = [
                # A hacky hack
                # TODO: follow up the issue opened in the fastparquet repo for a more general fix
                # issue URL:
                _SchemaReader._read_parquet_with_type_promotion_override(
                    chunk=chunk, columns=columns, parquet_engine=parquet_engine
                )
                # _pd.read_parquet(chunk, columns=columns, engine=parquet_engine)
                for chunk in self._chunks[self._index :]
                if _os.path.getsize(chunk) > 0
            ]
            if len(frames) == 1:
                df_out = frames[0]
            elif len(frames) > 1:
                df_out = _pd.concat(frames, copy=True)
            self._index = len(self._chunks)
        else:
            while self._index < len(self._chunks) and df_out is None:
                # Skip empty chunks so the user appears to have a continuous stream of data.
                if _os.path.getsize(self._chunks[self._index]) > 0:
                    df_out = _SchemaReader._read_parquet_with_type_promotion_override(
                        chunk=self._chunks[self._index], columns=columns, parquet_engine=parquet_engine, **kwargs
                    )
                self._index += 1

        if df_out is not None:
            self._schema.compare_dataframe_to_schema(df_out, read=True, column_subset=columns)

            # Make sure the columns are renamed to exactly what the user specifies.  This prevents unexpected
            # unicode v. string mismatches.  Also, if a schema is mapped with strict_names=False, the input might
            # have totally different names.
            user_columns = columns or _six.iterkeys(self._schema.type.sdk_columns)
            # User-specified columns may or may not be unicode
            # Since, in python 2, dictionary does a transparent translation between unicode and str for the key,
            # (https://stackoverflow.com/a/24532329)
            # we use this characteristic to create a trivial lookup dictionary, to make sure we can use either
            # unicode or str to lookup, but get back whatever type the user actually used
            user_column_dict = {c: c for c in user_columns}
            if len(self._schema.type.columns) > 0:
                # Avoid using pandas.DataFrame.rename() as this function incurs significant memory overhead
                df_out.columns = [
                    user_column_dict[col] if col in user_columns else col for col in df_out.columns.values
                ]
        return df_out


class _SchemaWriter(_SchemaIO):
    def __init__(self, schema_instance, local_dir):
        """
        :param Schema schema_instance:
        :param flytekit.common.utils.Directory local_dir:
        :param Text mode:
        """
        super(_SchemaWriter, self).__init__(schema_instance, local_dir, "Write-Only")

    @_exception_scopes.system_entry_point
    def close(self):
        """
        Closes the writing IO context and uploads data to s3.
        """
        try:
            # TODO: Introduce system logging
            # logging.info("Copying recursively {} -> {}".format(self._local_dir.name, self._schema.remote_prefix))
            _data_proxy.Data.put_data(self._local_dir.name, self._schema.remote_prefix, is_multipart=True)
        finally:
            super(_SchemaWriter, self).close()

    @_exception_scopes.system_entry_point
    def write(self, data_frame, coerce_timestamps="us", allow_truncated_timestamps=False):
        """
        Writes data frame as a chunk to the local directory owned by the Schema object.  Will later be uploaded to s3.

        :param pandas.DataFrame data_frame: data frame to write as parquet
        :param Text coerce_timestamps: format to store timestamp in parquet. 'us', 'ms', 's' are allowed values.
            Note: if your timestamps will lose data due to the coercion, your write will fail!  Nanoseconds are
            problematic in the Parquet format and will not work. See allow_truncated_timestamps.
        :param bool allow_truncated_timestamps: default False. Allow truncation when coercing timestamps to a coarser
            resolution.
        """
        self._access_guard()
        if not isinstance(data_frame, _pd.DataFrame):
            raise _user_exceptions.FlyteTypeException(
                expected_type=_pd.DataFrame,
                received_type=type(data_frame),
                received_value=data_frame,
                additional_msg="Only pandas DataFrame objects can be written to a Schema object",
            )

        self._schema.compare_dataframe_to_schema(data_frame)
        all_columns = list(data_frame.columns.values)

        # Convert all columns to unicode as pyarrow's parquet reader can not handle mixed strings and unicode.
        # Since columns from Hive are returned as unicode, if a user wants to add a column to a dataframe returned from
        # Hive, then output the new data, the user would have to provide a unicode column name which is unnatural.
        unicode_columns = [_six.text_type(col) for col in all_columns]
        data_frame.columns = unicode_columns
        try:
            filename = self._local_dir.get_named_tempfile(_os.path.join(str(self._index).zfill(6)))
            data_frame.to_parquet(
                filename, coerce_timestamps=coerce_timestamps, allow_truncated_timestamps=allow_truncated_timestamps,
            )
            if self._index == len(self._chunks):
                self._chunks.append(filename)
            self._index += 1
        finally:
            # Return to old names to prevent odd behavior with user.
            data_frame.columns = unicode_columns


class _SchemaBackingMpBlob(_blob_impl.MultiPartBlob):
    @property
    def directory(self):
        """
        :rtype: flytekit.common.utils.Directory
        """
        return self._directory

    def __enter__(self):
        if not self.local_path:
            if _data_proxy.LocalWorkingDirectoryContext.get() is None:
                raise _user_exceptions.FlyteAssertion(
                    "No temporary file system is present.  Either call this method from within the "
                    "context of a task or surround with a 'with LocalTestFileSystem():' block.  Or "
                    "specify a path when calling this function."
                )
            self._directory = _utils.AutoDeletingTempDir(
                _uuid.uuid4().hex, tmp_dir=_data_proxy.LocalWorkingDirectoryContext.get().name,
            )
            self._is_managed = True
            self._directory.__enter__()

            if "r" in self.mode:
                _data_proxy.Data.get_data(self.remote_location, self.local_path, is_multipart=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if "w" in self.mode:
            _data_proxy.Data.put_data(self.local_path, self.remote_location, is_multipart=True)
        return super(_SchemaBackingMpBlob, self).__exit__(exc_type, exc_val, exc_tb)


class SchemaType(_type_models.SchemaType, metaclass=_sdk_bases.ExtendedSdkType):
    _LITERAL_TYPE_TO_PROTO_ENUM = {
        _primitives.Integer.to_flyte_literal_type(): _type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER,
        _primitives.Float.to_flyte_literal_type(): _type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT,
        _primitives.Boolean.to_flyte_literal_type(): _type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN,
        _primitives.Datetime.to_flyte_literal_type(): _type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME,
        _primitives.Timedelta.to_flyte_literal_type(): _type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION,
        _primitives.String.to_flyte_literal_type(): _type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING,
    }

    def __init__(self, columns=None):
        super(SchemaType, self).__init__(None)
        self._set_columns(columns or [])

    @property
    def sdk_columns(self):
        """
        This is an ordered dictionary so iterating over it will be in the  order columns were specified in the
            constructor.
        :rtype: dict[Text, flytekit.common.types.base_sdk_types.FlyteSdkType]
        """
        return self._sdk_columns

    @property
    def columns(self):
        """
        :rtype: list[flytekit.models.types.SchemaType.SchemaColumn]
        """
        return [
            _type_models.SchemaType.SchemaColumn(n, type(self)._LITERAL_TYPE_TO_PROTO_ENUM[v.to_flyte_literal_type()])
            for n, v in _six.iteritems(self.sdk_columns)
        ]

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.types.SchemaType model:
        :rtype: SchemaType
        """
        _PROTO_ENUM_TO_SDK_TYPE = {
            _type_models.SchemaType.SchemaColumn.SchemaColumnType.INTEGER: _helpers.get_sdk_type_from_literal_type(
                _primitives.Integer.to_flyte_literal_type()
            ),
            _type_models.SchemaType.SchemaColumn.SchemaColumnType.FLOAT: _helpers.get_sdk_type_from_literal_type(
                _primitives.Float.to_flyte_literal_type()
            ),
            _type_models.SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN: _helpers.get_sdk_type_from_literal_type(
                _primitives.Boolean.to_flyte_literal_type()
            ),
            _type_models.SchemaType.SchemaColumn.SchemaColumnType.DATETIME: _helpers.get_sdk_type_from_literal_type(
                _primitives.Datetime.to_flyte_literal_type()
            ),
            _type_models.SchemaType.SchemaColumn.SchemaColumnType.DURATION: _helpers.get_sdk_type_from_literal_type(
                _primitives.Timedelta.to_flyte_literal_type()
            ),
            _type_models.SchemaType.SchemaColumn.SchemaColumnType.STRING: _helpers.get_sdk_type_from_literal_type(
                _primitives.String.to_flyte_literal_type()
            ),
        }
        return cls([(c.name, _PROTO_ENUM_TO_SDK_TYPE[c.type]) for c in model.columns])

    def _set_columns(self, columns):
        names_seen = set()
        for column in columns:
            if not isinstance(column, tuple):
                raise _user_exceptions.FlyteValueException(
                    column,
                    "When specifying a Schema type with a known set of columns.  Each column must be "
                    "specified as a tuple in the form ('name', type).",
                )
            if len(column) != 2:
                raise _user_exceptions.FlyteValueException(
                    column,
                    "When specifying a Schema type with a known set of columns.  Each column must be "
                    "specified as a tuple in the form ('name', type).",
                )
            name, sdk_type = column
            sdk_type = _helpers.python_std_to_sdk_type(sdk_type)

            if not isinstance(name, (str, _six.text_type)):
                additional_msg = (
                    "When specifying a Schema type with a known set of columns, the first element in"
                    " each tuple must be text."
                )
                raise _user_exceptions.FlyteTypeException(
                    received_type=type(name),
                    received_value=name,
                    expected_type={str, _six.text_type},
                    additional_msg=additional_msg,
                )

            if (
                not isinstance(sdk_type, _base_sdk_types.FlyteSdkType)
                or sdk_type.to_flyte_literal_type() not in get_supported_literal_types_to_pandas_types()
            ):
                additional_msg = (
                    "When specifying a Schema type with a known set of columns, the second element of "
                    "each tuple must be a supported type.  Failed for column: {name}".format(name=name)
                )
                raise _user_exceptions.FlyteTypeException(
                    expected_type=list(get_supported_literal_types_to_pandas_types().keys()),
                    received_type=sdk_type,
                    additional_msg=additional_msg,
                )

            if name in names_seen:
                raise ValueError(
                    "The column name {name} was specified multiple times when instantiating the "
                    "Schema.".format(name=name)
                )
            names_seen.add(name)

        self._sdk_columns = _collections.OrderedDict(columns)


class Schema(_literal_models.Schema, metaclass=_sdk_bases.ExtendedSdkType):
    def __init__(self, remote_path, mode="rb", schema_type=None):
        """
        :param Text remote_path:
        :param Text mode:
        :param SchemaType schema_type: [Optional] If specified, the schema will be forced to conform to this type.  If
            not specified, the schema will be considered generic.
        """
        self._mp_blob = _SchemaBackingMpBlob(remote_path, mode=mode)
        super(Schema, self).__init__(self._mp_blob.uri, schema_type or SchemaType())
        self._io_object = None

    @classmethod
    def promote_from_model(cls, model):
        """
        :param flytekit.models.literals.Schema model:
        :rtype: Schema
        """
        return cls(model.uri, schema_type=SchemaType.promote_from_model(model.type))

    @classmethod
    @_exception_scopes.system_entry_point
    def create_at_known_location(cls, known_remote_location, mode="wb", schema_type=None):
        """
        :param Text known_remote_location: The location to which to write the object.  Usually an s3 path.
        :param Text mode:
        :param SchemaType schema_type: [Optional] If specified, the schema will be forced to conform to this type.  If
            not specified, the schema will be considered generic.
        :rtype: Schema
        """
        return cls(known_remote_location, mode=mode, schema_type=schema_type)

    @classmethod
    @_exception_scopes.system_entry_point
    def create_at_any_location(cls, mode="wb", schema_type=None):
        """
        :param Text mode:
        :param SchemaType schema_type: [Optional] If specified, the schema will be forced to conform to this type.  If
            not specified, the schema will be considered generic.
        :rtype: Schema
        """
        return cls.create_at_known_location(_data_proxy.Data.get_remote_path(), mode=mode, schema_type=schema_type)

    @classmethod
    @_exception_scopes.system_entry_point
    def fetch(cls, remote_path, local_path=None, overwrite=False, mode="rb", schema_type=None):
        """
        :param Text remote_path: The location from which to fetch the object. Usually an s3 path.
        :param Text local_path: [Optional] A local path to which to download the object. If specified, the object
            will not be managed and might not be cleaned up by the system upon exiting the context.
        :param bool overwrite: If True, objects will be overwritten at the provided local_path in order to fetch this
            object.  Default is False.
        :param Text mode: Read or write mode of the object.
        :param SchemaType schema_type: [Optional] If specified, the schema will be forced to conform to this type.  If
            not specified, the schema will be considered generic.
        :rtype: Schema
        """
        schema = cls(remote_path, mode=mode, schema_type=schema_type)
        schema.download(local_path=local_path, overwrite=overwrite)
        return schema

    @classmethod
    @_exception_scopes.system_entry_point
    def from_python_std(cls, t_value, schema_type=None):
        """
        :param T t_value:
        :param SchemaType schema_type: [Optional] If specified, we will ensure
        :rtype: Schema
        """
        if isinstance(t_value, (str, _six.text_type)):
            if _os.path.isdir(t_value):
                schema = cls.create_at_any_location(schema_type=schema_type)
                schema.multipart_blob._directory = _utils.Directory(t_value)
                schema.upload()
            else:
                schema = cls.create_at_known_location(t_value, schema_type=schema_type)
            return schema
        elif isinstance(t_value, cls):
            return t_value
        elif isinstance(t_value, _pd.DataFrame):
            # Accepts a pandas dataframe and converts to a Schema object
            o = cls.create_at_any_location(schema_type=schema_type)
            with o as w:
                w.write(t_value)
            return o
        elif isinstance(t_value, list):
            # Accepts a list of pandas dataframe and converts to a Schema object
            o = cls.create_at_any_location(schema_type=schema_type)
            with o as w:
                for x in t_value:
                    if isinstance(x, _pd.DataFrame):
                        w.write(x)
                    else:
                        raise _user_exceptions.FlyteTypeException(
                            type(t_value),
                            {str, _six.text_type, Schema},
                            received_value=x,
                            additional_msg="A Schema object can only be create from a pandas DataFrame or a list of pandas DataFrame.",
                        )
            return o
        else:
            raise _user_exceptions.FlyteTypeException(
                type(t_value),
                {str, _six.text_type, Schema},
                received_value=t_value,
                additional_msg="Unable to create Schema from user-provided value.",
            )

    @classmethod
    def from_string(cls, string_value, schema_type=None):
        """
        :param Text string_value:
        :param SchemaType schema_type:
        :rtype: Schema
        """
        if not string_value:
            _user_exceptions.FlyteValueException(string_value, "Cannot create a Schema from an empty path")
        return cls.create_at_known_location(string_value, schema_type=schema_type)

    @classmethod
    @_exception_scopes.system_entry_point
    def create_from_hive_query(
        cls, select_query, stage_query=None, schema_to_table_name_map=None, schema_type=None, known_location=None,
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
        :param SchemaType schema_type: [Optional] If specified, the schema will be forced to conform to this type.  If
            not specified, the schema will be considered generic.
        :return: Schema, Text
        """
        schema_object = cls(
            known_location or _data_proxy.Data.get_remote_directory(), mode="wb", schema_type=schema_type,
        )

        if len(schema_object.type.sdk_columns) > 0:
            identity_dict = {n: n for n in _six.iterkeys(schema_object.type.sdk_columns)}
            identity_dict.update(schema_to_table_name_map or {})
            schema_to_table_name_map = identity_dict

            columnar_clauses = []
            for name, sdk_type in _six.iteritems(schema_object.type.sdk_columns):
                if sdk_type == _primitives.Float:
                    columnar_clauses.append(
                        "CAST({table_column_name} as double) {schema_name}".format(
                            table_column_name=schema_to_table_name_map[name], schema_name=name,
                        )
                    )
                else:
                    columnar_clauses.append(
                        "{table_column_name} as {schema_name}".format(
                            table_column_name=schema_to_table_name_map[name], schema_name=name,
                        )
                    )
            columnar_query = ",\n\t\t".join(columnar_clauses)
        else:
            columnar_query = "*"

        stage_query_str = _six.text_type(stage_query or "")
        # the stage query should always end with a semicolon
        stage_query_str = stage_query_str if stage_query_str.endswith(";") else (stage_query_str + ";")
        query = _HIVE_QUERY_FORMATTER.format(
            url=schema_object.remote_location,
            stage_query_str=stage_query_str,
            query_str=select_query.strip().strip(";"),
            columnar_query=columnar_query,
            table=_uuid.uuid4().hex,
        )
        return schema_object, query

    @property
    def local_path(self):
        """
        Local filesystem path where the file was downloaded
        :rtype: Text
        """
        return self._mp_blob.local_path

    @property
    def remote_location(self):
        """
        Path to where this MultiPartBlob will be synced.  This is needed for reverse compatibility.
        :rtype: Text
        """
        return self.uri

    @property
    def remote_prefix(self):
        """
        Path to where this MultiPartBlob will be synced.  This is needed for reverse compatibility.
        :rtype: Text
        """
        return self.uri

    @property
    def uri(self):
        """
        Path to where this MultiPartBlob will be synced.
        :rtype: Text
        """
        return self.multipart_blob.uri

    @property
    def mode(self):
        """
        The mode string the MultiPartBlob is associated with.
        :rtype: Text
        """
        return self._mp_blob.mode

    @property
    def type(self):
        """
        The schema type definition associated with this object.
        :rtype: SchemaType
        """
        return self._type

    @property
    def multipart_blob(self):
        """
        :rtype: flytekit.common.types.impl.blobs.MultiPartBlob
        """
        return self._mp_blob

    @_exception_scopes.system_entry_point
    def __enter__(self):
        """
        :rtype: _SchemaIO
        """
        if self._io_object is not None:
            raise _user_exceptions.FlyteAssertion(
                "The context of a schema can only be entered once at a time.  Make sure the previous "
                "'with' block has been exited."
            )

        self._mp_blob.__enter__()
        if "r" in self.mode:
            self._io_object = _SchemaReader(self, self.multipart_blob.directory)
        else:
            self._io_object = _SchemaWriter(self, self.multipart_blob.directory)
        return self._io_object

    @_exception_scopes.system_entry_point
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._io_object = None
        return self._mp_blob.__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self):
        return "Schema({columns}) @ {location} ({mode})".format(
            columns=self.type.columns,
            location=self.remote_prefix,
            mode="read-only" if "r" in self.mode else "write-only",
        )

    @_exception_scopes.system_entry_point
    def download(self, local_path=None, overwrite=False):
        """
        :param Text local_path: [Optional] A local path to which to download the object. If specified, the object
            will not be managed and might not be cleaned up by the system upon exiting the context.
        :param bool overwrite: If True, objects will be overwritten at the provided local_path in order to fetch this
            object.  Default is False.
        :rtype: Schema
        """
        self.multipart_blob.download(local_path=local_path, overwrite=overwrite)

    @_exception_scopes.system_entry_point
    def get_write_partition_to_hive_table_query(
        self,
        table_name,
        partitions=None,
        schema_to_table_name_map=None,
        partitions_in_table=False,
        append_to_partition=False,
    ):
        """
        Returns a Hive query string that will update the metatable to point to the data as the new partition.

        :param Text table_name:
        :param dict[Text, T] partitions: A dictionary mapping table partition key names to the values matching this
            partition.
        :param dict[Text, Text] schema_to_table_name_map: Mapping of names in current schema to table in which it is
            being inserted.  Currently not supported.  Must be None.
        :param bool partitions_in_table:  Whether or not the partition columns exist in the data being submitted.
            Currently not supported.  Must be false
        :param bool append_to_partition: Whether or not to append new values to a partition.  Currently not supported.
        :return: Text
        """
        partition_string = ""
        where_string = ""
        identity_dict = {n: n for n in _six.iterkeys(self.type.sdk_columns)}
        identity_dict.update(schema_to_table_name_map or {})
        schema_to_table_name_map = identity_dict
        table_to_schema_name_map = {v: k for k, v in _six.iteritems(schema_to_table_name_map)}

        if partitions:
            partition_conditions = []
            for partition_name, partition_value in _six.iteritems(partitions):
                if not isinstance(partition_name, (str, _six.text_type)):
                    raise _user_exceptions.FlyteTypeException(
                        expected_type={str, _six.text_type},
                        received_type=type(partition_name),
                        received_value=partition_name,
                        additional_msg="All partition names must be type str.",
                    )
                if type(partition_value) not in _ALLOWED_PARTITION_TYPES:
                    raise _user_exceptions.FlyteTypeException(
                        expected_type=_ALLOWED_PARTITION_TYPES,
                        received_type=type(partition_value),
                        received_value=partition_value,
                        additional_msg="Partition {name} has an unsupported type.".format(name=partition_name),
                    )

                # We need the string to be quoted in the query, so let's take repr of it.
                if isinstance(partition_value, (str, _six.text_type)):
                    partition_value = repr(partition_value)
                partition_conditions.append(
                    "{partition_name} = {partition_value}".format(
                        partition_name=partition_name, partition_value=partition_value
                    )
                )
            partition_formatter = "PARTITION (\n\t{conditions}\n)"
            partition_string = partition_formatter.format(conditions=",\n\t".join(partition_conditions))

        if partitions_in_table and partitions:
            where_clauses = []
            for partition_name, partition_value in partitions:
                where_clauses.append(
                    "\n\t\t{schema_name} = {value_str} AND ".format(
                        schema_name=table_to_schema_name_map[partition_name], value_str=partition_value,
                    )
                )
            where_string = "WHERE\n\t\t{where_clauses}".format(where_clauses=" AND\n\t\t".join(where_clauses))

        if where_string or partitions_in_table:
            raise _user_exceptions.FlyteAssertion(
                "Currently, the partition values should not be present in the schema pushed to Hive."
            )
        if append_to_partition:
            raise _user_exceptions.FlyteAssertion(
                "Currently, partitions can only be overwritten, they cannot be appended."
            )
        if not partitions:
            raise _user_exceptions.FlyteAssertion(
                "Currently, partition values MUST be specified for writing to a table."
            )

        return _format_insert_partition_query(
            remote_location=self.remote_location, table_name=table_name, partition_string=partition_string,
        )

    def compare_dataframe_to_schema(self, data_frame, column_subset=None, read=False):
        """
        Do necessary type checking of a pandas data frame.  Raise exception if it doesn't match.
        :param pandas.DateFrame data_frame: data frame to type check
        :param list[Text] column_subset:
        :param bool read: Used to alter error message for more clarity.
        """
        all_columns = list(data_frame.columns.values)
        schema_column_names = list(self.type.sdk_columns.keys())

        # Skip checking if we have a generic schema type (no specified columns)
        if not schema_column_names:
            return

        # If we specify a subset of columns, ensure they all exist and then only take those columns
        if column_subset is not None:
            schema_column_names = []
            failed_columns = []
            for column in column_subset:
                if column not in self.type.sdk_columns:
                    failed_columns.append(column)
                else:
                    schema_column_names.append(column)

            if len(failed_columns) > 0:
                additional_msg = ""
                raise _user_exceptions.FlyteAssertion(
                    "{} was/where requested but could not be found in the schema: {}.{}".format(
                        failed_columns, self.type.sdk_columns, additional_msg
                    )
                )

        if not all(c in all_columns for c in schema_column_names):
            raise _user_exceptions.FlyteTypeException(
                expected_type=self.type.sdk_columns,
                received_type=data_frame.columns,
                additional_msg="Mismatch between the data frame's column names {} and schema's column names {} "
                "with strict_names=True.".format(all_columns, schema_column_names),
            )

        # This only iterates if the Schema has specified columns.
        for name in schema_column_names:
            literal_type = self.type.sdk_columns[name].to_flyte_literal_type()
            dtype = data_frame[name].dtype

            # TODO np.issubdtype is deprecated. Replace it
            if all(
                not _np.issubdtype(dtype, allowed_type)
                for allowed_type in get_supported_literal_types_to_pandas_types()[literal_type]
            ):
                if read:
                    read_or_write_msg = "read data frame object from schema"
                else:
                    read_or_write_msg = "write data frame object to schema"
                additional_msg = (
                    "Cannot {read_write} because the types do not match. Column "
                    "'{name}' did not pass type checking.  Note: If your "
                    "column contains null values, the types might not transition as expected between parquet and "
                    "pandas.  For more information, see: "
                    "http://arrow.apache.org/docs/python/pandas.html#arrow-pandas-conversion".format(
                        read_write=read_or_write_msg, name=name
                    )
                )
                raise _user_exceptions.FlyteTypeException(
                    expected_type=get_supported_literal_types_to_pandas_types()[literal_type],
                    received_type=dtype,
                    additional_msg=additional_msg,
                )

    def cast_to(self, other_type):
        """
        :param SchemaType other_type:
        :rtype: Schema
        """
        if len(other_type.sdk_columns) > 0:
            for k, v in _six.iteritems(other_type.sdk_columns):
                if k not in self.type.sdk_columns:
                    raise _user_exceptions.FlyteTypeException(
                        self.type,
                        other_type,
                        additional_msg="Cannot cast because a required column '{}' was not found.".format(k),
                        received_value=self,
                    )
                if (
                    not isinstance(v, _base_sdk_types.FlyteSdkType)
                    or v.to_flyte_literal_type() != self.type.sdk_columns[k].to_flyte_literal_type()
                ):
                    raise _user_exceptions.FlyteTypeException(
                        self.type.sdk_columns[k],
                        v,
                        additional_msg="Cannot cast because the column type for column '{}' does not match.".format(k),
                    )
        return Schema(self.remote_location, mode=self.mode, schema_type=other_type)

    @_exception_scopes.system_entry_point
    def upload(self):
        """
        Upload the schema to the remote location
        """
        if "w" not in self.mode:
            raise _user_exceptions.FlyteAssertion("Cannot upload a read-only schema!")

        elif not self.local_path:
            raise _user_exceptions.FlyteAssertion(
                "The schema is not currently backed by a local directory "
                "and therefore cannot be uploaded.  Please write to this before "
                "attempting an upload."
            )
        else:
            # TODO: Introduce system logging
            # logging.info("Putting {} -> {}".format(self.local_path, self.remote_location))
            _data_proxy.Data.put_data(self.local_path, self.remote_location, is_multipart=True)
