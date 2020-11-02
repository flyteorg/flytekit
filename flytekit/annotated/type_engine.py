from __future__ import annotations

import datetime as _datetime
import json as _json
import mimetypes
import os
import typing
from abc import ABC, abstractmethod
from enum import Enum
from typing import Type

from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flytekit import typing as flyte_typing
from flytekit.annotated.context_manager import FlyteContext
from flytekit.common.types import primitives as _primitives
from flytekit.configuration import sdk
from flytekit.models import interface as _interface_models
from flytekit.models import types as _type_models
from flytekit.models.core import types as _core_types
from flytekit.models.literals import (
    Blob,
    BlobMetadata,
    Literal,
    LiteralCollection,
    LiteralMap,
    Primitive,
    Scalar,
    Schema,
)
from flytekit.models.types import LiteralType, SchemaType, SimpleType
from flytekit.plugins import pandas

T = typing.TypeVar("T")


class TypeTransformer(typing.Generic[T]):
    """
    Base transformer type that should be implemented for every python native type that can be handled by flytekit
    """

    def __init__(self, name: str, t: Type[T], enable_type_assertions: bool = True):
        self._t = t
        self._name = name
        self._type_assertions_enabled = enable_type_assertions

    @property
    def name(self):
        return self._name

    @property
    def python_type(self) -> Type[T]:
        """
        This returns the python type
        """
        return self._t

    @property
    def type_assertions_enabled(self) -> bool:
        """
        Indicates if the transformer wants type assertions to be enabled at the core type engine layer
        """
        return self._type_assertions_enabled

    @abstractmethod
    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Converts the python type to a Flyte LiteralType
        """
        raise NotImplementedError("Conversion to LiteralType should be implemented")

    @abstractmethod
    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        raise NotImplementedError(f"Conversion to Literal for python type {python_type} not implemented")

    @abstractmethod
    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        raise NotImplementedError(
            f"Conversion to python value expected type {expected_python_type} from literal not implemented"
        )

    def __repr__(self):
        return f"{self._name} Transforms ({self._t}) to Flyte native"

    def __str__(self):
        return str(self.__repr__())


class SimpleTransformer(TypeTransformer[T]):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(
            self,
            name: str,
            t: Type[T],
            lt: LiteralType,
            to_literal_transformer: typing.Callable[[T], Literal],
            from_literal_transformer: typing.Callable[[Literal], T],
    ):
        super().__init__(name, t)
        self._lt = lt
        self._to_literal_transformer = to_literal_transformer
        self._from_literal_transformer = from_literal_transformer

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        return self._lt

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        return self._to_literal_transformer(python_val)

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        return self._from_literal_transformer(lv)


class RestrictedTypeError(Exception):
    pass


class RestrictedType(TypeTransformer[T], ABC):
    """
    A Simple implementation of a type transformer that uses simple lambdas to transform and reduces boilerplate
    """

    def __init__(self, name: str, t: Type[T]):
        super().__init__(name, t)

    def get_literal_type(self, t: Type[T] = None) -> LiteralType:
        raise RestrictedTypeError(f"Transformer for type{self.python_type} is restricted currently")


class TypeEngine(object):
    _REGISTRY: typing.Dict[type, TypeTransformer[T]] = {}

    @classmethod
    def register(cls, transformer: TypeTransformer):
        """
        This should be used for all types that respond with the right type annotation when you use type(...) function
        """
        if transformer.python_type in cls._REGISTRY:
            existing = cls._REGISTRY[transformer.python_type]
            raise ValueError(
                f"Transformer f{existing.name} for type{transformer.python_type} is already registered."
                f" Cannot override with {transformer.name}"
            )
        cls._REGISTRY[transformer.python_type] = transformer

    @classmethod
    def get_transformer(cls, python_type: Type) -> TypeTransformer[T]:
        if python_type in cls._REGISTRY:
            return cls._REGISTRY[python_type]
        if hasattr(python_type, "__origin__"):
            if python_type.__origin__ in cls._REGISTRY:
                return cls._REGISTRY[python_type.__origin__]
            raise ValueError(f"Generic Type{python_type.__origin__} not supported currently in Flytekit.")
        raise ValueError(f"Type{python_type} not supported currently in Flytekit. Please register a new transformer")

    @classmethod
    def to_literal_type(cls, python_type: Type) -> LiteralType:
        transformer = cls.get_transformer(python_type)
        return transformer.get_literal_type(python_type)

    @classmethod
    def to_literal(cls, ctx: FlyteContext, python_val: typing.Any, python_type: Type, expected: LiteralType) -> Literal:
        transformer = cls.get_transformer(python_type)
        lv = transformer.to_literal(ctx, python_val, python_type, expected)
        # TODO Perform assertion here
        return lv

    @classmethod
    def to_python_value(cls, ctx: FlyteContext, lv: Literal, expected_python_type: Type) -> typing.Any:
        transformer = cls.get_transformer(expected_python_type)
        return transformer.to_python_value(ctx, lv, expected_python_type)

    @classmethod
    def named_tuple_to_variable_map(cls, t: typing.NamedTuple) -> _interface_models.VariableMap:
        variables = {}
        for idx, (var_name, var_type) in enumerate(t._field_types.items()):
            literal_type = cls.to_literal_type(var_type)
            variables[var_name] = _interface_models.Variable(type=literal_type, description=f"{idx}")
        return _interface_models.VariableMap(variables=variables)

    @classmethod
    def literal_map_to_kwargs(
            cls, ctx: FlyteContext, lm: LiteralMap, python_types: typing.Dict[str, type]
    ) -> typing.Dict[str, typing.Any]:
        """
        Given a literal Map (usually an input into a task - intermediate), convert to kwargs for the task
        """
        if len(lm.literals) != len(python_types):
            raise ValueError(
                f"Received more input values {len(lm.literals)}" f" than allowed by the input spec {len(python_types)}"
            )

        return {k: TypeEngine.to_python_value(ctx, lm.literals[k], v) for k, v in python_types.items()}

    @classmethod
    def get_available_transformers(cls) -> typing.KeysView[Type]:
        """
        Returns all python types for which transformers are available
        """
        return cls._REGISTRY.keys()


class ListTransformer(TypeTransformer[T]):
    def __init__(self):
        super().__init__("Typed List", list)

    @staticmethod
    def get_sub_type(t: Type[T]) -> Type[T]:
        """
        Return the generic Type T of the List
        """
        if hasattr(t, "__origin__") and t.__origin__ is list:
            if hasattr(t, "__args__"):
                return t.__args__[0]
        raise ValueError("Only generic typing.List[T] type is supported.")

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        """
        Only univariate Lists are supported in Flyte
        """
        try:
            sub_type = TypeEngine.to_literal_type(self.get_sub_type(t))
            return _type_models.LiteralType(collection_type=sub_type)
        except Exception as e:
            raise ValueError(f"Type of Generic List type is not supported, {e}")

    def to_literal(self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType) -> Literal:
        t = self.get_sub_type(python_type)
        lit_list = [TypeEngine.to_literal(ctx, x, t, expected.collection_type) for x in python_val]
        return Literal(collection=LiteralCollection(literals=lit_list))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        st = self.get_sub_type(expected_python_type)
        return [TypeEngine.to_python_value(ctx, x, st) for x in lv.collection.literals]


class DictTransformer(TypeTransformer[dict]):
    def __init__(self):
        super().__init__("Typed Dict", dict)

    @staticmethod
    def get_dict_types(t: Type[dict]) -> (type, type):
        """
        Return the generic Type T of the Dict
        """
        if hasattr(t, "__origin__") and t.__origin__ is dict:
            if hasattr(t, "__args__"):
                return t.__args__
        return None

    def get_literal_type(self, t: Type[dict]) -> LiteralType:
        tp = self.get_dict_types(t)
        if tp:
            if tp[0] == str:
                try:
                    sub_type = TypeEngine.to_literal_type(tp[1])
                    return _type_models.LiteralType(map_value_type=sub_type)
                except Exception as e:
                    raise ValueError(f"Type of Generic List type is not supported, {e}")
        return _primitives.Generic.to_flyte_literal_type()

    def to_literal(
            self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[dict], expected: LiteralType
    ) -> Literal:
        if expected and expected.simple and expected.simple == SimpleType.STRUCT:
            return Literal(scalar=Scalar(generic=_json_format.Parse(_json.dumps(python_val), _struct.Struct())))

        lit_map = {}
        for k, v in python_val.items():
            if type(k) != str:
                raise ValueError("Flyte MapType expects all keys to be strings")
            k_type, v_type = self.get_dict_types(python_type)
            lit_map[k] = TypeEngine.to_literal(ctx, v, v_type, expected.map_value_type)
        return Literal(map=LiteralMap(literals=lit_map))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[dict]) -> dict:
        if lv and lv.map and lv.map.literals:
            tp = self.get_dict_types(expected_python_type)
            py_map = {}
            for k, v in lv.map.literals.items():
                # TODO the type of the key is not known here?
                # TODO we could just just a reverse map in the engine from literal type to find the right converter
                py_map[k] = TypeEngine.to_python_value(ctx, v, tp[1])
            return py_map
        if lv and lv.scalar and lv.scalar.generic:
            return _json.loads(_json_format.MessageToJson(lv.scalar.generic))
        raise ValueError(f"Cannot convert from {lv} to {expected_python_type}")


class TextIOTransformer(TypeTransformer[typing.TextIO]):
    def __init__(self):
        super().__init__(name="TextIO", t=typing.TextIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".txt"], dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: typing.TextIO) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(), )

    def to_literal(
            self, ctx: FlyteContext, python_val: typing.TextIO, python_type: Type[typing.TextIO], expected: LiteralType
    ) -> Literal:
        raise NotImplementedError("Implement handle for TextIO")

    def to_python_value(
            self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[typing.TextIO]
    ) -> typing.TextIO:
        # TODO rename to get_auto_local_path()
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probably the responsibility of the framework to close() this
        return open(local_path, "r")


class BinaryIOTransformer(TypeTransformer[typing.BinaryIO]):
    def __init__(self):
        super().__init__(name="BinaryIO", t=typing.BinaryIO)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".bin"], dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[typing.BinaryIO]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(), )

    def to_literal(
            self, ctx: FlyteContext, python_val: typing.BinaryIO, python_type: Type[typing.BinaryIO],
            expected: LiteralType
    ) -> Literal:
        raise NotImplementedError("Implement handle for TextIO")

    def to_python_value(
            self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[typing.BinaryIO]
    ) -> typing.BinaryIO:
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probability the responsibility of the framework to close this
        return open(local_path, "rb")


class PathLikeTransformer(TypeTransformer[os.PathLike]):
    def __init__(self):
        super().__init__(name="os.PathLike", t=os.PathLike)

    def _blob_type(self) -> _core_types.BlobType:
        return _core_types.BlobType(
            format=mimetypes.types_map[".bin"], dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE,
        )

    def get_literal_type(self, t: Type[os.PathLike]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(), )

    def to_literal(
            self, ctx: FlyteContext, python_val: os.PathLike, python_type: Type[os.PathLike], expected: LiteralType
    ) -> Literal:
        # TODO we could guess the mimetype and allow the format to be changed at runtime. thus a non existent format
        #      could be replaced with a guess format?
        rpath = ctx.file_access.get_random_remote_path()
        ctx.file_access.put_data(python_val, rpath, is_multipart=False)
        return Literal(scalar=Scalar(blob=Blob(metadata=BlobMetadata(expected.blob), uri=rpath)))

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[os.PathLike]) -> os.PathLike:
        # TODO rename to get_auto_local_path()
        local_path = ctx.file_access.get_random_local_path()
        ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)
        # TODO it is probability the responsibility of the framework to close this
        return local_path


class FlyteFilePathTransformer(TypeTransformer[flyte_typing.FlyteFilePath]):
    def __init__(self):
        super().__init__(name="FlyteFilePath", t=flyte_typing.FlyteFilePath)

    @staticmethod
    def get_format(t: Type[flyte_typing.FlyteFilePath]) -> str:
        return t.extension()

    def _blob_type(self, format: str) -> _core_types.BlobType:
        return _core_types.BlobType(format=format, dimensionality=_core_types.BlobType.BlobDimensionality.SINGLE, )

    def get_literal_type(self, t: Type[flyte_typing.FlyteFilePath]) -> LiteralType:
        return _type_models.LiteralType(blob=self._blob_type(format=FlyteFilePathTransformer.get_format(t)))

    def to_literal(
            self,
            ctx: FlyteContext,
            python_val: flyte_typing.FlyteFilePath,
            python_type: Type[flyte_typing.FlyteFilePath],
            expected: LiteralType,
    ) -> Literal:
        remote_path = ""
        if isinstance(python_val, flyte_typing.FlyteFilePath):
            remote_path = python_val.remote_path if python_val.remote_path else ""
        else:
            if not (isinstance(python_val, os.PathLike) or isinstance(python_val, str)):
                raise AssertionError(f"Expected FlyteFilePath or os.PathLike object, received {type(python_val)}")

        if remote_path == "":
            remote_path = ctx.file_access.get_random_remote_path()

        ctx.file_access.put_data(f"{python_val}", remote_path, is_multipart=False)
        meta = BlobMetadata(type=self._blob_type(format=FlyteFilePathTransformer.get_format(python_type)))
        return Literal(scalar=Scalar(blob=Blob(metadata=meta, uri=remote_path)))

    def to_python_value(
            self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[flyte_typing.FlyteFilePath]
    ) -> flyte_typing.FlyteFilePath:
        local_path = ctx.file_access.get_random_local_path()

        # TODO improve this to accept the blob uri and local path as an argument?
        def _downloader():
            return ctx.file_access.get_data(lv.scalar.blob.uri, local_path, is_multipart=False)

        expected_format = FlyteFilePathTransformer.get_format(expected_python_type)
        return flyte_typing.FlyteFilePath[expected_format](local_path, _downloader, lv.scalar.blob.uri)


class SchemaIO(typing.Generic[T]):

    @abstractmethod
    def read(self, *files: typing.List[os.PathLike], columns=None, **kwargs) -> T:
        pass

    @abstractmethod
    def write(self, df: T, to_file: os.PathLike, **kwargs):
        pass


class ParquetIO(SchemaIO):
    PARQUET_ENGINE = "pyarrow"

    def _read(self, chunk: os.PathLike, columns: typing.List[str], **kwargs):
        return pandas.read_parquet(chunk, columns=columns, engine=self.PARQUET_ENGINE, **kwargs)

    def read(self, files: typing.List[os.PathLike], columns=None, **kwargs) -> pandas.DataFrame:
        frames = [self._read(chunk=f, columns=columns, **kwargs) for f in files if os.path.getsize(f) > 0]
        if len(frames) == 1:
            return frames[0]
        elif len(frames) > 1:
            return pandas.concat(frames, copy=True)
        return pandas.Dataframe()

    def write(
            self,
            df: pandas.DataFrame,
            to_file: os.PathLike,
            coerce_timestamps: str = "us",
            allow_truncated_timestamps: bool = False,
            **kwargs,
    ):
        """
        Writes data frame as a chunk to the local directory owned by the Schema object.  Will later be uploaded to s3.
        :param df: data frame to write as parquet
        :param to_file: Sink file to write the dataframe to
        :param coerce_timestamps: format to store timestamp in parquet. 'us', 'ms', 's' are allowed values.
            Note: if your timestamps will lose data due to the coercion, your write will fail!  Nanoseconds are
            problematic in the Parquet format and will not work. See allow_truncated_timestamps.
        :param allow_truncated_timestamps: default False. Allow truncation when coercing timestamps to a coarser
            resolution.
        """
        # TODO @ketan validate and remove this comment, as python 3 all strings are unicode
        # Convert all columns to unicode as pyarrow's parquet reader can not handle mixed strings and unicode.
        # Since columns from Hive are returned as unicode, if a user wants to add a column to a dataframe returned from
        # Hive, then output the new data, the user would have to provide a unicode column name which is unnatural.
        df.to_parquet(
            to_file,
            coerce_timestamps=coerce_timestamps,
            allow_truncated_timestamps=allow_truncated_timestamps,
            **kwargs,
        )


class FastParquetIO(ParquetIO):
    PARQUET_ENGINE = "fastparquet"

    def _read(self, chunk: os.PathLike, columns: typing.List[str], **kwargs):
        from fastparquet import ParquetFile as _ParquetFile
        from fastparquet import thrift_structures as _ts

        # TODO Follow up to figure out if this is not needed anymore
        # https://github.com/dask/fastparquet/issues/414#issuecomment-478983811
        df = pandas.read_parquet(chunk, columns=columns, engine=self.PARQUET_ENGINE, index=False)
        df_column_types = df.dtypes
        pf = _ParquetFile(chunk)
        schema_column_dtypes = {l.name: l.type for l in list(pf.schema.schema_elements)}

        for idx in df_column_types[df_column_types == "float16"].index.tolist():
            # A hacky way to get the string representations of the column types of a parquet schema
            # Reference:
            # https://github.com/dask/fastparquet/blob/f4ecc67f50e7bf98b2d0099c9589c615ea4b06aa/fastparquet/schema.py
            if _ts.parquet_thrift.Type._VALUES_TO_NAMES[schema_column_dtypes[idx]] == "BOOLEAN":
                df[idx] = df[idx].astype("object")
                df[idx].replace({0: False, 1: True, pandas.np.nan: None}, inplace=True)
        return df


_PARQUETIO_ENGINES: typing.Dict[str, ParquetIO] = {
    ParquetIO.PARQUET_ENGINE: ParquetIO(),
    FastParquetIO.PARQUET_ENGINE: FastParquetIO,
}


class FlyteSchema(object):
    class FlyteSchemaFormat(Enum):
        PARQUET = "parquet"

    # _SUPPORTED_TYPES = {
    #     {_np.int32, _np.int64, _np.uint32, _np.uint64},
    #     {_np.float32, _np.float64},
    #     {_np.bool},
    #     {_np.datetime64},
    #     {_np.timedelta64},
    #     {_np.object_, _np.str_, _np.string_},
    # }

    _SCHEMA_IO: typing.Dict[FlyteSchema.FlyteSchemaFormat: SchemaIO] = {
        FlyteSchemaFormat.PARQUET: _PARQUETIO_ENGINES[sdk.PARQUET_ENGINE.get()],
    }

    @classmethod
    def get_schema_io(cls) -> SchemaIO:
        if cls.format() not in cls._SCHEMA_IO:
            raise AssertionError(f"Unable to find a schema IO engine for file format {cls.format()}")
        return cls._SCHEMA_IO[cls.format()]

    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def format(cls) -> FlyteSchemaFormat:
        return FlyteSchema.FlyteSchemaFormat.PARQUET

    def __class_getitem__(cls, columns: typing.Dict[str, typing.Type],
                          fmt: FlyteSchemaFormat = FlyteSchemaFormat.PARQUET):
        if columns is None:
            return cls

        if not isinstance(columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict of column names and their types, received {type(columns)}")

        if len(columns) == 0:
            return cls

        if not isinstance(fmt, FlyteSchema.FlyteSchemaFormat):
            raise AssertionError(
                f"Only FlyteSchemaFormat types are supported, received format is {fmt} of type {type(fmt)}")

        class _TypedSchema(FlyteSchema):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteSchema

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return columns

            @classmethod
            def format(cls) -> FlyteSchema.FlyteSchemaFormat:
                return fmt

        return _TypedSchema

    def __init__(self, *args, **kwargs):
        """
        Want to support
         - different types of df (pandas, vaex, spark etc)
         - different number of dfs
         - or a list of files or a directory. Each of the files can be in one of the supported formats for a schema
           - in the beginning only parquet
         - it should support lazy streaming download and lazy streaming upload
        :param objs:
        """
        pass


def generate_ordered_files(directory: os.PathLike, n: int) -> typing.Generator[os.PathLike, None, None]:
    for i in range(n):
        yield os.path.join(directory, f"{i:05}")


class FlyteSchemaReader(FlyteSchema):
    """
    provides an iterator to read all
    """

    def __init__(self, remote_prefix: str, downloader: typing.Callable[[str, os.PathLike], typing.Any],
                 local_dir: os.PathLike):
        super().__init__()
        self._remote_prefx = remote_prefix
        self._downloader = downloader
        self._downloaded = False
        self._local_dir = local_dir

    def download(self):
        if self._downloaded:
            return
        self._downloader(self._remote_prefx, self._local_dir)
        self._downloaded = True

    def iter(self, **kwargs) -> typing.Generator:
        self.download()
        io = self.get_schema_io()
        with os.scandir(self._local_dir) as it:
            for entry in it:
                if not entry.name.startswith('.') and entry.is_file():
                    yield io.read(entry.path, **kwargs)

    def all(self, **kwargs):
        self.download()
        io = self.get_schema_io()
        files = []
        with os.scandir(self._local_dir) as it:
            for entry in it:
                if not entry.name.startswith('.') and entry.is_file():
                    files.append(entry.path)

        return io.read(*files, **kwargs)


class FlyteSchemaWriter(FlyteSchema):
    def __init__(self, local_dir: os.PathLike = None):
        super().__init__()
        if local_dir is None or local_dir == "":
            # Create default local dir
            pass
        self._local_dir = local_dir

        self._file_names = generate_ordered_files(self._local_dir)

    def local_dir(self) -> os.PathLike:
        return self._local_dir

    def write(self, *dfs, **kwargs):
        io = self.get_schema_io()
        for df in dfs:
            io.write(df, next(self._file_names), **kwargs)


class PandasDataFrameTransformer(TypeTransformer[pandas.DataFrame]):
    """
    Transforms a pd.DataFrame to Schema without column types.
    """

    def __init__(self, parquet_engine: ParquetIO):
        super().__init__("PandasDataFrame<->GenericSchema", pandas.DataFrame)
        self._parquet_engine = parquet_engine

    @staticmethod
    def _get_schema_type() -> SchemaType:
        return SchemaType(columns=[])

    def get_literal_type(self, t: Type[pandas.DataFrame]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type())

    def to_literal(
            self,
            ctx: FlyteContext,
            python_val: pandas.DataFrame,
            python_type: Type[pandas.DataFrame],
            expected: LiteralType,
    ) -> Literal:
        remote_path = ctx.file_access.get_random_remote_directory()
        local_dir = ctx.file_access.get_random_local_directory()
        f = list(generate_ordered_files(local_dir, 1))[0]
        self._parquet_engine.write(python_val, f)
        ctx.file_access.put_data(local_dir, remote_path, is_multipart=True)
        return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type())))

    def to_python_value(
            self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pandas.DataFrame]
    ) -> pandas.DataFrame:
        if not (lv and lv.scalar and lv.scalar.schema):
            return pandas.DataFrame()
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.download_directory(lv.scalar.schema.uri, local_dir)
        files = os.listdir(local_dir)
        files = [os.path.join(local_dir, f) for f in files]
        return self._parquet_engine.read(files)


def _register_default_type_transformers():
    TypeEngine.register(
        SimpleTransformer(
            "int",
            int,
            _primitives.Integer.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(integer=x))),
            lambda x: x.scalar.primitive.integer,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "float",
            float,
            _primitives.Float.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(float_value=x))),
            lambda x: x.scalar.primitive.float_value,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "bool",
            bool,
            _primitives.Boolean.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(boolean=x))),
            lambda x: x.scalar.primitive.boolean,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "str",
            str,
            _primitives.String.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(string_value=x))),
            lambda x: x.scalar.primitive.string_value,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "datetime",
            _datetime.datetime,
            _primitives.Datetime.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(datetime=x))),
            lambda x: x.scalar.primitive.datetime,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "timedelta",
            _datetime.timedelta,
            _primitives.Timedelta.to_flyte_literal_type(),
            lambda x: Literal(scalar=Scalar(primitive=Primitive(duration=x))),
            lambda x: x.scalar.primitive.duration,
        )
    )

    TypeEngine.register(
        SimpleTransformer(
            "none", None, _type_models.LiteralType(simple=_type_models.SimpleType.NONE), lambda x: None, lambda x: None,
        )
    )
    TypeEngine.register(ListTransformer())
    TypeEngine.register(DictTransformer())
    TypeEngine.register(FlyteFilePathTransformer())
    TypeEngine.register(TextIOTransformer())
    TypeEngine.register(PathLikeTransformer())
    TypeEngine.register(BinaryIOTransformer())

    parquet_io_engine = _PARQUETIO_ENGINES[sdk.PARQUET_ENGINE.get()]
    TypeEngine.register(PandasDataFrameTransformer(parquet_io_engine))

    # inner type is. Also unsupported are typing's Tuples. Even though you can look inside them, Flyte's type system
    # doesn't support these currently.
    # Confusing note: typing.NamedTuple is in here even though task functions themselves can return them. We just mean
    # that the return signature of a task can be a NamedTuple that contains another NamedTuple inside it.
    # Also, it's not entirely true that Flyte IDL doesn't support tuples. We can always fake them as structs, but we'll
    # hold off on doing that for now, as we may amend the IDL formally to support tuples.
    TypeEngine.register(RestrictedType("non typed tuple", tuple))
    TypeEngine.register(RestrictedType("non typed tuple", typing.Tuple))
    TypeEngine.register(RestrictedType("named tuple", typing.NamedTuple))


_register_default_type_transformers()
