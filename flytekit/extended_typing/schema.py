from __future__ import annotations

import datetime as _datetime
import os
import typing
from abc import abstractmethod
from enum import Enum
from typing import Type

import numpy as _np

from flytekit import FlyteContext
from flytekit.annotated.type_engine import T, TypeEngine, TypeTransformer
from flytekit.configuration import sdk
from flytekit.models.literals import Literal, Scalar, Schema
from flytekit.models.types import LiteralType, SchemaType
from flytekit.plugins import pandas


class SchemaFormat(Enum):
    """
    Represents the the schema storage format (at rest).
    Currently only parquet is supported
    """

    PARQUET = "parquet"
    # ARROW = "arrow"
    # HDF5 = "hdf5"
    # CSV = "csv"
    # RECORDIO = "recordio"


class SchemaOpenMode(Enum):
    READ = "r"
    WRITE = "w"


def generate_ordered_files(directory: os.PathLike, n: int) -> typing.Generator[os.PathLike, None, None]:
    for i in range(n):
        yield os.path.join(directory, f"{i:05}")


class FlyteSchema(object):
    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

    @classmethod
    def format(cls) -> SchemaFormat:
        return SchemaFormat.PARQUET

    def __class_getitem__(
        cls, columns: typing.Dict[str, typing.Type], fmt: SchemaFormat = SchemaFormat.PARQUET
    ) -> Type[FlyteSchema]:
        if columns is None:
            return FlyteSchema

        if not isinstance(columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict of column names and their types, received {type(columns)}"
            )

        if len(columns) == 0:
            return FlyteSchema

        if not isinstance(fmt, SchemaFormat):
            raise AssertionError(
                f"Only FlyteSchemaFormat types are supported, received format is {fmt} of type {type(fmt)}"
            )

        class _TypedSchema(FlyteSchema):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteSchema

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return columns

            @classmethod
            def format(cls) -> SchemaFormat:
                return fmt

        return _TypedSchema

    def __init__(
        self,
        local_path: os.PathLike = None,
        remote_path: os.PathLike = None,
        supported_mode: SchemaOpenMode = SchemaOpenMode.WRITE,
        downloader: typing.Callable[[str, os.PathLike], None] = None,
    ):

        if supported_mode == SchemaOpenMode.READ and remote_path is None:
            raise ValueError("To create a FlyteSchema in read mode, remote_path is required")
        if (
            supported_mode == SchemaOpenMode.WRITE
            and local_path is None
            and FlyteContext.current_context().file_access is None
        ):
            raise ValueError("To create a FlyteSchema in write mode, local_path is required")

        if local_path is None:
            local_path = FlyteContext.current_context().file_access.get_random_local_directory()
        self._local_path = local_path
        self._remote_path = remote_path
        self._supported_mode = supported_mode
        # This is a special attribute that indicates if the data was either downloaded or uploaded
        self._downloaded = False
        self._downloader = downloader

    @property
    def local_path(self) -> os.PathLike:
        return self._local_path

    @property
    def remote_path(self) -> os.PathLike:
        return self._remote_path

    @property
    def supported_mode(self) -> SchemaOpenMode:
        return self._supported_mode

    def open(
        self, dataframe_fmt: type = pandas.DataFrame, override_mode: SchemaOpenMode = None
    ) -> typing.Union[SchemaReader, SchemaWriter]:
        """
        Will return a reader or writer depending on the mode of the object when created. This mode can be
        overriden, but will depend on whether the override can be performed. For example, if the Object was
        created in a read-mode a "write mode" override is not allowed.
        if the object was created in write-mode, a read is allowed.
        :param dataframe_fmt:
        :param override_mode:
        :return:
        """
        if override_mode and self._supported_mode == SchemaOpenMode.READ and override_mode == SchemaOpenMode.WRITE:
            raise AssertionError("Readonly schema cannot be opened in write mode!")

        if self._supported_mode == SchemaOpenMode.READ and not self._downloaded:
            self._downloader(self._remote_path, self._local_path)
            self._downloaded = True
        return SchemaEngine.open(t=dataframe_fmt, s=self, mode=override_mode if override_mode else self._supported_mode)

    def as_readonly(self) -> FlyteSchema:
        if self._supported_mode == SchemaOpenMode.READ:
            return self
        s = FlyteSchema.__class_getitem__(self.columns(), self.format())(
            local_path=self.local_path,
            # Dummy path is ok, as we will assume data is already downloaded and will not download again
            remote_path=self.remote_path if self.remote_path else "",
            supported_mode=SchemaOpenMode.READ,
        )
        s._downloaded = True
        return s


class SchemaReader(typing.Generic[T]):
    def __init__(self, local_dir: os.PathLike, cols: typing.Dict[str, type], fmt: SchemaFormat):
        self._local_dir = local_dir
        self._fmt = fmt
        self._columns = cols

    @property
    def column_names(self) -> typing.Optional[typing.List[str]]:
        if self._columns:
            return list(self._columns.keys())
        return None

    @abstractmethod
    def _read(self, *path: os.PathLike, **kwargs) -> T:
        pass

    def iter(self, **kwargs) -> typing.Generator[T, None, None]:
        with os.scandir(self._local_dir) as it:
            for entry in it:
                if not entry.name.startswith(".") and entry.is_file():
                    yield self._read(entry.path, **kwargs)

    def all(self, **kwargs) -> T:
        files = []
        with os.scandir(self._local_dir) as it:
            for entry in it:
                if not entry.name.startswith(".") and entry.is_file():
                    files.append(entry.path)

        return self._read(*files, **kwargs)


class PandasSchemaReader(SchemaReader[pandas.DataFrame]):
    def __init__(self, local_dir: os.PathLike, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(local_dir, cols, fmt)
        self._parquet_engine = _PARQUETIO_ENGINES[sdk.PARQUET_ENGINE.get()]

    def _read(self, *path: os.PathLike, **kwargs) -> pandas.DataFrame:
        return self._parquet_engine.read(*path, columns=self.column_names, **kwargs)


class SchemaWriter(typing.Generic[T]):
    def __init__(self, local_dir: os.PathLike, cols: typing.Dict[str, type], fmt: SchemaFormat):
        self._local_dir = local_dir
        self._fmt = fmt
        self._columns = cols
        # TODO This should be change to send a stop instead of hardcoded to 1024
        self._file_name_gen = generate_ordered_files(self._local_dir, 1024)

    @abstractmethod
    def _write(self, df: T, path: os.PathLike, **kwargs):
        pass

    def write(self, *dfs, **kwargs):
        for df in dfs:
            self._write(df, next(self._file_name_gen), **kwargs)


class PandasSchemaWriter(SchemaWriter[pandas.DataFrame]):
    def __init__(self, local_dir: os.PathLike, cols: typing.Optional[typing.Dict[str, type]], fmt: SchemaFormat):
        super().__init__(local_dir, cols, fmt)
        self._parquet_engine = _PARQUETIO_ENGINES[sdk.PARQUET_ENGINE.get()]

    def _write(self, df: T, path: os.PathLike, **kwargs):
        return self._parquet_engine.write(df, to_file=path, **kwargs)


class PandasDataFrameTransformer(TypeTransformer[pandas.DataFrame]):
    """
    Transforms a pd.DataFrame to Schema without column types.
    """

    def __init__(self):
        super().__init__("PandasDataFrame<->GenericSchema", pandas.DataFrame)
        self._parquet_engine = _PARQUETIO_ENGINES[sdk.PARQUET_ENGINE.get()]

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
        local_dir = ctx.file_access.get_random_local_directory()
        w = PandasSchemaWriter(local_dir=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        w.write(python_val)
        remote_path = ctx.file_access.get_random_remote_directory()
        ctx.file_access.put_data(local_dir, remote_path, is_multipart=True)
        return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type())))

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[pandas.DataFrame]
    ) -> pandas.DataFrame:
        if not (lv and lv.scalar and lv.scalar.schema):
            return pandas.DataFrame()
        local_dir = ctx.file_access.get_random_local_directory()
        ctx.file_access.download_directory(lv.scalar.schema.uri, local_dir)
        r = PandasSchemaReader(local_dir=local_dir, cols=None, fmt=SchemaFormat.PARQUET)
        return r.all()


class FlyteSchemaTransformer(TypeTransformer[FlyteSchema]):
    _SUPPORTED_TYPES: typing.Dict[type : SchemaType.SchemaColumn.SchemaColumnType] = {
        _np.int32: SchemaType.SchemaColumn.SchemaColumnType.INTEGER,
        _np.int64: SchemaType.SchemaColumn.SchemaColumnType.INTEGER,
        _np.uint32: SchemaType.SchemaColumn.SchemaColumnType.INTEGER,
        _np.uint64: SchemaType.SchemaColumn.SchemaColumnType.INTEGER,
        int: SchemaType.SchemaColumn.SchemaColumnType.INTEGER,
        _np.float32: SchemaType.SchemaColumn.SchemaColumnType.FLOAT,
        _np.float64: SchemaType.SchemaColumn.SchemaColumnType.FLOAT,
        float: SchemaType.SchemaColumn.SchemaColumnType.FLOAT,
        _np.bool: SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN,
        bool: SchemaType.SchemaColumn.SchemaColumnType.BOOLEAN,
        _np.datetime64: SchemaType.SchemaColumn.SchemaColumnType.DATETIME,
        _datetime.datetime: SchemaType.SchemaColumn.SchemaColumnType.DATETIME,
        _np.timedelta64: SchemaType.SchemaColumn.SchemaColumnType.DURATION,
        _datetime.timedelta: SchemaType.SchemaColumn.SchemaColumnType.DURATION,
        _np.string_: SchemaType.SchemaColumn.SchemaColumnType.STRING,
        _np.str_: SchemaType.SchemaColumn.SchemaColumnType.STRING,
        _np.object_: SchemaType.SchemaColumn.SchemaColumnType.STRING,
        str: SchemaType.SchemaColumn.SchemaColumnType.STRING,
    }

    def __init__(self):
        super().__init__("FlyteSchema Transformer", FlyteSchema)

    def _get_schema_type(self, t: Type[FlyteSchema]) -> SchemaType:
        converted_cols: typing.List[SchemaType.SchemaColumn] = []
        for k, v in t.columns().items():
            if v not in self._SUPPORTED_TYPES:
                raise AssertionError(f"type {v} is currently not supported by FlyteSchema")
            converted_cols.append(SchemaType.SchemaColumn(name=k, type=self._SUPPORTED_TYPES[v]))
        return SchemaType(columns=converted_cols)

    def get_literal_type(self, t: Type[FlyteSchema]) -> LiteralType:
        return LiteralType(schema=self._get_schema_type(t))

    def to_literal(
        self, ctx: FlyteContext, python_val: FlyteSchema, python_type: Type[FlyteSchema], expected: LiteralType
    ) -> Literal:
        if isinstance(python_val, FlyteSchema):
            remote_path = python_val.remote_path
            if remote_path is None or remote_path == "":
                remote_path = ctx.file_access.get_random_remote_path()
            ctx.file_access.put_data(python_val.local_path, remote_path, is_multipart=True)
            return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type(python_type))))
        elif isinstance(python_val, pandas.DataFrame):
            local_dir = ctx.file_access.get_random_local_directory()
            w = PandasSchemaWriter(local_dir=local_dir, cols=python_type.columns(), fmt=python_type.format())
            w.write(python_val)
            remote_path = ctx.file_access.get_random_remote_directory()
            ctx.file_access.put_data(local_dir, remote_path, is_multipart=True)
            return Literal(scalar=Scalar(schema=Schema(remote_path, self._get_schema_type(python_type))))
        else:
            raise AssertionError(
                f"Only FlyteSchemaWriter or Pandas Dataframe object can be returned from a task,"
                f" returned object type {type(python_val)}"
            )

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[FlyteSchema]) -> FlyteSchema:
        if not (lv and lv.scalar and lv.scalar.schema):
            raise AssertionError("Can only covert a literal schema to a FlyteSchema")

        def downloader(x, y):
            ctx.file_access.download_directory(x, y)

        return expected_python_type(
            local_path=ctx.file_access.get_random_local_directory(),
            remote_path=lv.scalar.schema.uri,
            downloader=downloader,
            supported_mode=SchemaOpenMode.READ,
        )


class SchemaEngine(object):
    _SCHEMA_HANDLERS: typing.Dict[type, typing.Tuple[Type[SchemaReader], Type[SchemaWriter]]] = {}

    @classmethod
    def register_handler(cls, t: type, r: Type[SchemaReader], w: Type[SchemaWriter]):
        if t in cls._SCHEMA_HANDLERS:
            raise ValueError(f"SchemaHandler for {t} already registered")
        cls._SCHEMA_HANDLERS[t] = (r, w)

    @classmethod
    def open(cls, t: type, s: FlyteSchema, mode: SchemaOpenMode) -> typing.Union[SchemaReader[T], SchemaWriter[T]]:
        if t not in cls._SCHEMA_HANDLERS:
            raise ValueError(f"DataFrames of type {t} are not supported currently")
        r, w = cls._SCHEMA_HANDLERS[t]
        if mode == SchemaOpenMode.WRITE:
            return w(s.local_path, s.columns(), s.format())
        return r(s.local_path, s.columns(), s.format())


class ParquetIO(object):
    PARQUET_ENGINE = "pyarrow"

    def _read(self, chunk: os.PathLike, columns: typing.List[str], **kwargs) -> pandas.DataFrame:
        return pandas.read_parquet(chunk, columns=columns, engine=self.PARQUET_ENGINE, **kwargs)

    def read(self, *files: os.PathLike, columns: typing.List[str] = None, **kwargs) -> pandas.DataFrame:
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

    def _read(self, chunk: os.PathLike, columns: typing.List[str], **kwargs) -> pandas.DataFrame:
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
    FastParquetIO.PARQUET_ENGINE: FastParquetIO(),
}


SchemaEngine.register_handler(pandas.DataFrame, PandasSchemaReader, PandasSchemaWriter)
TypeEngine.register(PandasDataFrameTransformer())
TypeEngine.register(FlyteSchemaTransformer())
