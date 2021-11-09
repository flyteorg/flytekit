from __future__ import annotations

import datetime as _datetime
import os
import typing
from abc import ABC, abstractmethod
from typing import Type

import numpy as _np
import pandas as pd
import pyarrow as pa

from flytekit import FlyteContext
from flytekit.core.data_persistence import split_protocol
from flytekit.core.type_engine import TypeTransformer
from flytekit.extend import TypeEngine
from flytekit.models.core import types as type_models
from flytekit.models.core.literals import Literal, Scalar, StructuredDataset, StructuredDatasetMetadata
from flytekit.models.core.types import LiteralType, StructuredDatasetType
from flytekit.types.schema.types import SchemaFormat

T = typing.TypeVar("T")


class FlyteDatasetMetadata(object):
    def __init__(
        self,
        columns: typing.Dict[str, typing.Type],
        fmt: SchemaFormat = None,
        path: typing.Union[os.PathLike, str] = None,
    ):
        self.columns = columns
        self.fmt = fmt
        self.path = path


class FlyteDataset(object):
    """
    This is the main schema class that users should use.
    """

    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def format(cls) -> SchemaFormat:
        return SchemaFormat.PARQUET

    @classmethod
    def path(cls) -> typing.Union[os.PathLike, str]:
        return ""

    @classmethod
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

    def __class_getitem__(cls, metadata: FlyteDatasetMetadata) -> Type[FlyteDataset]:
        if metadata.columns is None:
            return cls

        if not isinstance(metadata.columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict "
                f"of column names and their types, received {type(metadata.columns)}"
            )

        if len(metadata.columns) == 0:
            return cls

        class _TypedFlyteDataset(FlyteDataset):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteDataset

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return metadata.columns

            @classmethod
            def format(cls) -> SchemaFormat:
                if metadata.fmt:
                    return metadata.fmt
                if metadata.path and split_protocol(metadata.path)[0] == "bq":
                    return SchemaFormat.BIGQUERY
                return SchemaFormat.PARQUET

            @classmethod
            def path(cls) -> os.PathLike:
                return metadata.path

        return _TypedFlyteDataset

    def __init__(
        self,
        local_path: typing.Union[os.PathLike, str] = None,
        remote_path: str = None,
        downloader: typing.Callable[[str, os.PathLike], None] = None,
    ):
        self._local_path = local_path
        self._remote_path = remote_path
        # This is a special attribute that indicates if the data was either downloaded or uploaded
        self._downloaded = False
        self._downloader = downloader

    @property
    def local_path(self) -> os.PathLike:
        return self._local_path

    @property
    def remote_path(self) -> str:
        return typing.cast(str, self._remote_path)

    def open_as(self, df_type: Type) -> typing.Any:
        try:
            if df_type not in FLYTE_DATASET_TRANSFORMER.DATASET_RETRIEVAL_HANDLERS:
                table = FLYTE_DATASET_TRANSFORMER.DATASET_RETRIEVAL_HANDLERS[pa.Table].read_parquet(self.remote_path)
                return FLYTE_DATASET_TRANSFORMER.DATASET_DECODING_HANDLERS[df_type].from_arrow(table)
            if self.format() is SchemaFormat.PARQUET:
                return FLYTE_DATASET_TRANSFORMER.DATASET_RETRIEVAL_HANDLERS[df_type].read_parquet(self.remote_path)
            elif self.format() is SchemaFormat.BIGQUERY:
                return FLYTE_DATASET_TRANSFORMER.DATASET_RETRIEVAL_HANDLERS[df_type].read_bigquery(self.remote_path)
        except Exception as e:
            raise NotImplementedError(f"We don't support {df_type}", e)


# These two base classes represent the to_literal side of the Type Engine.


class DatasetEncodingHandler:
    """
    Inherit from this base class if you want to tell flytekit how to turn an instance of a dataframe (e.g.
    pd.DataFrame or spark.DataFrame or even your own custom data frame object) into a serialized structure of
    some kind (e.g. a Parquet file on local disk, in-memory block of Arrow data).

    This only represents half of the story of taking an object in Python memory, and turning it into a Flyte literal.
    The other half is persisting it somehow. See DatasetPersistenceHandler.

    Flytekit ships with default handlers that can:

    - Write pandas DataFrame objects to Parquet files
    - Write Pandera data frame objects to Parquet files
    """

    def to_arrow(self, *args, **kwargs):
        raise NotImplementedError


class DatasetPersistenceHandler:
    """
    Inherit from this base class if you want to tell flytekit how to take an encoded dataset (e.g. a local Parquet
    file, a block of Arrow memory, even possibly a Python object), and persist it in some kind of a store, and
    return a flyte Literal.

    Flytekit ships with default handlers that know how to:

    - Write Parquet files to AWS/GCS
    - Write pandas DataFrame objects directly to BigQuery
    - Write Arrow objects/files to AWS/GCS/BigQuery
    """

    def to_parquet(self, *args, **kwargs):
        raise NotImplementedError

    def to_orc(self, *args, **kwargs):
        raise NotImplementedError

    def to_csv(self, *args, **kwargs):
        raise NotImplementedError

    def to_bigquery(self, *args, **kwargs):
        raise NotImplementedError


# These two base classes represent the to_python_value side of the Type Engine.


class DatasetDecodingHandler:
    """
    Inherit from this base class if you want to convert from an intermediate storage type (e.g. a local
    Parquet file, local serialized Arrow BatchRecord file, etc.) to a Python value.

    Flytekit ships with default handlers that know how to:

    - Turn a local Parquet file into a pandas DataFrame
    - Turn an Arrow RecordBatch into a pandas DataFrame
    """

    def from_arrow(self, *args, **kwargs):
        raise NotImplementedError

    def python_type(self):
        raise NotImplementedError


class DatasetRetrievalHandler:
    """
    Inherit from this base class if you want to tell flytekit how to read persisted data, and turn it into
    either a Python object ready for user consumption, or an intermediate construct like a Parquet file,
    an Arrow RecordBatch, or anything else that has a DatasetDecodingHandler associated with it.

    """

    def read_parquet(self, *args, **kwargs):
        raise NotImplementedError

    def read_bigquery(self, *args, **kwargs):
        raise NotImplementedError

    def read_orc(self, *args, **kwargs):
        raise NotImplementedError

    def read_csv(self, *args, **kwargs):
        raise NotImplementedError


class FlyteDatasetTransformerEngine(TypeTransformer[FlyteDataset]):
    """
    Think of this transformer as a higher-level meta transformer that is used for all the dataframe types.
    If you are bringing a custom data frame type, or any data frame type, to flytekit, instead of
    registering with the main type engine, you should register with this transformer instead.

    This transformer is special in that breaks the transformer into two pieces internally.

    to_literal

        Python value -> DatasetEncodingHandler -> DatasetPersistenceHandler -> Flyte Literal

    to_python_value

        Flyte Literal -> DatasetRetrievalHandler -> DatasetDecodingHandler -> Python value

    Basically the union of these four components have to comprise one of the original regular type engine
    transformers.

    Note that the paths taken for a given data frame type do not have to be the same. Let's say you
    want to store a custom dataframe into BigQuery. You can
    #. When going in the ``to_literal`` direction: Write a custom handler that converts directly from the dataframe type to a literal, persisting the data
      into BigQuery.
    #. When going in the ``to_python_value`` direction: Write a custom handler that converts from a local
    Parquet file into your custom data frame type. The handlers that come bundled with flytekit will automatically
    handle the translation from BigQuery into a local Parquet file.
    """

    _SUPPORTED_TYPES: typing.Dict[Type, LiteralType] = {
        _np.int32: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.int64: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.uint32: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.uint64: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        int: type_models.LiteralType(simple=type_models.SimpleType.INTEGER),
        _np.float32: type_models.LiteralType(simple=type_models.SimpleType.FLOAT),
        _np.float64: type_models.LiteralType(simple=type_models.SimpleType.FLOAT),
        float: type_models.LiteralType(simple=type_models.SimpleType.FLOAT),
        _np.bool_: type_models.LiteralType(simple=type_models.SimpleType.BOOLEAN),  # type: ignore
        bool: type_models.LiteralType(simple=type_models.SimpleType.BOOLEAN),
        _np.datetime64: type_models.LiteralType(simple=type_models.SimpleType.DATETIME),
        _datetime.datetime: type_models.LiteralType(simple=type_models.SimpleType.DATETIME),
        _np.timedelta64: type_models.LiteralType(simple=type_models.SimpleType.DURATION),
        _datetime.timedelta: type_models.LiteralType(simple=type_models.SimpleType.DURATION),
        _np.string_: type_models.LiteralType(simple=type_models.SimpleType.STRING),
        _np.str_: type_models.LiteralType(simple=type_models.SimpleType.STRING),
        _np.object_: type_models.LiteralType(simple=type_models.SimpleType.STRING),
        str: type_models.LiteralType(simple=type_models.SimpleType.STRING),
    }

    DATASET_DECODING_HANDLERS: typing.Dict[Type[typing.Any], DatasetDecodingHandler] = {}
    DATASET_ENCODING_HANDLERS: typing.Dict[Type[typing.Any], DatasetEncodingHandler] = {}
    DATASET_PERSISTENCE_HANDLERS: typing.Dict[Type[typing.Any], DatasetPersistenceHandler] = {}
    DATASET_RETRIEVAL_HANDLERS: typing.Dict[Type[typing.Any], DatasetRetrievalHandler] = {}

    _REGISTER_TYPES: typing.List[Type] = []

    def __init__(self):
        super().__init__("FlyteDataset Transformer", FlyteDataset)

    # TODO: This needs to be made recursive
    def _get_dataset_type(self, t: Type[FlyteDataset]) -> StructuredDatasetType:
        converted_cols = []
        # TODO (Kevin Su): implement it
        # for k, v in t.columns().items():
        #     if v not in self._SUPPORTED_TYPES:
        #         raise AssertionError(f"type {v} is currently not supported by FlyteDataset")
        #     converted_cols.append(
        #         type_models.StructuredDatasetType.DatasetColumn(name=k, literal_type=self._SUPPORTED_TYPES[v])
        #     )
        return type_models.StructuredDatasetType(columns=converted_cols)

    def register_handler(
        self,
        df_format: Type[T],
        h: typing.Union[
            DatasetDecodingHandler, DatasetEncodingHandler, DatasetPersistenceHandler, DatasetRetrievalHandler
        ],
    ):
        """
        Call this with any handler to register it with this dataframe meta-transformer
        """
        if df_format not in self._REGISTER_TYPES:
            self._REGISTER_TYPES.append(df_format)
            TypeEngine.register_additional_type(self, df_format)
        if isinstance(h, DatasetRetrievalHandler):
            self.DATASET_RETRIEVAL_HANDLERS[df_format] = h
        elif isinstance(h, DatasetPersistenceHandler):
            self.DATASET_PERSISTENCE_HANDLERS[df_format] = h
        elif isinstance(h, DatasetEncodingHandler):
            self.DATASET_ENCODING_HANDLERS[df_format] = h
        elif isinstance(h, DatasetDecodingHandler):
            self.DATASET_DECODING_HANDLERS[df_format] = h

    #     @classmethod
    #     def get_dataset_retrieval_handler(cls, t: Type) -> DatasetRetrievalHandler:
    #         if t not in cls._DATASET_RETRIEVAL_HANDLERS:
    #             raise ValueError(f"DataFrames of type {t} are not supported currently")
    #         return cls._DATASET_RETRIEVAL_HANDLERS[t]

    def assert_type(self, t: Type[FlyteDataset], v: typing.Any):
        return

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[FlyteDataset], expected: LiteralType
    ) -> Literal:
        uri = ""
        storage_format = ""
        # 1. Python value is FlyteDataset
        if isinstance(python_val, FlyteDataset):
            remote_path = python_val.remote_path
            if remote_path is None or remote_path == "":
                remote_path = ctx.file_access.get_random_remote_path()
                ctx.file_access.put_data(python_val.local_path, remote_path, is_multipart=False)
            uri = remote_path
            storage_format = python_val.format().name
        # 2. Python value is Dataframe, and signature is FlyteDataset[FlyteDatasetMetadata()]
        elif hasattr(python_type, "__origin__") and python_type.__origin__ is FlyteDataset:
            if type(python_val) not in self._REGISTER_TYPES:
                raise NotImplementedError
            # Convert dataframe to arrow
            if type(python_val) not in self.DATASET_PERSISTENCE_HANDLERS:
                python_val = self.DATASET_ENCODING_HANDLERS[type(python_val)].to_arrow(python_val)
            if python_type.format() == SchemaFormat.PARQUET:
                uri = python_type.path() or ctx.file_access.get_random_remote_path()
                self.DATASET_PERSISTENCE_HANDLERS[type(python_val)].to_parquet(python_val, uri)
                storage_format = SchemaFormat.PARQUET.name
            elif python_type.format() == SchemaFormat.BIGQUERY:
                uri = python_type.path()
                if uri is None:
                    raise ValueError
                storage_format = SchemaFormat.BIGQUERY.name
                self.DATASET_PERSISTENCE_HANDLERS[type(python_val)].to_bigquery(python_val, uri)
        # 3. Python value is Dataframe and signature is FlyteDataset or dataframe
        else:
            uri = ctx.file_access.get_random_local_path()
            # Convert dataframe to arrow
            if type(python_val) not in self.DATASET_PERSISTENCE_HANDLERS:
                python_val = self.DATASET_ENCODING_HANDLERS[type(python_val)].to_arrow(python_val)
            self.DATASET_PERSISTENCE_HANDLERS[type(python_val)].to_parquet(python_val, uri)
            storage_format = SchemaFormat.PARQUET.name

        return Literal(
            scalar=Scalar(
                structured_dataset=StructuredDataset(
                    uri=uri,
                    metadata=StructuredDatasetMetadata(
                        format=storage_format, structured_dataset_type=self._get_dataset_type(python_type)
                    ),
                )
            )
        )

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        fmt = lv.scalar.structured_dataset.metadata.format
        uri = lv.scalar.structured_dataset.uri

        if FlyteDataset in expected_python_type.mro():
            return expected_python_type(remote_path=uri)

        if fmt is SchemaFormat.PARQUET.name:
            if expected_python_type not in self.DATASET_RETRIEVAL_HANDLERS:
                arrow_df = self.DATASET_RETRIEVAL_HANDLERS[pa.Table].read_parquet(path=uri)
                return self.DATASET_DECODING_HANDLERS[expected_python_type].from_arrow(arrow_df)
            return self.DATASET_RETRIEVAL_HANDLERS[expected_python_type].read_parquet(path=uri)
        elif fmt is SchemaFormat.BIGQUERY.name:
            if expected_python_type not in self.DATASET_RETRIEVAL_HANDLERS:
                arrow_df = self.DATASET_RETRIEVAL_HANDLERS[pa.Table].read_bigquery(path=uri)
                return self.DATASET_DECODING_HANDLERS[expected_python_type].from_arrow(arrow_df)
            return self.DATASET_RETRIEVAL_HANDLERS[expected_python_type].read_bigquery(path=uri)
        else:
            raise ValueError(f"Not yet implemented {fmt}")

    def get_literal_type(self, t: typing.Union[Type[FlyteDataset], typing.Any]) -> LiteralType:
        return LiteralType(structured_dataset_type=self._get_dataset_type(t))

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        raise ValueError(f"Not yet implemented {literal_type}")


FLYTE_DATASET_TRANSFORMER = FlyteDatasetTransformerEngine()
TypeEngine.register(FLYTE_DATASET_TRANSFORMER)
