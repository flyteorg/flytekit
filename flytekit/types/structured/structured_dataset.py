from __future__ import annotations

import datetime as _datetime
import os
import typing
from typing import Any, Dict, List, Type, Union

import numpy as _np
import pyarrow as pa

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformer
from flytekit.extend import TypeEngine
from flytekit.models import types as type_models
from flytekit.models.literals import Literal, Scalar, StructuredDataset, StructuredDatasetMetadata
from flytekit.models.types import LiteralType, StructuredDatasetType
from flytekit.types.schema.types import SchemaFormat

T = typing.TypeVar("T")


class FlyteDatasetMetadata(object):
    def __init__(
        self,
        columns: Dict[str, typing.Type],
        fmt: SchemaFormat = None,
        path: Union[os.PathLike, str] = None,
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
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

    def __class_getitem__(cls, columns: typing.Dict[str, typing.Type]) -> Type[FlyteDataset]:
        if columns is None:
            return cls

        if not isinstance(columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict "
                f"of column names and their types, received {type(columns)}"
            )

        if len(columns) == 0:
            return cls

        class _TypedFlyteDataset(FlyteDataset):
            # Get the type engine to see this as kind of a generic
            __origin__ = FlyteDataset

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return columns

        return _TypedFlyteDataset

    def __init__(
        self,
        dataframe: typing.Optional[typing.Any] = None,
        local_path: typing.Union[os.PathLike, str] = None,
        remote_path: str = None,
        file_format: SchemaFormat = SchemaFormat.PARQUET,
        downloader: typing.Callable[[str, os.PathLike], None] = None,
    ):
        self._dataframe = dataframe
        self._local_path = local_path
        self._remote_path = remote_path
        self._file_format = file_format
        # This is a special attribute that indicates if the data was either downloaded or uploaded
        self._downloaded = False
        self._downloader = downloader

    @property
    def dataframe(self) -> Type[typing.Any]:
        return self._dataframe

    @property
    def local_path(self) -> os.PathLike:
        return self._local_path

    @property
    def remote_path(self) -> str:
        return typing.cast(str, self._remote_path)

    @property
    def file_format(self) -> SchemaFormat:
        return self._file_format

    def open_as(self, df_type: Type) -> typing.Any:
        return FLYTE_DATASET_TRANSFORMER.download(self.file_format, df_type, self.remote_path)


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

    def encode(self, *args, **kwargs):
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

    def persist(self, *args, **kwargs):
        raise NotImplementedError


class DatasetDecodingHandler:
    """
    Inherit from this base class if you want to convert from an intermediate storage type (e.g. a local
    Parquet file, local serialized Arrow BatchRecord file, etc.) to a Python value.

    Flytekit ships with default handlers that know how to:

    - Turn a local Parquet file into a pandas DataFrame
    - Turn an Arrow RecordBatch into a pandas DataFrame
    """

    def decode(self, *args, **kwargs):
        raise NotImplementedError

    def python_type(self):
        raise NotImplementedError


class DatasetRetrievalHandler:
    """
    Inherit from this base class if you want to tell flytekit how to read persisted data, and turn it into
    either a Python object ready for user consumption, or an intermediate construct like a Parquet file,
    an Arrow RecordBatch, or anything else that has a DatasetDecodingHandler associated with it.
    """

    def retrieve(self, path: str, *args, **kwargs):
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

    DATASET_DECODING_HANDLERS: Dict[Type[Any], Dict[Type[Any], DatasetDecodingHandler]] = {}
    DATASET_ENCODING_HANDLERS: Dict[Type[Any], Dict[Type[Any], DatasetEncodingHandler]] = {}
    DATASET_PERSISTENCE_HANDLERS: Dict[Type[typing.Any], Dict[Type[Any], DatasetPersistenceHandler]] = {}
    DATASET_RETRIEVAL_HANDLERS: Dict[Type[Any], Dict[Type[Any], DatasetRetrievalHandler]] = {}
    Handlers = typing.Union[
        DatasetDecodingHandler, DatasetEncodingHandler, DatasetPersistenceHandler, DatasetRetrievalHandler
    ]
    _REGISTER_TYPES: List[Type] = []

    def __init__(self):
        super().__init__("FlyteDataset Transformer", FlyteDataset)

    def _get_dataset_column_literal_type(self, t: Type):
        if t in self._SUPPORTED_TYPES:
            return self._SUPPORTED_TYPES[t]
        if hasattr(t, "__origin__") and t.__origin__ == list:
            return type_models.LiteralType(collection_type=self._get_dataset_column_literal_type(t.__args__[0]))
        if hasattr(t, "__origin__") and t.__origin__ == dict:
            return type_models.LiteralType(map_value_type=self._get_dataset_column_literal_type(t.__args__[1]))
        raise AssertionError(f"type {t} is currently not supported by FlyteDataset")

    def _get_dataset_type(self, t: Type[FlyteDataset]) -> StructuredDatasetType:
        # TODO: Convert pandas, arrow, and other dataframes column datatype to StructuredDatasetType
        if t != FlyteDataset:
            return StructuredDatasetType(columns=[])
        converted_cols: typing.List[StructuredDatasetType.DatasetColumn] = []
        for k, v in t.columns().items():
            lt = self._get_dataset_column_literal_type(v)
            converted_cols.append(StructuredDatasetType.DatasetColumn(name=k, literal_type=lt))
        return StructuredDatasetType(columns=converted_cols)

    def register_handler(
        self,
        from_type: Type[T],
        to_type: Type[T],
        h: Handlers,
    ):
        """
        Call this with any handler to register it with this dataframe meta-transformer
        """
        if from_type not in self._REGISTER_TYPES:
            self._REGISTER_TYPES.append(from_type)
            TypeEngine.register_additional_type(self, from_type)

        if to_type not in self._REGISTER_TYPES:
            self._REGISTER_TYPES.append(to_type)
            TypeEngine.register_additional_type(self, to_type)

        registry: dict

        if isinstance(h, DatasetRetrievalHandler):
            registry = self.DATASET_RETRIEVAL_HANDLERS
        elif isinstance(h, DatasetPersistenceHandler):
            registry = self.DATASET_PERSISTENCE_HANDLERS
        elif isinstance(h, DatasetEncodingHandler):
            registry = self.DATASET_ENCODING_HANDLERS
        elif isinstance(h, DatasetDecodingHandler):
            registry = self.DATASET_DECODING_HANDLERS

        if from_type not in registry:
            registry[from_type] = {}
        registry[from_type][to_type] = h

    def assert_type(self, t: Type[FlyteDataset], v: typing.Any):
        return

    def to_literal(
        self, ctx: FlyteContext, python_val: typing.Any, python_type: Type[FlyteDataset], expected: LiteralType
    ) -> Literal:
        uri: str = ""
        file_format: SchemaFormat
        # 1. Python value is FlyteDataset
        if isinstance(python_val, FlyteDataset):
            uri = python_val.remote_path or ctx.file_access.get_random_remote_path()
            df = python_val.dataframe
            file_format = python_val.file_format
            if python_val.local_path:
                ctx.file_access.put_data(python_val.local_path, uri, is_multipart=False)
            elif df is not None:
                self.upload(type(df), file_format, uri, df)
        # 2. Python value is Dataframe
        else:
            uri = ctx.file_access.get_random_remote_path()
            file_format = SchemaFormat.PARQUET
            self.upload(type(python_val), file_format, uri, python_val)

        return Literal(
            scalar=Scalar(
                structured_dataset=StructuredDataset(
                    uri=uri,
                    metadata=StructuredDatasetMetadata(
                        format=file_format.name, structured_dataset_type=self._get_dataset_type(python_type)
                    ),
                )
            )
        )

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        fmt = SchemaFormat.value_of(lv.scalar.structured_dataset.metadata.format)
        uri = lv.scalar.structured_dataset.uri

        if FlyteDataset in expected_python_type.mro():
            return expected_python_type(remote_path=uri, file_format=fmt)

        return self.download(fmt, expected_python_type, uri)

    def get_literal_type(self, t: typing.Union[Type[FlyteDataset], typing.Any]) -> LiteralType:
        return LiteralType(structured_dataset_type=self._get_dataset_type(t))

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        raise ValueError(f"Not yet implemented {literal_type}")

    def download(self, from_type: Union[Type, SchemaFormat], to_type: Union[Type, SchemaFormat], uri: str) -> Any:
        retrieve_handler = self._get_handler(from_type, to_type, self.DATASET_RETRIEVAL_HANDLERS)
        if retrieve_handler:
            return retrieve_handler.retrieve(path=uri)
        retrieve_handler = self._get_handler(
            from_type, self._get_intermediate_format(), self.DATASET_RETRIEVAL_HANDLERS
        )
        decoding_handler = self._get_handler(self._get_intermediate_format(), to_type, self.DATASET_DECODING_HANDLERS)
        if retrieve_handler and decoding_handler:
            table = retrieve_handler.retrieve(uri)
            return decoding_handler.decode(table)
        raise ValueError(f"Not yet implemented download data {to_type} from {from_type}")

    def upload(self, from_type: Type, to_type: Union[Type, SchemaFormat], uri: str, df: Any):
        persist_handler = self._get_handler(from_type, to_type, self.DATASET_PERSISTENCE_HANDLERS)
        if persist_handler:
            persist_handler.persist(df, uri)
            return
        encoding_handler = self._get_handler(from_type, self._get_intermediate_format(), self.DATASET_ENCODING_HANDLERS)
        persist_handler = self._get_handler(self._get_intermediate_format(), to_type, self.DATASET_PERSISTENCE_HANDLERS)

        if encoding_handler and persist_handler:
            table = encoding_handler.encode(df)
            persist_handler.persist(table, uri)
        else:
            raise NotImplementedError(f"Not yet implemented upload data {to_type} from {from_type}")

    @classmethod
    def _get_intermediate_format(cls):
        # This type should be configurable
        return pa.Table

    @classmethod
    def _get_handler(cls, from_type: Type, to_type: Type, handler: Dict[Type, dict]) -> typing.Optional[Handlers]:
        if from_type not in handler:
            return None
        elif to_type not in handler[from_type]:
            return None
        return handler[from_type][to_type]


FLYTE_DATASET_TRANSFORMER = FlyteDatasetTransformerEngine()
TypeEngine.register(FLYTE_DATASET_TRANSFORMER)
