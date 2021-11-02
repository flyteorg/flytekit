from __future__ import annotations

import datetime as _datetime
import os
import typing
from typing import Type
from flytekit.extend import TypeEngine
import numpy as _np

from flytekit.core.type_engine import TypeTransformer
from flytekit.models.core import types as type_models
from flytekit.models.core.types import LiteralType, StructuredDatasetType
from flytekit.types.schema.types import SchemaFormat

T = typing.TypeVar("T")


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

    def __class_getitem__(
        cls, columns: typing.Dict[str, typing.Type], fmt: str = SchemaFormat.PARQUET
    ) -> Type[FlyteDataset]:
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

            @classmethod
            def format(cls) -> str:
                return fmt

        return _TypedFlyteDataset

    def __init__(
        self,
        local_path: os.PathLike = None,
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

    def open_as(self, df_type: Type) -> Any:
        ...


# These two base classes represent the to_literal side of the Type Engine.


class DatasetEncodingHandler(object):
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


class DatasetPersistenceHandler(object):
    """
    Inherit from this base class if you want to tell flytekit how to take an encoded dataset (e.g. a local Parquet
    file, a block of Arrow memory, even possibly a Python object), and persist it in some kind of a store, and
    return a flyte Literal.

    Flytekit ships with default handlers that know how to:

    - Write Parquet files to AWS/GCS
    - Write pandas DataFrame objects directly to BigQuery
    - Write Arrow objects/files to AWS/GCS/BigQuery
    """

# These two base classes represent the to_python_value side of the Type Engine.


class DatasetDecodingHandler(object):
    """
    Inherit from this base class if you want to convert from an intermediate storage type (e.g. a local
    Parquet file, local serialized Arrow BatchRecord file, etc.) to a Python value.

    Flytekit ships with default handlers that know how to:

    - Turn a local Parquet file into a pandas DataFrame
    - Turn an Arrow RecordBatch into a pandas DataFrame
    """

    def python_type(self):
        ...


class DatasetRetrievalHandler(object):
    """
    Inherit from this base class if you want to tell flytekit how to read persisted data, and turn it into
    either a Python object ready for user consumption, or an intermediate construct like a Parquet file,
    an Arrow RecordBatch, or anything else that has a DatasetDecodingHandler associated with it.

    """


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

    def __init__(self):
        super().__init__("FlyteDataset Transformer", FlyteDataset)

    # TODO: This needs to be made recursive
    def _get_schema_type(self, t: Type[FlyteDataset]) -> StructuredDatasetType:
        converted_cols = []
        for k, v in t.columns().items():
            if v not in self._SUPPORTED_TYPES:
                raise AssertionError(f"type {v} is currently not supported by FlyteDataset")
            converted_cols.append(
                type_models.StructuredDatasetType.DatasetColumn(name=k, literal_type=self._SUPPORTED_TYPES[v])
            )
        return type_models.StructuredDatasetType(columns=converted_cols)

    def assert_type(self, t: Type[FlyteDataset], v: typing.Any):
        return

    def get_literal_type(self, t: typing.Union[Type[FlyteDataset], typing.Any]) -> LiteralType:
        return LiteralType(structured_dataset_type=self._get_schema_type(t))

    def register_handler(self, h: typing.Union[DatasetDecodingHandler, DatasetEncodingHandler, DatasetPersistenceHandler, DatasetRetrievalHandler]):
        """
        Call this with any handler to register it with this dataframe meta-transformer
        """
        if isinstance(h, DatasetRetrievalHandler):
            TypeEngine.register(self, )

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        raise ValueError(f"Not yet implemented {literal_type}")
