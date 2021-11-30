from __future__ import annotations

import datetime as _datetime
import os
import typing
from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, List, Type, Union
from flytekit.loggers import logger

import numpy as _np
import pandas as pd
import pyarrow as pa

from flytekit import FlyteContext
from flytekit.core.type_engine import TypeTransformer
from flytekit.extend import TypeEngine
from flytekit.models import types as type_models
from flytekit.models.literals import Literal, Scalar
from flytekit.models.literals import StructuredDataset as _StructuredDataset
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import LiteralType, SimpleType, StructuredDatasetType
from flytekit.types.schema.types_pandas import PandasDataFrameTransformer

T = typing.TypeVar("T")  # Local dataframe type
FT = typing.TypeVar("FT")  # Local dataframe type
IT = typing.TypeVar("IT")  # Intermediate type


class DatasetFormat(Enum):
    PARQUET = "parquet"
    BIGQUERY = "bigquery"

    @classmethod
    def value_of(cls, value):
        for k, v in cls.__members__.items():
            if k == value:
                return v
        else:
            raise ValueError(f"'{cls.__name__}' enum not found for '{value}'")


class StructuredDataset(object):
    """
    This is the main schema class that users should use.
    """

    @classmethod
    def columns(cls) -> typing.Dict[str, typing.Type]:
        return {}

    @classmethod
    def column_names(cls) -> typing.List[str]:
        return [k for k, v in cls.columns().items()]

    def __class_getitem__(cls, columns: typing.Dict[str, typing.Type]) -> Type[StructuredDataset]:
        if columns is None:
            return cls

        if not isinstance(columns, dict):
            raise AssertionError(
                f"Columns should be specified as an ordered dict "
                f"of column names and their types, received {type(columns)}"
            )

        if len(columns) == 0:
            return cls

        class _TypedStructuredDataset(StructuredDataset):
            # Get the type engine to see this as kind of a generic
            __origin__ = StructuredDataset

            @classmethod
            def columns(cls) -> typing.Dict[str, typing.Type]:
                return columns

        return _TypedStructuredDataset

    def __init__(
        self,
        dataframe: typing.Optional[typing.Any] = None,
        local_path: typing.Union[os.PathLike, str] = None,
        remote_path: str = None,
        file_format: DatasetFormat = DatasetFormat.PARQUET,
        downloader: typing.Callable[[str, os.PathLike], None] = None,
        metadata: typing.Optional[StructuredDatasetMetadata] = None,
    ):
        self._dataframe = dataframe
        self._local_path = local_path
        self._remote_path = remote_path
        self._file_format = file_format
        # This is a special attribute that indicates if the data was either downloaded or uploaded
        self._downloaded = False
        self._downloader = downloader
        self._metadata = metadata

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
    def file_format(self) -> DatasetFormat:
        return self._file_format

    def open_as(self, df_type: Type) -> typing.Any:
        return FLYTE_DATASET_TRANSFORMER.download(self.file_format, df_type, self.remote_path)


class DataFrameTransformer(TypeTransformer[T], ABC):
    def __init__(self, name: str, dataframe_type: Type[T]):
        super().__init__(name, dataframe_type)

    def get_literal_type(self, t: Type[T]) -> LiteralType:
        raise NotImplementedError

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        raise NotImplementedError


class PartialDataFrameTransformer(ABC):
    @abstractmethod
    @property
    def python_type(self) -> Type[T]:
        raise NotImplementedError

    @abstractmethod
    @property
    def intermediate_type(self) -> Type[IT]:
        raise NotImplementedError

    @abstractmethod
    def to_intermediate_value(
        self, ctx: FlyteContext, python_value: typing.Any, python_type: Type[T], literal_type: typing.Optional[LiteralType]
    ) -> IT:
        raise NotImplementedError

    @abstractmethod
    def to_python_value(self, ctx: FlyteContext, python_type: Type[T], intermediate_value: IT, literal: Literal) -> T:
        raise NotImplementedError


class IntermediateTypeHandler(ABC):
    @abstractmethod
    @property
    def intermediate_type(self) -> Type[IT]:
        raise NotImplementedError

    @abstractmethod
    def to_literal(
        self,
        ctx: FlyteContext,
        intermediate_value: IT,
        literal_type: LiteralType,
        python_val: typing.Optional[T],
        python_type: typing.Optional[Type[T]],
    ) -> Literal:
        """
        :param ctx:
        :param intermediate_value:
        :param literal_type:
        :param python_val:
        :param python_type:
        :return:
        :raises:
            TypeError: When converting from an intermediate value to a Literal, raise this if the handler is
            incapable of handling the incoming value. The type engine will automatically try the next one.
        """
        raise NotImplementedError

    @abstractmethod
    def to_intermediate_value(self, ctx: FlyteContext, lv: Literal, python_type: Type[T]) -> IT:
        """
        :raises:
            TypeError: When converting from a Literal to an intermediate value, contributors should raise this
            to signal to the StructuredDatasetTransformerEngine that the code isn't capable of handling the
            incoming value. The engine will automatically try the next one.
        """
        raise NotImplementedError


class StructuredDatasetTransformerEngine(TypeTransformer[StructuredDataset]):
    """
    Think of this transformer as a higher-level meta transformer that is used for all the dataframe types.
    If you are bringing a custom data frame type, or any data frame type, to flytekit, instead of
    registering with the main type engine, you should register with this transformer instead.

    This transformer is special in that breaks the transformer into two pieces internally.

    to_literal

        Python value -> DataFrameTransformer -> Flyte Literal
        or
        Python value -> PartialDataFrameTransformer -> IntermediateFormatHandler -> Flyte Literal

    to_python_value

        Flyte Literal -> DataFrameTransformer -> Python value
        or
        Flyte Literal -> IntermediateFormatHandler -> PartialDataFrameTransformer -> Python value

    Basically the plugin contributor needs to write either
      i.) a concrete class that's effectively a regular type engine transformer (minus a couple functions)
      ii.) concrete classes for one or both of the two step transformer base classes.

    Note that the paths taken for a given data frame type do not have to be the same. Let's say you
    want to store a custom dataframe into BigQuery. You can
    #. When going in the ``to_literal`` direction: Write a custom handler that converts directly from the dataframe type to a literal, persisting the data
      into BigQuery.
    #. When going in the ``to_python_value`` direction: Write a custom handler that converts from a local
    Parquet file into your custom data frame type. The handlers that come bundled with flytekit will automatically
    handle the translation from BigQuery into a local Parquet file.
    """

    FULL_DF_TRANSFORMERS: Dict[Type[T], DataFrameTransformer] = {}
    PARTIAL_DF_TRANSFORMERS: List[PartialDataFrameTransformer] = {}
    INTERMEDIATE_HANDLERS: List[IntermediateTypeHandler] = []

    Handlers = Union[DataFrameTransformer, IntermediateTypeHandler, PartialDataFrameTransformer]

    _REGISTER_TYPES: List[Type] = []

    def __init__(self):
        super().__init__("StructuredDataset Transformer", StructuredDataset)
        self._type_assertions_enabled = False

    def register_handler(
        self,
        h: Handlers,
    ):
        """
        Call this with any handler to register it with this dataframe meta-transformer
        """
        if isinstance(h, DataFrameTransformer):
            self.FULL_DF_TRANSFORMERS[h.python_type] = h
        elif isinstance(h, PartialDataFrameTransformer):
            self.PARTIAL_DF_TRANSFORMERS.append(h)
        elif isinstance(h, IntermediateTypeHandler):
            self.INTERMEDIATE_HANDLERS.append(h)
        else:
            raise TypeError(f"We don't support this type of handler {h}")

    def assert_type(self, t: Type[StructuredDataset], v: typing.Any):
        return

    def to_literal(
        self, ctx: FlyteContext, python_val: Union[StructuredDataset, typing.Any], python_type: Union[Type[StructuredDataset], Type], expected: LiteralType
    ) -> Literal:
        # If the type signature has the StructuredDataset class, it will, or at least should, also be a
        # StructuredDataset instance.
        if issubclass(python_type, StructuredDataset):
            assert isinstance(python_val, StructuredDataset)
            return self.encode(ctx, python_val.dataframe, python_type, )

        return self.encode(ctx, python_val, python_type, expected)
    
    def encode(
            self, ctx: FlyteContext, python_val: typing.Any,
            python_type: Union[Type[StructuredDataset], Type], expected: LiteralType
    ) -> Literal:
        # Otherwise, it's a dataframe instance/type of some kind.
        if python_type in self.FULL_DF_TRANSFORMERS:
            transformer = self.FULL_DF_TRANSFORMERS[python_type]
            return transformer.to_literal(ctx, python_val, python_type, expected)

        for partial in self.PARTIAL_DF_TRANSFORMERS:
            try:
                inter_value = partial.to_intermediate_value(ctx, python_val, python_type, expected)
            except TypeError:
                logger.debug(f"Skipping partial transformer {partial}")
                continue
            for handler in self.INTERMEDIATE_HANDLERS:
                # This issubclass relationship is intentional - it's more likely that an intermediate value
                # handler can handle multiple intermediate types, than a partial DF transformer can handle
                # multiple intermediate types.
                if issubclass(partial.intermediate_type, handler.intermediate_type):
                    try:
                        return handler.to_literal(ctx, inter_value, expected, python_val, python_type)
                    except TypeError as e:
                        # todo: same caveat here, may need to except all exceptions.
                        logger.debug(f"Skipping intermediate handler {handler}")
                        continue

        raise ValueError(f"Cannot turn {python_val} with signature {python_type} to literal")

    def to_python_value(self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]) -> T:
        fmt = DatasetFormat.value_of(lv.scalar.structured_dataset.metadata.format)
        uri = lv.scalar.structured_dataset.uri
        meta = lv.scalar.structured_dataset.metadata

        # Either a StructuredDataset type or some dataframe type.
        if issubclass(expected_python_type, StructuredDataset):
            return expected_python_type(remote_path=uri, file_format=fmt, metadata=meta)

        return self.open_as(ctx, lv, python_type=expected_python_type)

    def open_as(self, ctx: FlyteContext, lv: Literal, python_type: Type[FT]) -> FT:
        if python_type in self.FULL_DF_TRANSFORMERS:
            t = self.FULL_DF_TRANSFORMERS[python_type]
            return t.to_python_value(ctx, lv=lv, expected_python_type=python_type)

        # If not, we need to try all the things.
        for handler in self.INTERMEDIATE_HANDLERS:
            intermediate_type = handler.intermediate_type
            try:
                inter_value = handler.to_intermediate_value(ctx, lv, python_type)
            except TypeError as e:
                logger.debug(f"Intermediate handler {handler} doesn't handle value {lv} {e}")
                continue

            for partial in self.PARTIAL_DF_TRANSFORMERS:
                # This issubclass relationship is intentional, see comment in to_literal
                if issubclass(partial.intermediate_type, intermediate_type):
                    try:
                        return partial.to_python_value(ctx, python_type, inter_value, literal=lv)
                    except TypeError as e:
                        logger.debug(f"Partial transformer {partial} doesn't handle value {inter_value} {e}")
                        continue
                    # TODO: Need to make this except more errors, maybe all Exceptions.

        # At this point, we've tried all the intermediate handlers, and for all the intermediate handlers that
        # didn't raise an error, we tried all the partial transformers that have that intermediate type.
        raise ValueError(f"Could not handle {lv}")

    def get_literal_type(self, t: typing.Union[Type[StructuredDataset], typing.Any]) -> LiteralType:
        """
        Provide a concrete implementation so that writers of custom dataframe handlers since there's nothing that
        special about the literal type. Any dataframe type will always be associated with the structured dataset type.
        The other aspects of it - columns, external schema type, etc. can be read from associated metadata.

        :param t: The python dataframe type, which is mostly ignored.
        """
        # todo: fill in columns by checking for typing.annotated metadata
        return LiteralType(structured_dataset_type=StructuredDatasetType(columns=[]))

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        # todo: technically we should return the dataframe type specified in the constructor, but to do that,
        #   we'd have to store that, which we don't do today. See possibly #1363
        if literal_type.structured_dataset_type is not None:
            return StructuredDataset


FLYTE_DATASET_TRANSFORMER = StructuredDatasetTransformer()
TypeEngine.register(FLYTE_DATASET_TRANSFORMER)


class PDtoBQ:
    ...


class PDtoPQ:
    ...


class PQtoS3:
    ...


class S3toPD:
    ...
