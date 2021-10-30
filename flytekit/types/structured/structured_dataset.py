from __future__ import annotations

import datetime as _datetime
import os
import typing
from abc import abstractmethod
from enum import Enum
from typing import Type

import numpy as _np
import pandas as pd

from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.models.core import types as type_models
from flytekit.models.core.literals import Literal, Scalar, Schema
from flytekit.models.core.types import LiteralType, SchemaType, StructuredDatasetType
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


class FlyteDatasetTransformer(TypeTransformer[FlyteDataset]):
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

    def get_literal_type(self, t: Type[FlyteDataset]) -> LiteralType:
        return LiteralType(structured_dataset_type=self._get_schema_type(t))

    def guess_python_type(self, literal_type: LiteralType) -> Type[T]:
        raise ValueError(f"Not yet implemented {literal_type}")
