import dataclasses
import datetime
import functools
import os
import random
import typing
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import pytest
from dataclasses_json import dataclass_json
from google.protobuf.struct_pb2 import Struct

from flytekit.models.literals import Literal, Scalar, StructuredDatasetMetadata
from flytekit.models.types import LiteralType, StructuredDatasetType, SimpleType

from flytekit import ContainerTask, Secret, SQLTask, dynamic, kwtypes, map_task
from flytekit.common.translator import get_serializable
from flytekit.core import context_manager, launch_plan, promise
from flytekit.core.condition import conditional
from flytekit.core.context_manager import ExecutionState, FastSerializationSettings, Image, ImageConfig
from flytekit.core.data_persistence import FileAccessProvider
from flytekit.core.node import Node
from flytekit.core.promise import NodeOutput, Promise, VoidPromise
from flytekit.core.resources import Resources
from flytekit.core.task import TaskMetadata, task
from flytekit.core.testing import patch, task_mock
from flytekit.core.type_engine import RestrictedTypeError, TypeEngine
from flytekit.core.workflow import workflow
from flytekit.models import literals as _literal_models
from flytekit.models.core import types as _core_types
from flytekit.models.interface import Parameter
from flytekit.models.task import Resources as _resource_models
from flytekit.models.types import LiteralType, SimpleType
from flytekit.types.directory import FlyteDirectory, TensorboardLogs
from flytekit.types.file import FlyteFile, PNGImageFile
from flytekit.types.schema import FlyteSchema, SchemaOpenMode
import typing

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.dataframe
from pyspark.sql import SparkSession

from flytekit import FlyteContext, kwtypes, task, workflow
from flytekit.models import literals
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.types.structured.structured_dataset import (
    DF,
    FLYTE_DATASET_TRANSFORMER,
    PARQUET,
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)
from flytekit.types.structured.utils import get_filesystem


PANDAS_PATH = "/tmp/pandas.pq"
NUMPY_PATH = "/tmp/numpy.pq"
BQ_PATH = "bq://photo-313016:flyte.new_table3"

my_cols = kwtypes(w=typing.Dict[str, typing.Dict[str, int]], x=typing.List[typing.List[int]], y=int, z=str)

fields = [("some_int", pa.int32()), ("some_string", pa.string())]
arrow_schema = pa.schema(fields)

serialization_settings = context_manager.SerializationSettings(
    project="proj",
    domain="dom",
    version="123",
    image_config=ImageConfig(Image(name="name", fqn="asdf/fdsa", tag="123")),
    env={},
)


def test_types_pandas():
    pt = pd.DataFrame
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type is not None
    assert lt.structured_dataset_type.format == "parquet"
    assert lt.structured_dataset_type.columns == []


def test_types_annotated():
    pt = Annotated[pd.DataFrame, my_cols]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 4
    assert lt.structured_dataset_type.columns[0].literal_type.map_value_type.map_value_type.simple == SimpleType.INTEGER
    assert lt.structured_dataset_type.columns[1].literal_type.collection_type.collection_type.simple == SimpleType.INTEGER
    assert lt.structured_dataset_type.columns[2].literal_type.simple == SimpleType.INTEGER
    assert lt.structured_dataset_type.columns[3].literal_type.simple == SimpleType.STRING

    pt = Annotated[pd.DataFrame, arrow_schema]
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type.external_schema_type == "arrow"
    assert "some_string" in str(lt.structured_dataset_type.external_schema_bytes)


def test_types_sd():
    pt = StructuredDataset
    lt = TypeEngine.to_literal_type(pt)
    assert lt.structured_dataset_type is not None

    pt = StructuredDataset[my_cols]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 4



