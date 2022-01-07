import typing

import pytest

from flytekit.core import context_manager
from flytekit.core.context_manager import FlyteContext, FlyteContextManager, Image, ImageConfig
from flytekit.core.type_engine import TypeEngine
from flytekit.models import literals
from flytekit.models.types import SimpleType, StructuredDatasetType

try:
    from typing import Annotated, TypeAlias
except ImportError:
    from typing_extensions import Annotated, TypeAlias

import pandas as pd
import pyarrow as pa

from flytekit import kwtypes
from flytekit.types.structured.structured_dataset import (
    FLYTE_DATASET_TRANSFORMER,
    StructuredDataset,
    StructuredDatasetEncoder,
    protocol_prefix,
)

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


def test_protocol():
    assert protocol_prefix("s3://my-s3-bucket/file") == "s3"
    assert protocol_prefix("/file") == "/"


def generate_pandas() -> pd.DataFrame:
    return pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})


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
    assert (
        lt.structured_dataset_type.columns[1].literal_type.collection_type.collection_type.simple == SimpleType.INTEGER
    )
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

    pt = StructuredDataset[my_cols, "csv"]
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 4
    assert lt.structured_dataset_type.format == "csv"

    pt = StructuredDataset[{}, "csv"]
    assert pt.FILE_FORMAT == "csv"
    lt = TypeEngine.to_literal_type(pt)
    assert len(lt.structured_dataset_type.columns) == 0
    assert lt.structured_dataset_type.format == "csv"


def test_retrieving():
    assert FLYTE_DATASET_TRANSFORMER.get_encoder(pd.DataFrame, "/", "parquet") is not None
    with pytest.raises(ValueError):
        # We don't have a default "" format encoder
        FLYTE_DATASET_TRANSFORMER.get_encoder(pd.DataFrame, "/", "")

    e = FLYTE_DATASET_TRANSFORMER.get_encoder(pd.DataFrame, "s3", "parquet")
    e2 = FLYTE_DATASET_TRANSFORMER.get_encoder(pd.DataFrame, "s3://", "parquet")
    assert e is not None
    assert e is e2


def test_to_literal():
    ctx = FlyteContextManager.current_context()
    lt = TypeEngine.to_literal_type(pd.DataFrame)
    df = generate_pandas()

    lit = FLYTE_DATASET_TRANSFORMER.to_literal(ctx, df, python_type=pd.DataFrame, expected=lt)
    assert lit.scalar.structured_dataset.metadata.structured_dataset_type.format == "parquet"
    assert lit.scalar.structured_dataset.metadata.structured_dataset_type.format == "parquet"


MyDF: TypeAlias = pd.DataFrame


def test_fill_in():
    class TempEncoder(StructuredDatasetEncoder):
        def __init__(self):
            super().__init__(MyDF, "/")

        def encode(
            self,
            ctx: FlyteContext,
            structured_dataset: StructuredDataset,
            structured_dataset_type: StructuredDatasetType,
        ) -> literals.StructuredDataset:
            return literals.StructuredDataset(
                uri="", metadata=literals.StructuredDatasetMetadata(structured_dataset_type)
            )

    FLYTE_DATASET_TRANSFORMER.register_handler(TempEncoder())
