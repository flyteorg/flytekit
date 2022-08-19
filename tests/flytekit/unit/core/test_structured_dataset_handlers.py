import typing

import pandas as pd
import pyarrow as pa
import pytest

from flytekit.core import context_manager
from flytekit.core.base_task import kwtypes
from flytekit.models.literals import StructuredDatasetMetadata
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured import basic_dfs
from flytekit.types.structured.structured_dataset import (
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
    StructuredDatasetTransformerEngine,
)

my_cols = kwtypes(w=typing.Dict[str, typing.Dict[str, int]], x=typing.List[typing.List[int]], y=int, z=str)

fields = [("some_int", pa.int32()), ("some_string", pa.string())]
arrow_schema = pa.schema(fields)


def test_pandas():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})
    encoder = basic_dfs.PandasToParquetEncodingHandler()
    decoder = basic_dfs.ParquetToPandasDecodingHandler()

    ctx = context_manager.FlyteContextManager.current_context()
    sd = StructuredDataset(dataframe=df)
    sd_type = StructuredDatasetType(format="parquet")
    sd_lit = encoder.encode(ctx, sd, sd_type)

    df2 = decoder.decode(ctx, sd_lit, StructuredDatasetMetadata(sd_type))
    assert df.equals(df2)


def test_base_isnt_instantiable():
    with pytest.raises(TypeError):
        StructuredDatasetEncoder(pd.DataFrame, "", "")

    with pytest.raises(TypeError):
        StructuredDatasetDecoder(pd.DataFrame, "", "")


def test_arrow():
    encoder = basic_dfs.ArrowToParquetEncodingHandler()
    decoder = basic_dfs.ParquetToArrowDecodingHandler()
    assert encoder.protocol is None
    assert decoder.protocol is None
    assert encoder.python_type is decoder.python_type
    d = StructuredDatasetTransformerEngine.DECODERS[encoder.python_type]["s3"]["parquet"]
    assert d is not None
