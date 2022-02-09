import typing

import pandas as pd
import pyarrow as pa
import pytest

from flytekit.core import context_manager
from flytekit.core.base_task import kwtypes
from flytekit.models.types import StructuredDatasetType
from flytekit.types.structured import basic_dfs
from flytekit.types.structured.structured_dataset import (
    StructuredDataset,
    StructuredDatasetDecoder,
    StructuredDatasetEncoder,
)

my_cols = kwtypes(w=typing.Dict[str, typing.Dict[str, int]], x=typing.List[typing.List[int]], y=int, z=str)

fields = [("some_int", pa.int32()), ("some_string", pa.string())]
arrow_schema = pa.schema(fields)


def test_pandas():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [20, 22]})
    encoder = basic_dfs.PandasToParquetEncodingHandler("/")
    decoder = basic_dfs.ParquetToPandasDecodingHandler("/")

    ctx = context_manager.FlyteContextManager.current_context()
    sd = StructuredDataset(
        dataframe=df,
    )
    sd_lit = encoder.encode(ctx, sd, StructuredDatasetType(format="parquet"))

    df2 = decoder.decode(ctx, sd_lit)
    assert df.equals(df2)


def test_base_isnt_instantiable():
    with pytest.raises(TypeError):
        StructuredDatasetEncoder(pd.DataFrame, "", "")

    with pytest.raises(TypeError):
        StructuredDatasetDecoder(pd.DataFrame, "", "")
