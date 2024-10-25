from dataclasses import dataclass
from flytekit.types.file import FlyteFile
from flytekit.types.structured.structured_dataset import StructuredDataset
from flytekit.core.type_engine import DataclassTransformer
from typing import Union
import pytest

def test_dataclass_union_with_multiple_flytetypes_error():
    @dataclass
    class DC():
        x: Union[None, FlyteFile, StructuredDataset]


    dc = DC(x="s3://my-bucket/my-file")
    with pytest.raises(ValueError, match="Cannot have two Flyte types in a Union type"):
        DataclassTransformer()._make_dataclass_serializable(dc, DC)

