from dataclasses import dataclass
from flytekit.types.file import FlyteFile
from flytekit.types.structured.structured_dataset import StructuredDataset
from flytekit.core.type_engine import DataclassTransformer
from typing import Union
import pytest
import re

def test_dataclass_union_with_multiple_flytetypes_error():
    @dataclass
    class DC():
        x: Union[None, StructuredDataset, FlyteFile]


    dc = DC(x="s3://my-bucket/my-file")
    with pytest.raises(ValueError, match=re.escape("Cannot have more than one Flyte type in the Union when attempting to use the string shortcut. Please specify the full object (e.g. FlyteFile(...)) instead of just passing a string.")):
        DataclassTransformer()._make_dataclass_serializable(dc, DC)
