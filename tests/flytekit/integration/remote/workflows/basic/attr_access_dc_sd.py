"""
Test accessing and returning a StructuredDataset attribute from a dataclass instance.
"""
from dataclasses import dataclass

import pandas as pd
from flytekit import task, workflow
from flytekit.types.structured import StructuredDataset


@dataclass
class DC:
    sd: StructuredDataset


@task
def create_dc(uri: str) -> DC:
    """Create a dataclass with a StructuredDataset attribute.

    Args:
        uri: File URI.

    Returns:
        dc: A dataclass with a StructuredDataset attribute.
    """
    dc = DC(sd=StructuredDataset(uri=uri, file_format="parquet"))

    return dc


@task
def read_sd(dc: DC) -> StructuredDataset:
    """Read input StructuredDataset."""
    print("sd:", dc.sd.open(pd.DataFrame).all())

    return dc.sd


@workflow
def wf(uri: str) -> None:
    dc = create_dc(uri=uri)
    read_sd(dc=dc)


if __name__ == "__main__":
    wf(uri="tests/flytekit/integration/remote/workflows/basic/data/df.parquet")
