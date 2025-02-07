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
def create_dc() -> DC:
    """Create a dataclass with a StructuredDataset attribute.

    Returns:
        dc: A dataclass with a StructuredDataset attribute.
    """
    dc = DC(sd=StructuredDataset(dataframe=pd.DataFrame({"a": [5]})))

    return dc


@task
def read_sd(dc: DC) -> StructuredDataset:
    """Read input StructuredDataset."""
    print("sd:", dc.sd.open(pd.DataFrame).all())

    """Return a StructuredDataset attribute from a dataclass instance."""
    return dc.sd


@workflow
def wf() -> None:
    dc = create_dc()
    r = read_sd(dc=dc)


if __name__ == "__main__":
    wf()
