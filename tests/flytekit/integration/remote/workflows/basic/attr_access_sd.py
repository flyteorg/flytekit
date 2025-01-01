"""
Access StructuredDataset attribute from a dataclass.
"""
from dataclasses import dataclass

import pandas as pd
from flytekit import task, workflow
from flytekit.types.structured import StructuredDataset


URI = "tests/flytekit/integration/remote/workflows/basic/data/df.parquet"


@dataclass
class DC:
    sd: StructuredDataset


@task
def build_dc(uri: str) -> DC:
    dc = DC(sd=StructuredDataset(uri=uri, file_format="parquet"))

    return dc


@task
def t_sd_attr(sd: StructuredDataset) -> StructuredDataset:
    print("sd:", sd.open(pd.DataFrame).all())

    return sd


@workflow
def wf(uri: str) -> None:
    dc = build_dc(uri=uri)
    t_sd_attr(sd=dc.sd)


if __name__ == "__main__":
    wf(uri=URI)
