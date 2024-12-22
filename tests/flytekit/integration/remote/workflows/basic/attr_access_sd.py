"""
Access StructuredDataset attribute from a dataclass.
"""
from dataclasses import dataclass, field

import pandas as pd
from flytekit import task, workflow
from flytekit.types.structured import StructuredDataset


S3_URI = "s3://my-s3-bucket/df.parquet"


@dataclass
class DC:
    sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(uri=S3_URI, file_format="parquet"))


@task
def t_sd_attr(sd: StructuredDataset) -> StructuredDataset:
    print("sd:", sd.open(pd.DataFrame).all())

    return sd


@workflow
def wf(dc: DC = DC()) -> None:
    t_sd_attr(sd=dc.sd)


if __name__ == "__main__":
    wf()
