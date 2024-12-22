"""
Access StructuredDataset attribute from a dataclass.
"""
from dataclasses import dataclass, field

import pandas as pd
from flytekit import task, workflow, ImageSpec
from flytekit.types.structured import StructuredDataset


# Build docker image and push to the local registry
flytekit_hash = "4e467b9ecd4172a77965780ea4783acea40cb502"
flytekit = f"git+https://github.com/flyteorg/flytekit.git@{flytekit_hash}"
image = ImageSpec(
    packages=[
        flytekit,
        "pandas",
        "pyarrow"
    ],
    apt_packages=["git"],
    registry="localhost:30000",
)


S3_URI = "s3://my-s3-bucket/df.parquet"


@dataclass
class DC:
    sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(uri=S3_URI, file_format="parquet"))


@task(container_image=image)
def t_sd_attr(sd: StructuredDataset) -> StructuredDataset:
    print("sd:", sd.open(pd.DataFrame).all())

    return sd


@workflow
def wf(dc: DC = DC()) -> None:
    t_sd_attr(sd=dc.sd)


if __name__ == "__main__":
    wf()
