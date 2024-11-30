"""
Access StructuredDataset attribute from a dataclass.
"""
from dataclasses import dataclass, field

import pandas as pd
from flytekit import task, workflow, ImageSpec
from flytekit.types.structured import StructuredDataset


flytekit_hash = "adc1061709b2cff74c2e66dd65399d6a59954023"
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



@dataclass
class DC:
    sd: StructuredDataset = field(default_factory=lambda: StructuredDataset(
            uri="s3://my-s3-bucket/s3_flyte_dir/df.parquet",
            file_format="parquet"
        )
    )


@task(container_image=image)
def t_sd_attr(sd: StructuredDataset) -> StructuredDataset:
    print(sd.open(pd.DataFrame).all())
    return sd


@task(container_image=image)
def t_sd_attr_2(sd: StructuredDataset) -> StructuredDataset:
    print("sd:", sd.open(pd.DataFrame).all())
    return sd


@workflow
def wf(dc: DC):
    sd_2 = t_sd_attr(sd=dc.sd)
    t_sd_attr_2(sd=sd_2)


if __name__ == "__main__":
    from flytekit.clis.sdk_in_container import pyflyte
    from click.testing import CliRunner
    import os

    input_val = '{"dc": {"sd": {"uri": "s3://my-s3-bucket/s3_flyte_dir/df.parquet", "file_format": "parquet"}}}'
    runner = CliRunner()
    path = os.path.realpath(__file__)
    result = runner.invoke(pyflyte.main, ["run", path, "wf", "--dc", input_val])
    print(result.output)
