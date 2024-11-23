import typing
import os
from dataclasses import dataclass, fields, field
from typing import Dict, List
from flytekit.types.file import FlyteFile
from flytekit.types.structured import StructuredDataset
from flytekit.types.directory import FlyteDirectory
from flytekit import task, workflow, ImageSpec
import datetime
from enum import Enum
import pandas as pd

@dataclass
class DC:
    ff: FlyteFile
    sd: StructuredDataset
    fd: FlyteDirectory


@task
def t1(dc: DC = DC(ff=FlyteFile(os.path.realpath(__file__)),
                   sd=StructuredDataset(
                       uri="tests/flytekit/integration/remote/workflows/basic/data/df.parquet",
                       file_format="parquet"),
                    fd=FlyteDirectory("tests/flytekit/integration/remote/workflows/basic/data/")
                   )):

    with open(dc.ff, "r") as f:
        print("File Content: ", f.read())

    print("sd:", dc.sd.open(pd.DataFrame).all())

    df_path = os.path.join(dc.fd.path, "df.parquet")
    print("fd: ", os.path.isdir(df_path))

    return dc

@workflow
def wf(dc: DC = DC(ff=FlyteFile(os.path.realpath(__file__)),
                   sd=StructuredDataset(
                       uri="tests/flytekit/integration/remote/workflows/basic/data/df.parquet",
                       file_format="parquet"),
                    fd=FlyteDirectory("tests/flytekit/integration/remote/workflows/basic/data/")
                   )):
    t1(dc=dc)

if __name__ == "__main__":
    wf()
