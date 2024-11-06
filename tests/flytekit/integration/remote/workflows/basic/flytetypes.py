
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


@task
def t1(dc: DC = DC(ff=FlyteFile(os.path.realpath(__file__)))):
    with open(dc.ff, "r") as f:
        print("File Content: ", f.read())

@workflow
def wf(dc: DC = DC(ff=FlyteFile(os.path.realpath(__file__)))):
    t1(dc=dc)

if __name__ == "__main__":
    wf()
