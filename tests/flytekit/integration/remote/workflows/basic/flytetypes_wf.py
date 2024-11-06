import os
from dataclasses import dataclass
from flytekit.types.file import FlyteFile
from flytekit import task, workflow


@dataclass
class DC:
    ff: FlyteFile

@task
def t1(dc: DC = DC(ff=FlyteFile(os.path.realpath(__file__)))) -> DC:
    with open(dc.ff, "r") as f:
        print("File Content: ", f.read())
    return dc

@workflow
def wf(dc: DC = DC(ff=FlyteFile(os.path.realpath(__file__)) )):
    t1(dc=dc)

if __name__ == "__main__":
    print(f"Running wf() {wf()}")