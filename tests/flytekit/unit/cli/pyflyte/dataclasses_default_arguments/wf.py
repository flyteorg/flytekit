from dataclasses import dataclass

from dataclasses_json import dataclass_json

from flytekit import task, workflow


@dataclass_json
@dataclass
class DataclassA:
    a: str
    b: int


@task
def t(dca: DataclassA):
    print(dca)


@workflow
def wf(dca: DataclassA = DataclassA("hello", 42)):
    t(dca=dca)
