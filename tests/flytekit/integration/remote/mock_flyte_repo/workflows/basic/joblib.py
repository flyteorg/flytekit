"""Test joblib file."""

import typing
from pathlib import Path

import joblib

from flytekit import task, workflow
from flytekit.types.file import JoblibSerializedFile


@task
def joblib_task(obj: typing.List[int], filename: str) -> JoblibSerializedFile:
    Path(filename).parent.mkdir(parents=True)
    joblib.dump(obj, filename)
    return JoblibSerializedFile(path=filename)


@workflow
def joblib_workflow(obj: typing.List[int], filename: str) -> JoblibSerializedFile:
    return joblib_task(obj=obj, filename=filename)
