from __future__ import annotations

import typing
from typing import TypeVar, Generic
from typing_extensions import reveal_type, Annotated

if typing.TYPE_CHECKING:
    from flytekit.remote.remote import FlyteRemote

T = TypeVar("T")


class Artifact(Generic[T]):
    def __class_getitem__(cls, user_type: typing.Type[T]) -> typing.Type[T]:
        return Annotated[user_type, 3]

    def __init__(self, s: str):
        ...

    @classmethod
    def get(cls, remote: FlyteRemote):
        ...

    @classmethod
    def resolve(cls):
        ...

    def download(self):
        ...

    @classmethod
    def new(self):
        ...

    @classmethod
    def create_from(cls):
        ...


# Artifact.new(python_val, pytype, schema, metadata, remote, type=Dataset ...)
#
# aa = A[float]("hi")
# reveal_type(aa)
# print(type(aa))
