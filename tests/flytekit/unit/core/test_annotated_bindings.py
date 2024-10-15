import asyncio
import pytest
from enum import Enum
from dataclasses_json import DataClassJsonMixin
from mashumaro.mixins.json import DataClassJSONMixin
import os
import sys
import tempfile
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union, TypeVar, Type
from flytekit.models.types import LiteralType, SimpleType, TypeStructure, UnionType
from typing_extensions import Annotated
from flytekit.types.pickle.pickle import FlytePickleTransformer
from flytekit.types.schema import FlyteSchema
from flytekit.core.type_engine import TypeEngine
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.context_manager import FlyteContextManager, ExecutionState, FlyteContext
from flytekit.core.type_engine import DataclassTransformer
from flytekit import task, workflow
from flytekit.types.directory import FlyteDirectory
from flytekit.types.file import FlyteFile
from flytekit.models.literals import Binary, Literal, LiteralCollection, LiteralMap, Primitive, Scalar, Union, Void
from flytekit.types.structured import StructuredDataset
from flytekit.configuration import SerializationSettings, ImageConfig, Image
from flytekit.core.type_engine import TypeEngine, AsyncTypeTransformer, TypeTransformerFailedError

ss = SerializationSettings(
    project="test_proj",
    domain="test_domain",
    version="abc",
    image_config=ImageConfig(Image(name="name", fqn="image", tag="name")),
    env={},
)

T = TypeVar("T")


def test_cross_type_binding_no_literals():
    @dataclass
    class DataShard:
        # Path to the data shard.
        path: str
        # Checksum of the data shard.
        checksum: str

    PackagedList = Annotated[list[DataShard], FlytePickleTransformer()]
    ParentList = list[PackagedList]

    def gen_foo(batch_size: int) -> ParentList:
        d1 = [DataShard(path="some/path", checksum="some check")] * 30
        return [d1[i:i + batch_size] for i in range(0, len(d1), batch_size)]

    @dynamic
    def d0(ii: ParentList) -> None:
        for idx, l in enumerate(ii):
            print(f"List {idx}: {l}")
            t0(ii=ii)

    @task
    def t0(ii: PackagedList) -> None:
        for shard in ii:
            print(f"Shard: {shard}")

    foofoo = gen_foo(batch_size=10)

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(ss        )
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)):
            dynamic_job_spec = d0.compile_into_workflow(ctx, d0._task_function, ii=foofoo)
            for xx in dynamic_job_spec.nodes:
                print(xx)


class ListTransformer(AsyncTypeTransformer[T]):
    """
    Transformer that handles a univariate typing.List[T]
    """

    def __init__(self):
        super().__init__("Typed List", list)

    @staticmethod
    def get_sub_type(t: Type[T]) -> Type[T]:
        """
        Return the generic Type T of the List
        """
        if (sub_type := ListTransformer.get_sub_type_or_none(t)) is not None:
            return sub_type

        raise ValueError("Only generic univariate typing.List[T] type is supported.")

    @staticmethod
    def get_sub_type_or_none(t: Type[T]) -> Optional[Type[T]]:
        """
        Return the generic Type T of the List, or None if the generic type cannot be inferred
        """
        if hasattr(t, "__origin__"):
            # Handle annotation on list generic, eg:
            # Annotated[typing.List[int], 'foo']
            if is_annotated(t):
                return ListTransformer.get_sub_type(get_args(t)[0])

            if getattr(t, "__origin__") is list and hasattr(t, "__args__"):
                return getattr(t, "__args__")[0]

        return None

    def get_literal_type(self, t: Type[T]) -> Optional[LiteralType]:
        """
        Only univariate Lists are supported in Flyte
        """
        try:
            sub_type = TypeEngine.to_literal_type(self.get_sub_type(t))
            return _type_models.LiteralType(collection_type=sub_type)
        except Exception as e:
            raise ValueError(f"Type of Generic List type is not supported, {e}")

    @staticmethod
    def is_batchable(t: Type):
        """
        This function evaluates whether the provided type is batchable or not.
        It returns True only if the type is either List or Annotated(List) and the List subtype is FlytePickle.
        """
        from flytekit.types.pickle import FlytePickle

        if is_annotated(t):
            return ListTransformer.is_batchable(get_args(t)[0])
        if get_origin(t) is list:
            subtype = get_args(t)[0]
            if subtype == FlytePickle or (hasattr(subtype, "__origin__") and subtype.__origin__ == FlytePickle):
                return True
        return False

    async def async_to_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        if type(python_val) != list:
            raise TypeTransformerFailedError("Expected a list")

        if ListTransformer.is_batchable(python_type):
            from flytekit.types.pickle.pickle import BatchSize, FlytePickle

            batch_size = len(python_val)  # default batch size
            # parse annotated to get the number of items saved in a pickle file.
            if is_annotated(python_type):
                for annotation in get_args(python_type)[1:]:
                    if isinstance(annotation, BatchSize):
                        batch_size = annotation.val
                        break
            if batch_size > 0:
                lit_list = [
                    TypeEngine.to_literal(ctx, python_val[i : i + batch_size], FlytePickle, expected.collection_type)
                    for i in range(0, len(python_val), batch_size)
                ]  # type: ignore
            else:
                lit_list = []
        else:
            t = self.get_sub_type(python_type)
            lit_list = [TypeEngine.async_to_literal(ctx, x, t, expected.collection_type) for x in python_val]
            lit_list = await asyncio.gather(*lit_list)

        return Literal(collection=LiteralCollection(literals=lit_list))

    async def async_to_python_value(  # type: ignore
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]
    ) -> Optional[List[T]]:
        if lv and lv.scalar and lv.scalar.binary is not None:
            return self.from_binary_idl(lv.scalar.binary, expected_python_type)  # type: ignore

        try:
            lits = lv.collection.literals
        except AttributeError:
            raise TypeTransformerFailedError(
                (
                    f"The expected python type is '{expected_python_type}' but the received Flyte literal value "
                    f"is not a collection (Flyte's representation of Python lists)."
                )
            )
        st = self.get_sub_type(expected_python_type)
        result = [TypeEngine.async_to_python_value(ctx, x, st) for x in lits]
        result = await asyncio.gather(*result)
        return result  # type: ignore  # should be a list, thinks its a tuple

    def guess_python_type(self, literal_type: LiteralType) -> list:  # type: ignore
        if literal_type.collection_type:
            ct: Type = TypeEngine.guess_python_type(literal_type.collection_type)
            return typing.List[ct]  # type: ignore
        raise ValueError(f"List transformer cannot reverse {literal_type}")


def test_string_to_list():
    ...




def test_list_to_dict():
    ...

