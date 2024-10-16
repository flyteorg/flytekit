import asyncio
from dataclasses import dataclass
from typing import List, Optional, TypeVar, Type, Tuple

from typing_extensions import Annotated

from flytekit import task, workflow
from flytekit.configuration import SerializationSettings, ImageConfig, Image
from flytekit.core.context_manager import FlyteContextManager, ExecutionState, FlyteContext
from flytekit.core.dynamic_workflow_task import dynamic
from flytekit.core.type_engine import SimpleTransformer
from flytekit.core.type_engine import TypeEngine, AsyncTypeTransformer, TypeTransformerFailedError
from flytekit.models import types as type_models
from flytekit.models.literals import Literal, LiteralCollection, Primitive, Scalar, LiteralMap
from flytekit.models.types import LiteralType
from flytekit.types.pickle.pickle import FlytePickleTransformer

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
        FlyteContextManager.current_context().with_serialization_settings(ss)
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)):
            dynamic_job_spec = d0.compile_into_workflow(ctx, d0._task_function, ii=foofoo)
            assert len(dynamic_job_spec.nodes) == 3
            for nn in dynamic_job_spec.nodes:
                assert len(nn.inputs) == 1
                assert nn.inputs[0].var == "ii"
                assert nn.inputs[0].binding.scalar.blob is not None

    @workflow
    def wf(ii: ParentList):
        d0(ii=ii)

    wf(ii=foofoo)


def test_cross_type_binding_no_literals_parent():
    @dataclass
    class DataShard:
        # Path to the data shard.
        path: str
        # Checksum of the data shard.
        checksum: str

    pt = FlytePickleTransformer()
    ParentList = list[Annotated[list[Annotated[DataShard, pt]], pt]]

    def gen_foo(batch_size: int) -> ParentList:
        d1 = [DataShard(path="some/path", checksum="some check")] * 30
        return [d1[i:i + batch_size] for i in range(0, len(d1), batch_size)]

    @dynamic
    def d0(ii: ParentList) -> None:
        t0(ii=ii)

    @task
    def t0(ii: ParentList) -> None:
        for shard in ii:
            print(f"Shard: {shard}")

    foofoo = gen_foo(batch_size=10)

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(ss)
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)):
            dynamic_job_spec = d0.compile_into_workflow(ctx, d0._task_function, ii=foofoo)
            assert len(dynamic_job_spec.nodes) == 1
            assert len(dynamic_job_spec.nodes[0].inputs[0].binding.collection.bindings) == 3

    @workflow
    def wf(ii: ParentList):
        d0(ii=ii)

    wf(ii=foofoo)


class MyStr(object):
    def __init__(self, s: str):
        self.s = s
        self.index = None

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self):
        if self.index >= len(self.s):
            raise StopIteration
        value = self.s[self.index]
        self.index += 1
        return value

    def items(self):
        for k, v in enumerate(self.s):
            yield str(k), v


my_str_simple = SimpleTransformer(
    "mystr",
    MyStr,
    type_models.LiteralType(simple=type_models.SimpleType.STRING),
    lambda x: Literal(
        scalar=Scalar(primitive=Primitive(string_value=x.s))
    ),
    lambda x: x.s,
)


class MyStrListTransformer(AsyncTypeTransformer[T]):
    """
    Sample list transformer that should not be written
    """
    def __init__(self):
        super().__init__("Typed List", MyStr)

    @staticmethod
    def get_sub_type_or_none(t: Type[T]) -> Type:
        return str

    def get_literal_type(self, t: Type[T]) -> Optional[LiteralType]:
        sub_type = TypeEngine.to_literal_type(str)
        return type_models.LiteralType(collection_type=sub_type)

    async def async_to_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        if type(python_val) != MyStr:
            raise TypeTransformerFailedError("Expected a string")

        listified = list(python_val.s)
        lit_list = [TypeEngine.async_to_literal(ctx, x, str, expected.collection_type) for x in listified]
        lit_list = await asyncio.gather(*lit_list)

        return Literal(collection=LiteralCollection(literals=lit_list))

    async def async_to_python_value(  # type: ignore
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]
    ) -> Optional[List[T]]:
        lits = lv.collection.literals
        result = [TypeEngine.async_to_python_value(ctx, x, str) for x in lits]
        result = await asyncio.gather(*result)
        result = "".join(result)  # type: ignore
        return MyStr(result)  # type: ignore

    def guess_python_type(self, literal_type: LiteralType) -> list:  # type: ignore
        raise NotImplementedError


def test_sample_transformer():
    ctx = FlyteContext.current_context()
    tt = MyStrListTransformer()
    lt = tt.get_literal_type(MyStr)
    inp = MyStr("hello")
    lit = tt.to_literal(ctx, inp, MyStr, lt)
    final = tt.to_python_value(ctx, lit, MyStr)
    assert final.s == "hello"


def test_sample_transformer_wf():
    TypeEngine.register(my_str_simple)

    @dynamic
    def d0(ii: MyStr) -> None:
        t0(ii=ii)

    @task
    def t0(ii: MyStr) -> None:
        print(f"Letters: {ii}")

    starting = MyStr("hello")

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(ss)
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)):
            dynamic_job_spec = d0.compile_into_workflow(ctx, d0._task_function, ii=starting)
            assert len(dynamic_job_spec.nodes) == 1
            assert len(dynamic_job_spec.nodes[0].inputs) == 1
            bd = dynamic_job_spec.nodes[0].inputs[0].binding
            assert bd.scalar is not None

    del TypeEngine._REGISTRY[MyStr]


def test_sample_transformer_wf_annotated():
    TypeEngine.register(my_str_simple)
    tf = MyStrListTransformer()

    @dynamic
    def d0(ii: MyStr) -> None:
        t0(ii=ii)

    @task
    def t0(ii: Annotated[MyStr, tf]) -> None:
        print(f"Letters: {ii}")

    starting = MyStr("hello")

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(ss)
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)):
            dynamic_job_spec = d0.compile_into_workflow(ctx, d0._task_function, ii=starting)
            assert len(dynamic_job_spec.nodes) == 1
            assert len(dynamic_job_spec.nodes[0].inputs) == 1
            bd = dynamic_job_spec.nodes[0].inputs[0].binding
            assert bd.collection is not None
            assert len(bd.collection.bindings) == 5

    del TypeEngine._REGISTRY[MyStr]


class MyStrDictTransformer(AsyncTypeTransformer[T]):
    """
    Sample transformer that should not be written, turns a string into a dict
    """
    def __init__(self):
        super().__init__("custom str to dict transformer", MyStr)

    @staticmethod
    def extract_types_or_metadata(t: Type[T]) -> Tuple:
        return str, str

    def get_literal_type(self, t: Type[T]) -> Optional[LiteralType]:
        sub_type = TypeEngine.to_literal_type(str)
        return type_models.LiteralType(map_value_type=sub_type)

    async def async_to_literal(
        self, ctx: FlyteContext, python_val: T, python_type: Type[T], expected: LiteralType
    ) -> Literal:
        if type(python_val) != MyStr:
            raise TypeTransformerFailedError("Expected a mystr")

        listified = list(python_val.s)
        lit_list = [TypeEngine.async_to_literal(ctx, x, str, expected.collection_type) for x in listified]
        lit_list = await asyncio.gather(*lit_list)
        lit_dict = {str(idx): v for idx, v in enumerate(lit_list)}

        return Literal(map=LiteralMap(literals=lit_dict))

    async def async_to_python_value(  # type: ignore
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[T]
    ) -> Optional[List[T]]:
        lits = lv.map.literals
        result = {k: asyncio.create_task(TypeEngine.async_to_python_value(ctx, v, str)) for k, v in lits.items()}
        await asyncio.gather(*result.values())

        py_values = {}
        for k, v in result.items():
            py_values[k] = v.result()

        result = [v for v in py_values.values()]  # type: ignore
        result = "".join(result)  # type: ignore
        return MyStr(result)  # type: ignore

    def guess_python_type(self, literal_type: LiteralType) -> list:  # type: ignore
        raise NotImplementedError


def test_sample_dict_transformer_2():
    ctx = FlyteContext.current_context()
    tt = MyStrDictTransformer()
    lt = tt.get_literal_type(MyStr)
    inp = MyStr("hello")
    lit = tt.to_literal(ctx, inp, MyStr, lt)
    final = tt.to_python_value(ctx, lit, MyStr)
    assert final.s == "hello"


def test_sample_transformer_wf_annotated_dict():
    TypeEngine.register(my_str_simple)
    tf = MyStrDictTransformer()

    @dynamic
    def d0(ii: MyStr) -> None:
        t0(ii=ii)

    @task
    def t0(ii: Annotated[MyStr, tf]) -> None:
        print(f"Letters: {ii}")

    starting = MyStr("hello")

    with FlyteContextManager.with_context(
        FlyteContextManager.current_context().with_serialization_settings(ss)
    ) as ctx:
        new_exc_state = ctx.execution_state.with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
        with FlyteContextManager.with_context(ctx.with_execution_state(new_exc_state)):
            dynamic_job_spec = d0.compile_into_workflow(ctx, d0._task_function, ii=starting)
            assert len(dynamic_job_spec.nodes) == 1
            assert len(dynamic_job_spec.nodes[0].inputs) == 1
            bd = dynamic_job_spec.nodes[0].inputs[0].binding
            assert bd.map is not None
            assert len(bd.map.bindings) == 5
            assert bd.map.bindings["0"].scalar is not None
            print(dynamic_job_spec)

    del TypeEngine._REGISTRY[MyStr]
