import mock

import pytest
import typing
import asyncio
import datetime
from flytekit.core.context_manager import FlyteContext, FlyteContextManager

from flytekit.core.type_engine import (
    AsyncTypeTransformer,
    TypeEngine,
)
from flytekit.models.literals import (
    Literal,
    Primitive,
    Scalar,
)
from flytekit.models.types import LiteralType, SimpleType

T = typing.TypeVar("T")


class MyInt:
    def __init__(self, x: int):
        self.val = x

    def __eq__(self, other):
        if not isinstance(other, MyInt):
            return False
        return other.val == self.val


class MyIntAsyncTransformer(AsyncTypeTransformer[MyInt]):
    def __init__(self):
        super().__init__(name="MyAsyncInt", t=MyInt)
        self.my_lock = asyncio.Lock()
        self.my_count = 0

    def assert_type(self, t, v):
        return

    def get_literal_type(self, t: typing.Type[MyInt]) -> LiteralType:
        return LiteralType(simple=SimpleType.INTEGER)

    async def async_to_literal(
        self,
        ctx: FlyteContext,
        python_val: MyInt,
        python_type: typing.Type[MyInt],
        expected: LiteralType,
    ) -> Literal:
        print(f"start waiting on {python_val.val} {datetime.datetime.now()}")
        async with self.my_lock:
            self.my_count += 1
            if self.my_count > 2:
                raise Exception("This should not happen")
        await asyncio.sleep(0.1)
        print(f"done waiting on {python_val.val} {datetime.datetime.now()}")
        lit = Literal(scalar=Scalar(primitive=Primitive(integer=python_val.val)))

        async with self.my_lock:
            self.my_count -= 1

        return lit

    async def async_to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: typing.Type[MyInt]
    ) -> MyInt:
        return MyInt(lv.scalar.primitive.integer)

    def guess_python_type(self, literal_type: LiteralType) -> typing.Type[MyInt]:
        return MyInt


def test_file_formats_getting_literal_type():
    TypeEngine.register(MyIntAsyncTransformer())

    lt = LiteralType(simple=SimpleType.INTEGER)
    python_val = [MyInt(10), MyInt(11), MyInt(12), MyInt(13), MyInt(14)]
    ctx = FlyteContext.current_context()

    with mock.patch("flytekit.core.type_engine._TYPE_ENGINE_COROS_BATCH_SIZE", 2):
        TypeEngine.to_literal(ctx, python_val, typing.List[MyInt], lt)

    with mock.patch("flytekit.core.type_engine._TYPE_ENGINE_COROS_BATCH_SIZE", 5):
        with pytest.raises(Exception):
            TypeEngine.to_literal(ctx, python_val, typing.List[MyInt], lt)

    del TypeEngine._REGISTRY[MyInt]
