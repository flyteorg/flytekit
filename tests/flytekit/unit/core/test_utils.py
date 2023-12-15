import pytest

import flytekit
from flytekit import FlyteContextManager, task
from flytekit.core.utils import _dnsify, timeit, ClassDecorator


@pytest.mark.parametrize(
    "input,expected",
    [
        ("test.abc", "test-abc"),
        ("test", "test"),
        ("", ""),
        (".test", "test"),
        ("Test", "test"),
        ("test.", "test"),
        ("test-", "test"),
        ("test$", "test"),
        ("te$t$", "tet"),
        ("t" * 64, f"da4b348ebe-{'t'*52}"),
    ],
)
def test_dnsify(input, expected):
    assert _dnsify(input) == expected


def test_timeit():
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params._decks = []

    with timeit("Set disable_deck to False"):
        kwargs = {}
        kwargs["disable_deck"] = False

    ctx = FlyteContextManager.current_context()
    time_info_list = ctx.user_space_params.timeline_deck.time_info
    names = [time_info["Name"] for time_info in time_info_list]
    # check if timeit works for flytekit level code
    assert "Set disable_deck to False" in names

    @task(**kwargs)
    def t1() -> int:
        @timeit("Download data")
        def download_data():
            return "1"

        data = download_data()

        with timeit("Convert string to int"):
            return int(data)

    t1()

    time_info_list = flytekit.current_context().timeline_deck.time_info
    names = [time_info["Name"] for time_info in time_info_list]

    # check if timeit works for user level code
    assert "Download data" in names
    assert "Convert string to int" in names


def test_class_decorator():
    class MyDecorator(ClassDecorator):
        def __init__(self, func=None, *, foo="bar"):
            self.func = func
            self.foo = foo
            super().__init__(func, foo=foo)

        def _wrap_call(self, *args, **kwargs):
            return self.func(*args, **kwargs)

        def get_extra_config(self):
            return {"foo": self.foo}

    @MyDecorator(foo="baz")
    def t():
        return "hello world"

    assert t() == "hello world"
    assert t.get_extra_config() == {"foo": "baz"}
