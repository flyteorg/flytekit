import pandas as pd
import pytest
from mock import mock

import flytekit
from flytekit import Deck, FlyteContextManager, task
from flytekit.deck import TopFrameRenderer
from flytekit.deck.deck import _output_deck


def test_deck():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    ctx = FlyteContextManager.current_context()
    ctx.user_space_params._decks = [ctx.user_space_params.default_deck]
    renderer = TopFrameRenderer()
    deck_name = "test"
    deck = Deck(deck_name)
    deck.append(renderer.to_html(df))
    assert deck.name == deck_name
    assert deck.html is not None
    assert len(ctx.user_space_params.decks) == 2

    _output_deck("test_task", ctx.user_space_params)


@pytest.mark.parametrize(
    "disable_deck,expected_decks",
    [
        (None, 0),
        (False, 2),  # input and output decks
        (True, 0),
    ],
)
def test_deck_for_task(disable_deck, expected_decks):
    ctx = FlyteContextManager.current_context()

    kwargs = {}
    if disable_deck is not None:
        kwargs["disable_deck"] = disable_deck

    @task(**kwargs)
    def t1(a: int) -> str:
        return str(a)

    t1(a=3)
    assert len(ctx.user_space_params.decks) == expected_decks


@pytest.mark.parametrize(
    "disable_deck, expected_decks",
    [
        (None, 1),
        (False, 1 + 2),  # input and output decks
        (True, 1),
    ],
)
def test_deck_pandas_dataframe(disable_deck, expected_decks):
    ctx = FlyteContextManager.current_context()

    kwargs = {}
    if disable_deck is not None:
        kwargs["disable_deck"] = disable_deck

    @task(**kwargs)
    def t_df(a: str) -> int:
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        flytekit.current_context().default_deck.append(TopFrameRenderer().to_html(df))
        return int(a)

    t_df(a="42")
    assert len(ctx.user_space_params.decks) == expected_decks


@mock.patch("flytekit.deck.deck._ipython_check")
def test_deck_in_jupyter(mock_ipython_check):
    mock_ipython_check.return_value = True

    ctx = FlyteContextManager.current_context()
    ctx.user_space_params._decks = [ctx.user_space_params.default_deck]
    v = ctx.get_deck()
    from IPython.core.display import HTML

    assert isinstance(v, HTML)
    _output_deck("test_task", ctx.user_space_params)

    @task()
    def t1(a: int) -> str:
        return str(a)

    with flytekit.new_context() as ctx:
        t1(a=3)
        deck = ctx.get_deck()
        assert deck is not None
