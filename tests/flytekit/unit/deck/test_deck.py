import pandas as pd

from flytekit import Deck, FlyteContextManager
from flytekit.deck import FrameRenderer
from flytekit.deck.deck import _output_deck


def test_deck():
    df = pd.DataFrame({"Name": ["Tom", "Joseph"], "Age": [1, 22]})
    ctx = FlyteContextManager.current_context()
    renderer = FrameRenderer(df)
    deck_name = "test"
    deck = Deck(deck_name, renderer)
    assert deck.name == deck_name
    assert deck.renderers == [renderer]
    assert len(ctx.user_space_params.decks) == 2

    task_input = {"x": 2}
    task_output = {"y": "hello"}
    _output_deck("test_task", ctx.user_space_params, task_input, task_output)
