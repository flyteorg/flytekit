from flytekit.core import card


def test_card():
    c = card.Card(text="hello")
    assert c.card_type == card.CardType.UNSET


def test_model_card():
    c = card.ModelCard(text="hello")
    assert c.card_type == card.CardType.MODEL
    c = card.Card(text="hello", card_type=card.CardType.MODEL)
    assert c.card_type == card.CardType.MODEL


def test_data_card():
    c = card.DataCard(text="hello")
    assert c.card_type == card.CardType.DATA
