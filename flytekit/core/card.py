from __future__ import annotations

import typing
from dataclasses import dataclass, field
from enum import Enum


class CardType(Enum):
    UNSET = 0
    MODEL = 1
    DATA = 2


@dataclass
class Card(object):
    text: str
    card_type: CardType = field(default=CardType.UNSET)

    @classmethod
    def from_obj(cls, card_obj: typing.Any, card_type: typing.Optional[CardType]) -> Card:
        return cls(text=str(card_obj), card_type=card_type)


@dataclass
class DataCard(Card):
    card_type: CardType = field(default=CardType.DATA)

    @classmethod
    def from_obj(cls, card_obj: typing.Any, _: typing.Optional[CardType]) -> Card:
        return cls(text=str(card_obj), card_type=CardType.DATA)


@dataclass
class ModelCard(Card):
    card_type: CardType = field(default=CardType.MODEL)

    @classmethod
    def from_obj(cls, card_obj: typing.Any, _: typing.Optional[CardType]) -> Card:
        return cls(text=str(card_obj), card_type=CardType.MODEL)
