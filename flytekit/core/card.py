from __future__ import annotations

import typing


class Card(object):
    def __init__(self, text: str):
        self.text = text

    @classmethod
    def from_obj(cls, card_obj: typing.Any) -> Card:
        return cls(str(card_obj))
