from __future__ import annotations

"""
There's no difference betweeen this file and a.py except that we have postponed evaluation
of annotations turned on here.
"""
try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

from .a import AA

my_aa = Annotated[AA, "some annotation"]


def t1(in1: int) -> AA:
    return AA()


def t2(in1: int) -> my_aa:
    return my_aa()
