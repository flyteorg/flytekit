from __future__ import annotations
"""
There's no difference betweeen this file and a.py except that we have postponed evaluation
of annotations turned on here.
"""
try:
    from typing import Annotated, get_args, get_origin
except ImportError:
    from typing_extensions import Annotated, get_origin, get_args


from .a import AA


my_aa = Annotated[AA, "some annotation"]


def t1() -> AA:
    return AA()


def t2() -> my_aa:
    return my_aa()
