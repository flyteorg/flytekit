try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated


class AA(object):
    ...


my_aa = Annotated[AA, "some annotation"]


def t1(in1: int) -> AA:
    return AA()


def t2(in1: int) -> my_aa:
    return my_aa()
