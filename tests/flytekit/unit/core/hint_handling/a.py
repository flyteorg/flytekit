try:
    from typing import Annotated, get_args, get_origin
except ImportError:
    from typing_extensions import Annotated, get_origin, get_args


class AA(object):
    ...


my_aa = Annotated[AA, "some annotation"]


def t1(in1: int) -> AA:
    return AA()


def t2(in1: int) -> my_aa:
    return my_aa()
