try:
    from typing import get_args, get_origin, get_type_hints
except ImportError:
    from typing_extensions import get_origin, get_args, get_type_hints

import inspect

from .a import t1 as a_t1
from .a import t2 as a_t2
from .b import t1 as b_t1
from .b import t2 as b_t2


def printer(fn):
    print(f"In {fn.__module__} FN: {fn.__name__}")
    type_hints = get_type_hints(fn, include_extras=True)
    print(f"  Type hints {type_hints} return type hint")
    signature = inspect.signature(fn)
    print(f"  Inspect signature: {signature}")
    print(f"  Inspect signature return_annotation: {signature.return_annotation}")
    print("")
    return_type = type_hints.get("return", None)
    print(f"  Origin: {get_origin(return_type)}")
    print(f"  Args: {get_args(return_type)}")
    print("===============================\n")


def test_hinting():
    printer(a_t1)
    printer(a_t2)
    printer(b_t1)
    printer(b_t2)
