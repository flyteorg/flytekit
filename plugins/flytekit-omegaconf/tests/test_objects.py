from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Union

from omegaconf import MISSING, OmegaConf


class MultiTypeEnum(str, Enum):
    fifo = "fifo"  # first in first out
    filo = "filo"  # first in last out


@dataclass
class MySubConf:
    my_attr: Optional[Union[int, str]] = 1
    list_attr: List[int] = field(default_factory=list)


@dataclass
class MyConf:
    my_attr: Optional[MySubConf] = None


class SpecialConf(MyConf):
    key: int = 1


TEST_CFG = OmegaConf.create(
    {
        "a": 1,
        "b": 1.0,
        "c": {
            "d": 1,
            "e": MISSING,
            "f": [
                {
                    "g": 2,
                    "h": 1.2,
                },
                {"j": 0.5, "k": "foo", "l": "bar"},
            ],
        },
    }
)
