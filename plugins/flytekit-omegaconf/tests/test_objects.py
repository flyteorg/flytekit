from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Union


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
