"""Pandera validation configuration."""

from dataclasses import dataclass
from typing import Literal, Optional


@dataclass
class PanderaValidationConfig:
    on_error: Literal["raise", "report"] = "report"
    head: Optional[int] = None
    tail: Optional[int] = None
    sample: Optional[int] = None
    random_state: Optional[int] = None
    lazy: bool = True
