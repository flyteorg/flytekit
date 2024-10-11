"""Pandera validation configuration."""

from dataclasses import dataclass
from typing import Literal


@dataclass
class PanderaValidationConfig:
    # determine how to handle validation errors in the Flyte type transformer
    on_error: Literal["raise", "warn"] = "raise"
