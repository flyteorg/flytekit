from __future__ import annotations

from enum import Enum


class CopyFileDetection(Enum):
    LOADED_MODULES = 1
    ALL = 2
    # This option's meaning will change in the future. In the future this will mean that no files should be copied
    # (i.e. no fast registration is used). For now, both this value and setting this Enum to Python None are both
    # valid to distinguish between users explicitly setting --copy none and not setting the flag.
    # Currently, this is only used for register, not for package or run because run doesn't have a no-fast-register
    # option and package is by default non-fast.
    NO_COPY = 3
