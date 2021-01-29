"""
Flytekit Directory Type (:mod:`flytekit.types.directory`)
==========================================================
.. currentmodule:: flytekit.types.directory

Similar to :py:class:`flytekit.types.file.FlyteFile` there are some 'preformatted' directory types.

.. autosummary::
   :toctree: generated/

   FlyteDirectory
   TensorboardLogs
"""

import typing

from .types import FlyteDirectory

# The following section provides some predefined aliases for commonly used FlyteDirectory formats.

TensorboardLogs = FlyteDirectory[typing.TypeVar("tensorboard")]
"""
    This type can be used to denote that the output is a folder that contains logs that can be loaded in tensorboard.
    this is usually the SummaryWriter output in pytorch or Keras callbacks which record the history readable by
    tensorboard
"""
