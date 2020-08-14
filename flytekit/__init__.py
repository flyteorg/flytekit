from __future__ import absolute_import

import flytekit.plugins  # noqa: F401

__version__ = "0.13.0b0"

import logging as _logging

logger = _logging.getLogger("flytekit")

# create console handler and set level to debug
ch = _logging.StreamHandler()
ch.setLevel(_logging.DEBUG)

# create formatter
formatter = _logging.Formatter("%(asctime)s-%(name)s-%(levelname)s$ %(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


class FlyteWorkflowMeta(type):
    """
    This class should be used as the metaclass for all Flyte workflow classes. It is here currently as a placeholder,
    to future-proof design changes, in case we need to better __prepare__ classes in the future or otherwise change
    how workflow classes are constructed.
    """
