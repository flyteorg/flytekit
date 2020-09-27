import flytekit.plugins

import logging as _logging
from flytekit.annotated.context_manager import FlyteContext

__version__ = '0.11.3'

logger = _logging.getLogger('flytekit')

# create console handler and set level to debug
ch = _logging.StreamHandler()
ch.setLevel(_logging.DEBUG)

# create formatter
formatter = _logging.Formatter('%(asctime)s-%(name)s-%(levelname)s$ %(message)s')

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)


def current_context():
    return FlyteContext.current_context().user_space_params
