from flytekit import logger
from flytekit.loggers import cli_logger

import logging

logging.basicConfig()


logger.debug("This is a debug line")
logger.info("This is an info line")
logger.warning("This is a warning line")



cli_logger.debug("CLI logger line is a debug line")
cli_logger.info("CLI logger line is an info line")
cli_logger.warning("CLI logger line is a warning line")

