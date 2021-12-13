import logging
import os

from pythonjsonlogger import jsonlogger

# Always set the root flytekit logger to debug so everything is logged to the logger
logger = logging.getLogger("flytekit")
logger.setLevel(logging.DEBUG)
# Stop propagation so that configuration is isolated to this file (it doesn't matter what the global Python
# root logger is set to).
logger.propagate = False

# Child loggers
auth_logger = logger.getChild("auth")
cli_logger = logger.getChild("cli")
remote_logger = logger.getChild("remote")

# create console handler
ch = logging.StreamHandler()

# Don't want to import the configuration library since that will cause all sorts of circular imports, let's
# just use the environment variable if it's defined. Decide in the future when we implement better controls
# if we should control with the channel or with the logger level.
# The handler log level controls whether log statements will actually print to the screen
logging_env_var = "FLYTE_SDK_LOGGING_LEVEL"
level_from_env = os.getenv(logging_env_var)
if level_from_env is not None:
    ch.setLevel(int(level_from_env))
else:
    ch.setLevel(logging.WARNING)

# create formatter
formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
