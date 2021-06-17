import logging as _logging
import os as _os

from pythonjsonlogger import jsonlogger

logger = _logging.getLogger("flytekit")
# Always set the root logger to debug until we can add more user based controls
logger.setLevel(_logging.DEBUG)

# Child loggers
auth_logger = logger.getChild("auth")
cli_logger = logger.getChild("cli")

# create console handler and set level to debug
ch = _logging.StreamHandler()

# Don't want to import the configuration library since that will cause all sorts of circular imports, let's
# just use the environment variable if it's defined. Decide in the future when we implement better controls
# if we should control with the channel or with the logger level.
logging_env_var = "FLYTE_SDK_LOGGING_LEVEL"
level_from_env = _os.getenv(logging_env_var)
if level_from_env is not None:
    ch.setLevel(int(level_from_env))
else:
    ch.setLevel(_logging.DEBUG)

# create formatter
formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
