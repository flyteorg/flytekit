import logging
import os

from pythonjsonlogger import jsonlogger

# Note:
# The environment variable controls exposed to affect the individual loggers should be considered to be beta.
# The ux/api may change in the future.
# At time of writing, the code was written to preserve existing default behavior
# For now, assume this is the environment variable whose usage will remain unchanged and controls output for all
# loggers defined in this file.
LOGGING_ENV_VAR = "FLYTE_SDK_LOGGING_LEVEL"

# By default, the root flytekit logger to debug so everything is logged, but enable fine-tuning
logger = logging.getLogger("flytekit")
# Root logger control
flytekit_root_env_var = f"{LOGGING_ENV_VAR}_ROOT"
if os.getenv(flytekit_root_env_var) is not None:
    logger.setLevel(int(os.getenv(flytekit_root_env_var)))
else:
    logger.setLevel(logging.DEBUG)

# Stop propagation so that configuration is isolated to this file (so that it doesn't matter what the
# global Python root logger is set to).
logger.propagate = False

# Child loggers
child_loggers = {
    "auth": logger.getChild("auth"),
    "cli": logger.getChild("cli"),
    "remote": logger.getChild("remote"),
    "entrypoint": logger.getChild("entrypoint"),
    "user_space": logger.getChild("user_space"),
}
auth_logger = child_loggers["auth"]
cli_logger = child_loggers["cli"]
remote_logger = child_loggers["remote"]
entrypoint_logger = child_loggers["entrypoint"]
user_space_logger = child_loggers["user_space"]

# create console handler
ch = logging.StreamHandler()

# Don't want to import the configuration library since that will cause all sorts of circular imports, let's
# just use the environment variable if it's defined. Decide in the future when we implement better controls
# if we should control with the channel or with the logger level.
# The handler log level controls whether log statements will actually print to the screen
level_from_env = os.getenv(LOGGING_ENV_VAR)
if level_from_env is not None:
    ch.setLevel(int(level_from_env))
else:
    ch.setLevel(logging.WARNING)

for log_name, child_logger in child_loggers.items():
    env_var = f"{LOGGING_ENV_VAR}_{log_name.upper()}"
    level_from_env = os.getenv(env_var)
    if level_from_env is not None:
        child_logger.setLevel(int(level_from_env))

# create formatter
formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")

# add formatter to ch
ch.setFormatter(formatter)

# add ch to logger
logger.addHandler(ch)
