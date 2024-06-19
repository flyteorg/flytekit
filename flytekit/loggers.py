import logging
import os
import typing

from pythonjsonlogger import jsonlogger

from .tools import interactive

# Note:
# The environment variable controls exposed to affect the individual loggers should be considered to be beta.
# The ux/api may change in the future.
# At time of writing, the code was written to preserve existing default behavior
# For now, assume this is the environment variable whose usage will remain unchanged and controls output for all
# loggers defined in this file.
LOGGING_ENV_VAR = "FLYTE_SDK_LOGGING_LEVEL"
LOGGING_FMT_ENV_VAR = "FLYTE_SDK_LOGGING_FORMAT"
LOGGING_RICH_FMT_ENV_VAR = "FLYTE_SDK_RICH_TRACEBACKS"

# By default, the root flytekit logger to debug so everything is logged, but enable fine-tuning
logger = logging.getLogger("flytekit")
user_space_logger = logging.getLogger("user_space")

# Stop propagation so that configuration is isolated to this file (so that it doesn't matter what the
# global Python root logger is set to).
logger.propagate = False


def set_flytekit_log_properties(
    handler: typing.Optional[logging.Handler] = None,
    filter: typing.Optional[logging.Filter] = None,
    level: typing.Optional[int] = None,
):
    """
    flytekit logger, refers to the framework logger. It is possible to selectively tune the logging for flytekit.

    Sets the flytekit logger to the specified handler, filter, and level. If any of the parameters are None, then
    the corresponding property on the flytekit logger will not be set.

    :param handler: logging.Handler to add to the flytekit logger
    :param filter: logging.Filter to add to the flytekit logger
    :param level: logging level to set the flytekit logger to
    """
    global logger
    if handler is not None:
        logger.handlers.clear()
        logger.addHandler(handler)
    if filter is not None:
        logger.addFilter(filter)
    if level is not None:
        logger.setLevel(level)


def set_user_logger_properties(
    handler: typing.Optional[logging.Handler] = None,
    filter: typing.Optional[logging.Filter] = None,
    level: typing.Optional[int] = None,
):
    """
    user_space logger, refers to the user's logger. It is possible to selectively tune the logging for the user.

    :param handler: logging.Handler to add to the user_space_logger
    :param filter: logging.Filter to add to the user_space_logger
    :param level: logging level to set the user_space_logger to
    """
    global user_space_logger
    if handler is not None:
        user_space_logger.addHandler(handler)
    if filter is not None:
        user_space_logger.addFilter(filter)
    if level is not None:
        user_space_logger.setLevel(level)


def _get_env_logging_level(default_level: int = logging.WARNING) -> int:
    """
    Returns the logging level set in the environment variable, or logging.WARNING if the environment variable is not
    set.
    """
    return int(os.getenv(LOGGING_ENV_VAR, default_level))


def initialize_global_loggers():
    """
    Initializes the global loggers to the default configuration.
    """
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt="[%(name)s] %(message)s")
    if os.environ.get(LOGGING_FMT_ENV_VAR, "json") == "json":
        formatter = jsonlogger.JsonFormatter(fmt="%(asctime)s %(name)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)

    set_flytekit_log_properties(handler, None, _get_env_logging_level())
    set_user_logger_properties(handler, None, logging.INFO)

    # Use Rich logging while running in the local execution
    if os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", None) is None or interactive.ipython_check():
        upgrade_to_rich_logging()


def is_rich_logging_enabled() -> bool:
    return os.environ.get(LOGGING_RICH_FMT_ENV_VAR) != "0"


def upgrade_to_rich_logging(log_level: typing.Optional[int] = logging.WARNING):
    if not is_rich_logging_enabled():
        return
    try:
        import click
        from rich.console import Console
        from rich.logging import RichHandler

        import flytekit

        handler = RichHandler(
            tracebacks_suppress=[click, flytekit],
            rich_tracebacks=True,
            omit_repeated_times=False,
            log_time_format="%H:%M:%S.%f",
            console=Console(width=os.get_terminal_size().columns),
        )
        formatter = logging.Formatter(fmt="%(message)s")
        handler.setFormatter(formatter)
        set_flytekit_log_properties(handler, None, _get_env_logging_level(default_level=log_level))
        set_user_logger_properties(handler, None, logging.INFO)
    except OSError as e:
        logger.debug(f"Failed to initialize rich logging: {e}")
        pass


def get_level_from_cli_verbosity(verbosity: int) -> int:
    """
    Converts a verbosity level from the CLI to a logging level.

    :param verbosity: verbosity level from the CLI
    :return: logging level
    """
    if verbosity == 0:
        return logging.CRITICAL
    elif verbosity == 1:
        return logging.WARNING
    elif verbosity == 2:
        return logging.INFO
    else:
        return logging.DEBUG


# Default initialization
initialize_global_loggers()
