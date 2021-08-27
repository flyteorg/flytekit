import os

from flytekit.configuration import common as _common_config

# Secrets management
SECRETS_ENV_PREFIX = _common_config.FlyteStringConfigurationEntry("secrets", "env_prefix", default="_FSEC_")
"""
This is the prefix that will be used to lookup for injected secrets at runtime. This can be overridden to using
FLYTE_SECRETS_ENV_PREFIX variable
"""

SECRETS_DEFAULT_DIR = _common_config.FlyteStringConfigurationEntry(
    "secrets", "default_dir", default=os.path.join(os.sep, "etc", "secrets")
)
"""
This is the default directory that will be used to find secrets as individual files under. This can be overridden using
FLYTE_SECRETS_DEFAULT_DIR.
"""

SECRETS_FILE_PREFIX = _common_config.FlyteStringConfigurationEntry("secrets", "file_prefix", default="")
"""
This is the prefix for the file in the default dir.
"""
