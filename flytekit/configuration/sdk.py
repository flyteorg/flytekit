from flytekit.configuration import common as _config_common

WORKFLOW_PACKAGES = _config_common.FlyteStringListConfigurationEntry("sdk", "workflow_packages", default=[])
"""
This is a comma-delimited list of packages that SDK tools will use to discover entities for the purpose of registration
and execution of entities.
"""

EXECUTION_ENGINE = _config_common.FlyteStringConfigurationEntry("sdk", "execution_engine", default="flyte")
"""
This is a comma-delimited list of package strings, in order, for resolving execution behavior.

TODO: Explain how this would be used to extend the SDK
"""

TYPE_ENGINES = _config_common.FlyteStringListConfigurationEntry("sdk", "type_engines", default=[])
"""
This is a comma-delimited list of package strings, in order, for resolving type behavior.

TODO: Explain how this would be used to extend the SDK
"""

LOCAL_SANDBOX = _config_common.FlyteStringConfigurationEntry("sdk", "local_sandbox", default="/tmp/flyte")
"""
This is the path where SDK will place files during local executions and testing.  The SDK will not automatically
clean up data in these directories.
"""

SDK_PYTHON_VENV = _config_common.FlyteStringListConfigurationEntry("sdk", "python_venv", default=[])
"""
This is a list of commands/args which will be prefixed to the entrypoint command by SDK.
"""

ROLE = _config_common.FlyteStringConfigurationEntry("sdk", "role")
"""
This is the role the SDK will use by default to execute workflows.  For example, in AWS this should be an IAM role
string.
"""

NAME_FORMAT = _config_common.FlyteStringConfigurationEntry("sdk", "name_format", default="{module}.{name}")
"""
This is a Python format string which the SDK will use to generate names for discovered entities.  The default is
'{module}.{name}' which will result in strings like 'package.module.name'.  Any template portion of the string can only
include 'module' or 'name'.  So '{name}' is valid, but '{key}' is not.
"""

TASK_NAME_FORMAT = _config_common.FlyteStringConfigurationEntry("sdk", "task_name_format", fallback=NAME_FORMAT)
"""
This is a Python format string which the SDK will use to generate names for tasks. Any template portion of the
string can only include 'module' or 'name'.  So '{name}' is valid, but '{key}' is not. If not specified,
we fall back to the configuration for :py:attr:`flytekit.configuration.sdk.NAME_FORMAT`
"""

WORKFLOW_NAME_FORMAT = _config_common.FlyteStringConfigurationEntry("sdk", "workflow_name_format", fallback=NAME_FORMAT)
"""
This is a Python format string which the SDK will use to generate names for workflows. Any template portion of the
string can only include 'module' or 'name'.  So '{name}' is valid, but '{key}' is not. If not specified,
we fall back to the configuration for :py:attr:`flytekit.configuration.sdk.NAME_FORMAT`
"""

LAUNCH_PLAN_NAME_FORMAT = _config_common.FlyteStringConfigurationEntry(
    "sdk", "launch_plan_name_format", fallback=NAME_FORMAT
)
"""
This is a Python format string which the SDK will use to generate names for launch plans. Any template portion of the
string can only include 'module' or 'name'.  So '{name}' is valid, but '{key}' is not. If not specified,
we fall back to the configuration for :py:attr:`flytekit.configuration.sdk.NAME_FORMAT`
"""

LOGGING_LEVEL = _config_common.FlyteIntegerConfigurationEntry("sdk", "logging_level", default=20)
"""
This is the default logging level for the Python logging library and will be set before user code runs.
Note that this configuration is special in that it is a runtime setting, not a compile time setting.  This is the only
runtime option in this file.
"""

PARQUET_ENGINE = _config_common.FlyteStringConfigurationEntry("sdk", "parquet_engine", default="pyarrow")
"""
This is the parquet engine to use when reading data from parquet files.
"""

FAST_REGISTRATION_DIR = _config_common.FlyteStringConfigurationEntry("sdk", "fast_registration_dir")
"""
This is the remote directory where fast-registered code will be uploaded to.
Users calling fast-execute need write permission to this directory.
Furthermore, it is important that whichever role executes your workflow has read access to this directory.
"""
