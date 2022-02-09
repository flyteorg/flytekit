import tempfile

from flytekit.configuration import common as _config_common

WORKFLOW_PACKAGES = _config_common.FlyteStringListConfigurationEntry("sdk", "workflow_packages", default=[])
"""
This is a comma-delimited list of packages that SDK tools will use to discover entities for the purpose of registration
and execution of entities.
"""

LOCAL_SANDBOX = _config_common.FlyteStringConfigurationEntry(
    "sdk",
    "local_sandbox",
    default=tempfile.mkdtemp(prefix="flyte"),
)
"""
This is the path where SDK will place files during local executions and testing.  The SDK will not automatically
clean up data in these directories.
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

# Feature Gate
USE_STRUCTURED_DATASET = _config_common.FlyteBoolConfigurationEntry("sdk", "use_structured_dataset", default=False)
"""
Note: This gate will be switched to True at some point in the future. Definitely by 1.0, if not v0.31.0.

"""
