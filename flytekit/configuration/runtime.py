from flytekit.configuration import common as _common_config


# TODO ideally these should be simply lookedup from env vars
# Project, Domain and Version represent the values at registration time.
TASK_PROJECT = _common_config.FlyteStringConfigurationEntry("internal", "task_project", default="")
TASK_DOMAIN = _common_config.FlyteStringConfigurationEntry("internal", "task_domain", default="")
TASK_NAME = _common_config.FlyteStringConfigurationEntry("internal", "task_name", default="")
TASK_VERSION = _common_config.FlyteStringConfigurationEntry("internal", "task_version", default="")

# Execution project and domain represent the values passed by execution engine at runtime.
EXECUTION_PROJECT = _common_config.FlyteStringConfigurationEntry("internal", "execution_project", default="")
EXECUTION_DOMAIN = _common_config.FlyteStringConfigurationEntry("internal", "execution_domain", default="")
EXECUTION_WORKFLOW = _common_config.FlyteStringConfigurationEntry("internal", "execution_workflow", default="")
EXECUTION_LAUNCHPLAN = _common_config.FlyteStringConfigurationEntry("internal", "execution_launchplan", default="")
EXECUTION_NAME = _common_config.FlyteStringConfigurationEntry("internal", "execution_id", default="")