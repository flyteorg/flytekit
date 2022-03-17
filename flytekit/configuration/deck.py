from flytekit.configuration import common as _config_common

# Output deck at the end of the task execution.
ENABLE_DECK = _config_common.FlyteBoolConfigurationEntry("enable", "deck", default=True)
