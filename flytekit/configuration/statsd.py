from flytekit.configuration import common as _common_config

# StatsD Config flags should ideally be controlled at the platform level and not through flytekit's config file.
# They are meant to allow administrators to control certain behavior according to how the system is configured.

HOST = _common_config.FlyteStringConfigurationEntry("statsd", "host", default="localhost")
PORT = _common_config.FlyteIntegerConfigurationEntry("statsd", "port", default=8125)
DISABLED = _common_config.FlyteBoolConfigurationEntry("statsd", "disabled", default=False)
DISABLE_TAGS = _common_config.FlyteBoolConfigurationEntry("statsd", "disable_tags", default=False)
