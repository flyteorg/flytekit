from flytekit.configuration import common as _common_config

HOST = _common_config.FlyteStringConfigurationEntry("statsd", "host", default="localhost")
PORT = _common_config.FlyteIntegerConfigurationEntry("statsd", "port", default=8125)
DISABLED = _common_config.FlyteBoolConfigurationEntry("statsd", "disabled", default=False)
