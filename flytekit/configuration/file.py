from __future__ import annotations

import configparser
import configparser as _configparser
import os
import typing
from dataclasses import dataclass

from flytekit.exceptions import user as _user_exceptions


@dataclass
class ConfigEntry(object):
    """
    Creates a record for the config entry. contains
    Args:
        section: section the option should be found unddd
        option: the option str to lookup
        type_: Expected type of the value
        default_val: stores the default value. Use this only in some circumstances, where you are not using flytekit
                    Config object
    """

    section: str
    option: str
    type_: typing.Type
    default_val: typing.Any = configparser._UNSET

    def get_default(self) -> typing.Any:
        if self.default_val == configparser._UNSET:
            return None
        return self.default_val

    def read_from_env(self) -> typing.Optional[typing.Any]:
        """
        Reads the config entry from environment variable, the structure of the env var is current
        ``FLYTE_{SECTION}_{OPTION}`` all upper cased. We will change this in the future.
        :return:
        """
        env = f"FLYTE_{self.section.upper()}_{self.option.upper()}"
        return os.environ.get(env, None)

    def read_from_file(self, cfg: ConfigFile,
                       transform: typing.Optional[typing.Callable] = None) -> typing.Optional[typing.Any]:
        if not cfg:
            return None
        try:
            v = cfg.get(self)
            return transform(v) if transform else v
        except configparser.Error:
            pass
        return None

    def read(self, cfg: ConfigFile,  transform: typing.Optional[typing.Callable] = None) -> typing.Optional[typing.Any]:
        return self.read_from_env() or self.read_from_file(cfg, transform)


class ConfigFile(object):
    def __init__(self, location: str):
        """
        Load the config from this location
        """
        self._location = location
        self._config = _configparser.ConfigParser()
        self._config.read(self._location)
        if self._config.has_section("internal"):
            raise _user_exceptions.FlyteAssertion(
                "The config file '{}' cannot contain a section for internal "
                "only configurations.".format(self._location)
            )

    def get(self, c: ConfigEntry) -> typing.Any:
        if issubclass(c.type_, int):
            return self._config.getint(c.section, c.option, fallback=c.default_val)

        if issubclass(c.type_, bool):
            return self._config.getboolean(c.section, c.option, fallback=c.default_val)

        if issubclass(c.type_, list):
            v = self._config.get(c.section, c.option, fallback=c.default_val)
            return v.split(",")

        return self._config.get(c.section, c.option, fallback=c.default_val)

    @property
    def config(self) -> _configparser.ConfigParser:
        return self._config


def get_config_file(c: typing.Union[str, ConfigFile]) -> typing.Optional[ConfigFile]:
    """
    Checks if the given argument is a file or a configFile and returns a loaded configFile else returns None
    """
    if c is None:
        return None
    if isinstance(c, str):
        return ConfigFile(c)
    return c


def set_if_exists(d: dict, k: str, v: typing.Any) -> dict:
    """
    Given a dict `d` sets the key `k` with value of config `c`, if the config `c` is set
    It also returns the updated dictionary.

    .. note::

        The input dictionary `d` will be mutated.

    """
    if v:
        d[k] = v
    return d
