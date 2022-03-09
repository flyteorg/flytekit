from __future__ import annotations

import configparser
import configparser as _configparser
import os
import typing
from dataclasses import dataclass
from pathlib import Path

from flytekit.exceptions import user as _user_exceptions
from flytekit.loggers import logger


@dataclass
class LegacyConfigEntry(object):
    """
    Creates a record for the config entry. contains
    Args:
        section: section the option should be found unddd
        option: the option str to lookup
        type_: Expected type of the value
    """

    section: str
    option: str
    type_: typing.Type = str

    def read_from_env(self, transform: typing.Optional[typing.Callable] = None) -> typing.Optional[typing.Any]:
        """
        Reads the config entry from environment variable, the structure of the env var is current
        ``FLYTE_{SECTION}_{OPTION}`` all upper cased. We will change this in the future.
        :return:
        """
        env = f"FLYTE_{self.section.upper()}_{self.option.upper()}"
        v = os.environ.get(env, None)
        if v is None:
            return None
        return transform(v) if transform else v

    def read_from_file(
        self, cfg: ConfigFile, transform: typing.Optional[typing.Callable] = None
    ) -> typing.Optional[typing.Any]:
        if not cfg:
            return None
        try:
            v = cfg.get(self)
            return transform(v) if transform else v
        except configparser.Error:
            pass
        return None


def bool_transformer(config_val: typing.Any):
    if type(config_val) is str:
        return True if config_val and not config_val.lower() in ["false", "0", "off", "no"] else False
    else:
        return config_val


@dataclass
class ConfigEntry(object):
    """
    A top level Config entry holder, that holds multiple different representations of the config.
    Currently only legacy is supported, but more will be added soon
    """

    legacy: LegacyConfigEntry
    transform: typing.Optional[typing.Callable[[str], typing.Any]] = None

    legacy_default_transforms = {
        bool: bool_transformer,
    }

    def __post_init__(self):
        if self.legacy:
            if not self.transform and self.legacy.type_ in ConfigEntry.legacy_default_transforms:
                self.transform = ConfigEntry.legacy_default_transforms[self.legacy.type_]

    def read(self, cfg: typing.Optional[ConfigFile] = None) -> typing.Optional[typing.Any]:
        """
        Reads the config Entry from the various sources in the following order,
         First try to read from environment, if not then try to read from the given config file
        :param cfg:
        :return:
        """
        from_env = self.legacy.read_from_env(self.transform)
        if from_env is None:
            return self.legacy.read_from_file(cfg, self.transform)
        return from_env


class ConfigFile(object):
    def __init__(self, location: str):
        """
        Load the config from this location
        """
        self._location = location
        # TODO, we can choose legacy vs other config using the extension. For .yaml, we can use the new config parser
        self._legacy_config = self._read_legacy_config(location)

    def _read_legacy_config(self, location: str) -> _configparser.ConfigParser:
        c = _configparser.ConfigParser()
        c.read(self._location)
        if c.has_section("internal"):
            raise _user_exceptions.FlyteAssertion(
                "The config file '{}' cannot contain a section for internal " "only configurations.".format(location)
            )
        return c

    def _get_from_legacy(self, c: LegacyConfigEntry) -> typing.Any:
        if issubclass(c.type_, bool):
            return self._legacy_config.getboolean(c.section, c.option)

        if issubclass(c.type_, int):
            return self._legacy_config.getint(c.section, c.option)

        if issubclass(c.type_, list):
            v = self._legacy_config.get(c.section, c.option)
            return v.split(",")

        return self._legacy_config.get(c.section, c.option)

    def get(self, c: typing.Union[LegacyConfigEntry]) -> typing.Any:
        if isinstance(c, LegacyConfigEntry):
            return self._get_from_legacy(c)
        raise NotImplementedError("Support for other config types besides .ini / .config files not yet supported")

    @property
    def legacy_config(self) -> _configparser.ConfigParser:
        return self._legacy_config


def get_config_file(c: typing.Union[str, ConfigFile, None]) -> typing.Optional[ConfigFile]:
    """
    Checks if the given argument is a file or a configFile and returns a loaded configFile else returns None
    """
    if c is None:
        # See if there's a config file in the current directory where Python is being run from
        current_location_config = Path("flytekit.config")
        if current_location_config.exists():
            logger.info(f"Using configuration from Python process root {current_location_config.absolute()}")
            return ConfigFile(current_location_config.absolute())

        # If not, see if there's a config in the user's home directory
        home_dir_config = Path(Path.home(), ".flyte", "config")  # _default_config_file_name in main.py
        if home_dir_config.exists():
            logger.info(f"Using configuration from home directory {home_dir_config.absolute()}")
            return ConfigFile(home_dir_config.absolute())

        # If not, then return None and let caller handle
        return None
    if isinstance(c, str):
        return ConfigFile(c)
    return c


def set_if_exists(d: dict, k: str, v: typing.Any) -> dict:
    """
    Given a dict ``d`` sets the key ``k`` with value of config ``v``, if the config value ``v`` is set
    and return the updated dictionary.

    .. note::

        The input dictionary ``d`` will be mutated.
    """
    if v:
        d[k] = v
    return d
