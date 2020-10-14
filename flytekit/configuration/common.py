import abc as _abc
import configparser as _configparser
import os as _os

from flytekit.common.exceptions import user as _user_exceptions


def format_section_key(section, key):
    """
    :param Text section:
    :param Text key:
    :rtype: Text
    """
    return "FLYTE_{section}_{key}".format(section=section.upper(), key=key.upper())


class FlyteConfigurationFile(object):
    def __init__(self, location=None):
        """
        This singleton is initialized on module load with empty location. If pyflyte is called with
        a config flag, it'll reload the singleton with the passed config path.

        :param Text location: used to load config from this location.
        """
        self._location = None
        self._config = None
        self.reset_config(location)

    def _load_config(self):
        if self._config is None and self._location:
            config = _configparser.ConfigParser()
            config.read(self._location)
            if config.has_section("internal"):
                raise _user_exceptions.FlyteAssertion(
                    "The config file '{}' cannot contain a section for internal "
                    "only configurations.".format(self._location)
                )
            self._config = config

    def get_string(self, section, key, default=None):
        """
        :param Text section:
        :param Text key:
        :param Text default:
        :rtype: Text
        """
        self._load_config()
        if self._config is not None:
            try:
                return self._config.get(section, key, fallback=default)
            except Exception:
                pass
        return default

    def get_int(self, section, key, default=None):
        """
        :param Text section:
        :param Text key:
        :param int default:
        :rtype: int
        """
        self._load_config()
        if self._config is not None:
            try:
                return self._config.getint(section, key, fallback=default)
            except Exception:
                pass
        return default

    def get_bool(self, section, key, default=None):
        """
        :param Text section:
        :param Text key:
        :param bool default:
        :rtype: bool
        """
        self._load_config()
        if self._config is not None:
            try:
                return self._config.getboolean(section, key, fallback=default)
            except Exception:
                pass
        return default

    def reset_config(self, location):
        """
        :param Text location:
        """
        self._location = location or _os.environ.get("FLYTE_INTERNAL_CONFIGURATION_PATH")
        self._config = None


class _FlyteConfigurationPatcher(object):
    def __init__(self, new_value, config):
        """
        :param Text new_value:
        :param _FlyteConfigurationEntry config:
        """
        self._new_value = new_value
        self._config = config
        self._old_value = None

    def __enter__(self):
        self._old_value = _os.environ.get(self._config.env_var, None)
        if self._new_value is not None:
            _os.environ[self._config.env_var] = self._new_value
        elif self._old_value is not None:
            del _os.environ[self._config.env_var]

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._old_value is not None:
            _os.environ[self._config.env_var] = self._old_value
        else:
            del _os.environ[self._config.env_var]


def _get_file_contents(location):
    """
    This reads an input file, and returns the string contents, and should be used for reading credentials.
    This function will also strip newlines.

    :param Text location: The file path holding the client id or secret
    :rtype: Text
    """
    if _os.path.isfile(location):
        with open(location, "r") as f:
            return f.read().replace("\n", "")
    return None


class _FlyteConfigurationEntry(object, metaclass=_abc.ABCMeta):
    def __init__(self, section, key, default=None, validator=None, fallback=None):
        self._section = section
        self._key = key
        self._default = default
        self._validator = validator
        self._fallback = fallback

    @property
    def env_var(self):
        """
        :rtype: Text
        """
        return format_section_key(self._section, self._key)

    @_abc.abstractmethod
    def _getter(self):
        pass

    def retrieve_value(self):
        """
        The logic in this function changes the lookup behavior for all configuration objects before hitting the
        configuration file.

        For a given configuration object ('mysection', 'mysetting'), it will now look at this waterfall:

            i.) The environment variable named 'FLYTE_MYSECTION_MYSETTING'

            ii.) The value of the environment variable that is named the value of the environment variable named
                 'FLYTE_MYSECTION_MYSETTING'. That is if os.environ['FLYTE_MYSECTION_MYSETTING'] = 'AAA' and
                  os.environ['AA'] = 'abc', then 'abc' will be the final value.

            iii.) The contents of the file pointed to by the environment variable named 'FLYTE_MYSECTION_MYSETTING',
                  assuming the value is a file.

        While it is helpful, this pattern does interrupt the manually specified fallback logic, by effective injecting
        two more fallbacks behind the scenes. Just keep this in mind as you are using/creating configuration objects.
        :rtype: Text
        """
        val = _os.environ.get(self.env_var, None)
        if val is None:
            referenced_env_var = _os.environ.get("{}_FROM_ENV_VAR".format(self.env_var), None)
            if referenced_env_var is not None:
                val = _os.environ.get(referenced_env_var, None)
            if val is None:
                referenced_file = _os.environ.get("{}_FROM_FILE".format(self.env_var), None)
                if referenced_file is not None:
                    val = _get_file_contents(referenced_file)
        return val

    def get(self):
        val = self._getter()
        if val is None and self._fallback is not None:
            val = self._fallback.get()
        if self._validator:
            self._validator(val)
        return val

    def is_set(self):
        val = self._getter()
        return val is not None

    def get_patcher(self, value):
        return _FlyteConfigurationPatcher(value, self)


class _FlyteRequiredConfigurationEntry(_FlyteConfigurationEntry):
    def __init__(self, section, key, validator=None):
        super(_FlyteRequiredConfigurationEntry, self).__init__(section, key, validator=self._validate_not_null)
        self._extra_validator = validator

    def _validate_not_null(self, val):
        if val is None:
            raise _user_exceptions.FlyteAssertion(
                "No configuration set for [{}] {}.  This is a required configuration.".format(self._section, self._key)
            )
        if self._extra_validator:
            self._extra_validator(val)


class FlyteStringConfigurationEntry(_FlyteConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_string(self._section, self._key, default=self._default)
        return val


class FlyteIntegerConfigurationEntry(_FlyteConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_int(self._section, self._key, default=self._default)
        if val is not None:
            return int(val)
        return None


class FlyteBoolConfigurationEntry(_FlyteConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()

        if val is None:
            return CONFIGURATION_SINGLETON.get_bool(self._section, self._key, default=self._default)
        else:
            # Because bool('False') is True, compare to the same values that ConfigParser uses
            if val.lower() in ["false", "0", "off", "no"]:
                return False
            return True


class FlyteStringListConfigurationEntry(_FlyteConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_string(self._section, self._key)
        if val is None:
            return self._default
        return val.split(",")


class FlyteRequiredStringConfigurationEntry(_FlyteRequiredConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_string(self._section, self._key, default=self._default)
        return val


class FlyteRequiredIntegerConfigurationEntry(_FlyteRequiredConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_int(self._section, self._key, default=self._default)
        return int(val)


class FlyteRequiredBoolConfigurationEntry(_FlyteRequiredConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_bool(self._section, self._key, default=self._default)
        return bool(val)


class FlyteRequiredStringListConfigurationEntry(_FlyteRequiredConfigurationEntry):
    def _getter(self):
        val = self.retrieve_value()
        if val is None:
            val = CONFIGURATION_SINGLETON.get_string(self._section, self._key)
        if val is None:
            return self._default
        return val.split(",")


CONFIGURATION_SINGLETON = FlyteConfigurationFile()
