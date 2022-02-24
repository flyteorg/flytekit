import configparser
import configparser as _configparser
import tempfile
import typing
from dataclasses import dataclass

from flytekit import logger
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


class Images(object):
    @staticmethod
    def get_specified_images(cfg: ConfigFile) -> typing.Dict[str, str]:
        """
        This section should contain options, where the option name is the friendly name of the image and the corresponding
        value is actual FQN of the image. Example of how the section is structured
        [images]
        my_image1=docker.io/flyte:tag
        # Note that the tag is optional. If not specified it will be the default version identifier specified
        my_image2=docker.io/flyte

        :returns a dictionary of name: image<fqn+version> Version is optional
        """
        images: typing.Dict[str, str] = {}
        if cfg is None:
            return images
        try:
            image_names = cfg.config.options("images")
        except configparser.NoSectionError:
            logger.info("No images specified, will use the default image")
            image_names = None
        if image_names:
            for i in image_names:
                images[str(i)] = cfg.config.get("images", i)
        return images


class AWS(object):
    SECTION = "aws"
    S3_ENDPOINT = ConfigEntry(SECTION, "endpoint", str)
    S3_ACCESS_KEY_ID = ConfigEntry(SECTION, "access_key_id")
    S3_SECRET_ACCESS_KEY = ConfigEntry(SECTION, "secret_access_key")
    S3_ACCESS_KEY_ID_ENV_NAME = "AWS_ACCESS_KEY_ID"
    S3_SECRET_ACCESS_KEY_ENV_NAME = "AWS_SECRET_ACCESS_KEY"
    S3_ENDPOINT_ARG_NAME = "--endpoint-url"
    ENABLE_DEBUG = ConfigEntry(SECTION, "enable_debug", bool, default_val=False)
    RETRIES = ConfigEntry(SECTION, "retries", int, default_val=3)
    BACKOFF_SECONDS = ConfigEntry(SECTION, "backoff_seconds", int, default_val=5)


class GCP(object):
    SECTION = "gcp"
    GSUTIL_PARALLELISM = ConfigEntry(SECTION, "gsutil_parallelism", bool, default_val=False)


class Credentials(object):
    SECTION = "credentials"
    COMMAND = ConfigEntry(SECTION, "command", str)
    """
    This command is executed to return a token using an external process.
    """

    CLIENT_ID = ConfigEntry(SECTION, "client_id", str)
    """
    This is the public identifier for the app which handles authorization for a Flyte deployment.
    More details here: https://www.oauth.com/oauth2-servers/client-registration/client-id-secret/.
    """

    CLIENT_CREDENTIALS_SECRET = ConfigEntry(SECTION, "client_secret", str)
    """
    Used for basic auth, which is automatically called during pyflyte. This will allow the Flyte engine to read the
    password directly from the environment variable. Note that this is less secure! Please only use this if mounting the
    secret as a file is impossible.
    """

    SCOPES = ConfigEntry(SECTION, "scopes", list)

    AUTH_MODE = ConfigEntry(SECTION, "auth_mode", str)
    """
    The auth mode defines the behavior used to request and refresh credentials. The currently supported modes include:
    - 'standard' This uses the pkce-enhanced authorization code flow by opening a browser window to initiate credentials
            access.
    - 'basic' or 'client_credentials' This uses cert-based auth in which the end user enters a client id and a client
            secret and public key encryption is used to facilitate authentication.
    - None: No auth will be attempted.
    """


class Platform(object):
    SECTION = "platform"
    URL = ConfigEntry(SECTION, "url", str)
    INSECURE = ConfigEntry(SECTION, "insecure", bool)


class LocalSDK(object):
    SECTION = "sdk"
    WORKFLOW_PACKAGES = ConfigEntry(SECTION, "workflow_packages", list, default_val=[])
    """
    This is a comma-delimited list of packages that SDK tools will use to discover entities for the purpose of registration
    and execution of entities.
    """

    LOCAL_SANDBOX = ConfigEntry(SECTION, "local_sandbox", str, default_val=tempfile.mkdtemp(prefix="flyte"))
    """
    This is the path where SDK will place files during local executions and testing.  The SDK will not automatically
    clean up data in these directories.
    """

    LOGGING_LEVEL = ConfigEntry(SECTION, "logging_level", int, default_val=20)
    """
    This is the default logging level for the Python logging library and will be set before user code runs.
    Note that this configuration is special in that it is a runtime setting, not a compile time setting.  This is the only
    runtime option in this file.

    TODO delete the one from internal config
    """

    # Feature Gate
    USE_STRUCTURED_DATASET = ConfigEntry(SECTION, "use_structured_dataset", bool, default_val=False)
    """
    Note: This gate will be switched to True at some point in the future. Definitely by 1.0, if not v0.31.0.

    """


class Secrets(object):
    SECTION = "secrets"
    # Secrets management
    ENV_PREFIX = ConfigEntry(SECTION, "env_prefix", str)
    """
    This is the prefix that will be used to lookup for injected secrets at runtime. This can be overridden to using
    FLYTE_SECRETS_ENV_PREFIX variable
    """

    DEFAULT_DIR = ConfigEntry(SECTION, "default_dir", str)
    """
    This is the default directory that will be used to find secrets as individual files under. This can be overridden using
    FLYTE_SECRETS_DEFAULT_DIR.
    """

    FILE_PREFIX = ConfigEntry(SECTION, "file_prefix", str)
    """
    This is the prefix for the file in the default dir.
    """


class StatsD(object):
    SECTION = "secrets"
    # StatsD Config flags should ideally be controlled at the platform level and not through flytekit's config file.
    # They are meant to allow administrators to control certain behavior according to how the system is configured.

    HOST = ConfigEntry(SECTION, "host", str)
    PORT = ConfigEntry(SECTION, "port", int)
    DISABLED = ConfigEntry(SECTION, "disabled", bool)
    DISABLE_TAGS = ConfigEntry(SECTION, "disable_tags", bool)
