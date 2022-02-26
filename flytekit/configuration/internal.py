import configparser
import tempfile
import typing

from flytekit.configuration.file import ConfigFile, ConfigEntry


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
            image_names = None
        if image_names:
            for i in image_names:
                images[str(i)] = cfg.config.get("images", i)
        return images


class AWS(object):
    SECTION = "aws"
    S3_ENDPOINT = ConfigEntry(SECTION, "endpoint", str)
    S3_ACCESS_KEY_ID = ConfigEntry(SECTION, "access_key_id", str)
    S3_SECRET_ACCESS_KEY = ConfigEntry(SECTION, "secret_access_key", str)
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

    LOCAL_SANDBOX = ConfigEntry(SECTION, "local_sandbox", str)
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