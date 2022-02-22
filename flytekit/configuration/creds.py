from flytekit.configuration import common as _config_common

COMMAND = _config_common.FlyteStringListConfigurationEntry("credentials", "command", default=None)
"""
This command is executed to return a token using an external process.
"""

CLIENT_ID = _config_common.FlyteStringConfigurationEntry("credentials", "client_id", default=None)
"""
This is the public identifier for the app which handles authorization for a Flyte deployment.
More details here: https://www.oauth.com/oauth2-servers/client-registration/client-id-secret/.
"""

CLIENT_CREDENTIALS_SECRET = _config_common.FlyteStringConfigurationEntry("credentials", "client_secret", default=None)
"""
Used for basic auth, which is automatically called during pyflyte. This will allow the Flyte engine to read the
password directly from the environment variable. Note that this is less secure! Please only use this if mounting the
secret as a file is impossible.
"""

SCOPES = _config_common.FlyteStringListConfigurationEntry("credentials", "scopes", default=[])

AUTH_MODE = _config_common.FlyteStringConfigurationEntry("credentials", "auth_mode", default="standard")
"""
The auth mode defines the behavior used to request and refresh credentials. The currently supported modes include:
- 'standard' This uses the pkce-enhanced authorization code flow by opening a browser window to initiate credentials
        access.
- 'basic' or 'client_credentials' This uses cert-based auth in which the end user enters a client id and a client
        secret and public key encryption is used to facilitate authentication.
- None: No auth will be attempted.
"""
