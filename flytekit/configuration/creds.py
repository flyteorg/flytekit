from __future__ import absolute_import

from flytekit.configuration import common as _config_common

DISCOVERY_ENDPOINT = _config_common.FlyteStringConfigurationEntry('credentials', 'discovery_endpoint', default=None)
"""
This endpoint fetches authorization server metadata as describe in this proposal:
https://tools.ietf.org/id/draft-ietf-oauth-discovery-08.html.
The endpoint path can be relative or absolute.
"""

CLIENT_ID = _config_common.FlyteStringConfigurationEntry('credentials', 'client_id', default=None)
"""
This is the public identifier for the app which handles authorization for a Flyte deployment.
More details here: https://www.oauth.com/oauth2-servers/client-registration/client-id-secret/.
"""

REDIRECT_URI = _config_common.FlyteStringConfigurationEntry('credentials', 'redirect_uri', default=None)
"""
This is the redirect uri registered with the app which handles authorization for a Flyte deployment.
More details here: https://www.oauth.com/oauth2-servers/redirect-uris/.
"""


AUTHORIZATION_METADATA_KEY = _config_common.FlyteStringConfigurationEntry('credentials', 'authorization_metadata_key',
                                                                          default="authorization")
"""
The authorization metadata key used for passing access tokens in gRPC requests.
Traditionally this value is 'authorization' however it is made configurable.
"""
