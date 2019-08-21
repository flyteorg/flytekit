from __future__ import absolute_import

from flytekit.configuration import common as _config_common

ASSUMABLE_IAM_ROLE = _config_common.FlyteStringConfigurationEntry('auth', 'assumable_iam_role', default=None)
"""
This is the role the SDK will use by default to execute workflows.  For example, in AWS this should be an IAM role
string.
"""

KUBERNETES_SERVICE_ACCOUNT = _config_common.FlyteStringConfigurationEntry(
    'auth', 'kubernetes_service_account', default=None)
"""
This is the kubernetes service account that will be passed to workflow executions.
"""
