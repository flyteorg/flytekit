from __future__ import absolute_import

from flytekit.clis.auth import credentials as _credentials


def test_get_discovery_endpoint():
    endpoint = _credentials._get_discovery_endpoint('//localhost:8088', 'localhost:8089', True)
    assert endpoint == 'http://localhost:8088/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint('//localhost:8088', 'localhost:8089', False)
    assert endpoint == 'https://localhost:8088/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint('//localhost:8088/path', 'localhost:8089', True)
    assert endpoint == 'http://localhost:8088/path/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint('//localhost:8088/path', 'localhost:8089', False)
    assert endpoint == 'https://localhost:8088/path/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint('//flyte.corp.com', 'localhost:8089', False)
    assert endpoint == 'https://flyte.corp.com/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint('//flyte.corp.com/path', 'localhost:8089', False)
    assert endpoint == 'https://flyte.corp.com/path/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint(None, 'localhost:8089', True)
    assert endpoint == 'http://localhost:8089/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint(None, 'localhost:8089', False)
    assert endpoint == 'https://localhost:8089/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint(None, 'flyte.corp.com', True)
    assert endpoint == 'http://flyte.corp.com/.well-known/oauth-authorization-server'

    endpoint = _credentials._get_discovery_endpoint(None, 'flyte.corp.com', False)
    assert endpoint == 'https://flyte.corp.com/.well-known/oauth-authorization-server'
