from __future__ import absolute_import
from flytekit.clients.raw import RawSynchronousFlyteClient
import mock

@mock.patch('flytekit.clients.raw._admin_service')
@mock.patch('flytekit.clients.raw._insecure_channel')
def test_client_set_token(mock_channel, mock_admin):
    mock_channel.return_value = True
    mock_admin.AdminServiceStub.return_value = True
    client = RawSynchronousFlyteClient(url='a.b.com', insecure=True)
    client.set_access_token('abc')
    assert client._metadata[0][1] == 'Bearer abc'
