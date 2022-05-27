import mock

from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context


@mock.patch("flytekit.clis.sdk_in_container.helpers.FlyteRemote")
def test_saving_remote(mock_remote):
    mock_context = mock.MagicMock
    mock_context.obj = {}
    get_and_save_remote_with_click_context(mock_context, "p", "d")
    assert mock_context.obj["flyte_remote"] is not None
