import mock

from flytekit import GCSPersistence


@mock.patch("flytekit.extras.persistence.gcs_gsutil._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.gcs_gsutil.GCSPersistence._check_binary")
def test_put(mock_check, mock_exec):
    proxy = GCSPersistence()
    proxy.put("/test", "gs://my-bucket/k1")
    mock_exec.assert_called_with(
        ['gsutil', 'cp', '/test', 'gs://my-bucket/k1'])


@mock.patch("flytekit.extras.persistence.gcs_gsutil._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.gcs_gsutil.GCSPersistence._check_binary")
def test_put_recursive(mock_check, mock_exec):
    proxy = GCSPersistence()
    proxy.put("/test", "gs://my-bucket/k1", True)
    mock_exec.assert_called_with(
        ['gsutil', 'cp', '-r', '/test/*', 'gs://my-bucket/k1/'])


@mock.patch("flytekit.extras.persistence.gcs_gsutil._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.gcs_gsutil.GCSPersistence._check_binary")
def test_get(mock_check, mock_exec):
    proxy = GCSPersistence()
    proxy.get("gs://my-bucket/k1", "/test")
    mock_exec.assert_called_with(
        ['gsutil', 'cp', 'gs://my-bucket/k1', '/test'])


@mock.patch("flytekit.extras.persistence.gcs_gsutil._update_cmd_config_and_execute")
@mock.patch("flytekit.extras.persistence.gcs_gsutil.GCSPersistence._check_binary")
def test_get_recursive(mock_check, mock_exec):
    proxy = GCSPersistence()
    proxy.get("gs://my-bucket/k1", "/test", True)
    mock_exec.assert_called_with(
        ['gsutil', 'cp', '-r', 'gs://my-bucket/k1/*', '/test'])
