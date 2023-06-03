from flytekit.core.data_persistence import FileAccessProvider


def test_get_random_remote_path():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    path = fp.join(fp.raw_output_prefix, fp.get_random_string())
    assert path.startswith("s3://my-bucket")
    assert fp.raw_output_prefix == "s3://my-bucket/"


def test_is_remote():
    fp = FileAccessProvider("/tmp", "s3://my-bucket")
    assert fp.is_remote("./checkpoint") is False
    assert fp.is_remote("/tmp/foo/bar") is False
    assert fp.is_remote("file://foo/bar") is False
    assert fp.is_remote("s3://my-bucket/foo/bar") is True


# def test_remote_file_access():
#     def get_upload_signed_url(content_md5: bytes, filename: str) -> CreateUploadLocationResponse:
#         return CreateUploadLocationResponse(
#             signed_url="s3://my-bucket/foo/bar", native_url="https://my-bucket.s3.amazonaws.com/foo/bar"
#         )
#
#     fp = RemoteFileAccessProvider("/tmp", "s3://my-bucket")
#     fp._get_upload_signed_url_fn = get_upload_signed_url
#     tmp = tempfile.NamedTemporaryFile(delete=False)
#     remote_path = fp.get_random_remote_path(tmp.name)
#
#     with pytest.raises(FlyteAssertion, match="No connection adapters were found"):
#         fp.put_data(tmp.name, remote_path, False)
#
#     with pytest.raises(FlyteAssertion, match="Recursive put is not supported"):
#         fp.put_data(tmp.name, remote_path, True)
