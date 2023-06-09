from base64 import b64encode

import pytest
import asyncio
from flytekit.configuration import Config
from flytekit.remote.remote import FlyteRemote
from flytekit.remote.remote_fs import RemoteFS


def test_basics():
    r = FlyteRemote(
        Config.for_sandbox(),
        default_project="flytesnacks",
        default_domain="development",
        data_upload_location="flyte://dv1/",
    )
    fs = RemoteFS(remote=r)
    assert fs.protocol == "flyte"
    assert fs.sep == "/"
    assert fs.unstrip_protocol("dv/fwu11/") == "flyte://dv/fwu11/"


def test_upl():
    r = FlyteRemote(
        Config.for_sandbox(),
        default_project="flytesnacks",
        default_domain="development",
        data_upload_location="flyte://data",
    )
    encoded_md5 = b64encode(b"hi2dfsfj23333ileksa")
    xx = r.client.get_upload_signed_url(
        "flytesnacks", "development", content_md5=encoded_md5, filename="parent/child/1"
    )
    print(xx.native_url)


@pytest.mark.sandbox_test
def test_remote_upload():
    r = FlyteRemote(
        Config.for_sandbox(),
        default_project="flytesnacks",
        default_domain="development",
        data_upload_location="flyte://data/<proj>/<domain>",
    )
    fs = RemoteFS(remote=r)

    # Test uploading a file and folder.
    fs.put("/Users/ytong/temp/data/source", "flyte://data", recursive=True)

    print("hi")

    # Test downloading a literal to a file.
