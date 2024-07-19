import typing
from flytekit.configuration import Config
from flytekit.remote.remote import FlyteRemote

REMOTE_ENTRY = None

def init_remote(
    config: Config,
    default_project: typing.Optional[str] = None,
    default_domain: typing.Optional[str] = None,
    data_upload_location: str = "flyte://my-s3-bucket/",
    **kwargs,
):
    global REMOTE_ENTRY
    REMOTE_ENTRY = FlyteRemote(
        config=config, 
        default_project=default_project,
        default_domain=default_domain,
        data_upload_location=data_upload_location,
        **kwargs
    )