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
    """
    Initializes the global remote client. This is required before executing any tasks or workflows remotely via `.remote()` method.

    :param default_project: default project to use when fetching or executing flyte entities.
    :param default_domain: default domain to use when fetching or executing flyte entities.
    :param data_upload_location: this is where all the default data will be uploaded when providing inputs.
        The default location - `s3://my-s3-bucket/data` works for sandbox/demo environment. Please override this for non-sandbox cases.
    :return:
    """
    global REMOTE_ENTRY
    REMOTE_ENTRY = FlyteRemote(
        config=config,
        default_project=default_project,
        default_domain=default_domain,
        data_upload_location=data_upload_location,
        **kwargs,
    )
