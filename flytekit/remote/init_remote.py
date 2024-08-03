import threading
import typing

from flytekit.configuration import Config
from flytekit.remote.remote import FlyteRemote
from flytekit.tools.interactive import ipython_check
from flytekit.tools.translator import Options

REMOTE_ENTRY: typing.Optional[FlyteRemote] = None
# TODO: This should be merged into the FlyteRemote in the future
REMOTE_DEFAULT_OPTIONS: typing.Optional[Options] = None
REMOTE_ENTRY_LOCK = threading.Lock()


def init_remote(
    config: Config,
    default_project: typing.Optional[str] = None,
    default_domain: typing.Optional[str] = None,
    data_upload_location: str = "flyte://my-s3-bucket/",
    default_options: typing.Optional[Options] = None,
    interactive_mode_enabled: bool = ipython_check(),
    **kwargs,
):
    """
    Initializes the global remote client. This is required before executing any tasks or workflows remotely via `.remote()` method.

    :param default_project: default project to use when fetching or executing flyte entities.
    :param default_domain: default domain to use when fetching or executing flyte entities.
    :param data_upload_location: this is where all the default data will be uploaded when providing inputs.
        The default location - `s3://my-s3-bucket/data` works for sandbox/demo environment. Please override this for non-sandbox cases.
    :param default_options: default options to use when executing tasks or workflows remotely.
    :param interactive_mode_enabled: If True, the client will be configured to work in interactive mode.
    :return:
    """
    global REMOTE_ENTRY, REMOTE_DEFAULT_OPTIONS
    with REMOTE_ENTRY_LOCK:
        if REMOTE_ENTRY is None:
            REMOTE_ENTRY = FlyteRemote(
                config=config,
                default_project=default_project,
                default_domain=default_domain,
                data_upload_location=data_upload_location,
                interactive_mode_enabled=interactive_mode_enabled,
                **kwargs,
            )
            # TODO: This should be merged into the FlyteRemote in the future
            REMOTE_DEFAULT_OPTIONS = default_options
        else:
            raise AssertionError("Remote client already initialized")
