import click

from flytekit.clis.sdk_in_container.constants import CTX_CONFIG_FILE
from flytekit.configuration import Config
from flytekit.loggers import cli_logger
from flytekit.remote.remote import FlyteRemote

FLYTE_REMOTE_INSTANCE_KEY = "flyte_remote"


def get_and_save_remote_with_click_context(
    ctx: click.Context, project: str, domain: str, save: bool = True
) -> FlyteRemote:
    """
    NB: This function will by default mutate the click Context.obj dictionary, adding a remote key with value
        of the created FlyteRemote object.

    :param ctx: the click context object
    :param project: default project for the remote instance
    :param domain: default domain
    :param save: If false, will not mutate the context.obj dict
    :return: FlyteRemote instance
    """
    cfg_file_location = ctx.obj.get(CTX_CONFIG_FILE)
    cfg_obj = Config.auto(cfg_file_location)
    cli_logger.info(
        f"Creating remote with config {cfg_obj}" + (f" with file {cfg_file_location}" if cfg_file_location else "")
    )
    r = FlyteRemote(cfg_obj, default_project=project, default_domain=domain)
    if save:
        ctx.obj[FLYTE_REMOTE_INSTANCE_KEY] = r
    return r
