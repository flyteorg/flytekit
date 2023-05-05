from dataclasses import replace
from typing import Optional

import rich_click as click

from flytekit.clis.sdk_in_container.constants import CTX_CONFIG_FILE
from flytekit.configuration import Config, ImageConfig, get_config_file
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
    cfg_file = get_config_file(cfg_file_location)
    if cfg_file is None:
        cfg_obj = Config.for_sandbox()
        cli_logger.info("No config files found, creating remote with sandbox config")
    else:
        cfg_obj = Config.auto(cfg_file_location)
        cli_logger.info(
            f"Creating remote with config {cfg_obj}" + (f" with file {cfg_file_location}" if cfg_file_location else "")
        )
    r = FlyteRemote(cfg_obj, default_project=project, default_domain=domain)
    if save:
        ctx.obj[FLYTE_REMOTE_INSTANCE_KEY] = r
    return r


def patch_image_config(config_file: Optional[str], image_config: ImageConfig) -> ImageConfig:
    """
    Merge ImageConfig object with images defined in config file
    """
    # Images come from three places:
    # * The default flytekit images, which are already supplied by the base run_level_params.
    # * The images provided by the user on the command line.
    # * The images provided by the user via the config file, if there is one. (Images on the command line should
    #   override all).
    #
    # However, the run_level_params already contains both the default flytekit images (lowest priority), as well
    # as the images from the command line (highest priority). So when we read from the config file, we only
    # want to add in the images that are missing, including the default, if that's also missing.
    additional_image_names = set([v.name for v in image_config.images])
    new_additional_images = [v for v in image_config.images]
    new_default = image_config.default_image
    if config_file:
        cfg_ic = ImageConfig.auto(config_file=config_file)
        new_default = new_default or cfg_ic.default_image
        for addl in cfg_ic.images:
            if addl.name not in additional_image_names:
                new_additional_images.append(addl)
    return replace(image_config, default_image=new_default, images=new_additional_images)
