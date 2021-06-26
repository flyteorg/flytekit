import click

from flytekit.core.context_manager import Image, look_up_image_info, ImageConfig

_DEFAULT_IMAGE_NAME = "default"


def validate_image(ctx, param, values):
    default_image = None
    images = []
    for v in values:
        if '=' in v:
            vals = v.split('=')
            if len(vals) > 2:
                raise click.BadParameter(f"Bad value received [{v}] for param {param}, more than 1 `=` is not allowed")
            name = vals[0]
            tag = vals[1]
            img = look_up_image_info(name, tag, False)
        else:
            img = look_up_image_info(_DEFAULT_IMAGE_NAME, v, False)

        if default_image and img.name == _DEFAULT_IMAGE_NAME:
            raise click.BadParameter(
                f"Only one default image can be specified. Received multiple {default_image}&{img}")
        if img.name == _DEFAULT_IMAGE_NAME:
            default_image = img
        else:
            images.append(img)

    return ImageConfig(default_image, images)


@click.command("package")
@click.option(
    "-i", "--image", "images", required=False, multiple=True,
    type=click.UNPROCESSED, callback=validate_image,
    help="A fully qualified tag for an docker image, e.g. somedocker.com/myimage:someversion123. This is a "
         "multi-option and can be of the form --image xyz.io/docker:latest"
         " --image my_image=xyz.io/docker2:latest. Note, the `name=image_uri`. The name is optional, if not"
         "provided the image will be used as the default image. All the names have to be unique, and thus"
         "there can only be one --image option with no-name."
)
@click.option(
    "--output",
    required=False,
    type=click.Path(dir_okay=True, file_okay=False, writable=True, resolve_path=True),
    default="./_pb_output",
    help="Directory where the package should be output to.",
)
@click.option(
    "--archive",
    default=False,
    required=False,
    help="This flag ensures that the output is a single zipped tarball.",
)
@click.option(
    "--fast",
    default=False,
    required=False,
    help="This flag enables fast packaging, that allows `no container build` deploys of flyte workflows and tasks."
         "Note this needs additional configuration, refer to the docs.",
)
@click.pass_context
def package(ctx, images, output, archive, fast):
    """
    This command produces protobufs for tasks and templates.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.  In lieu of Admin,
        this serialization step will set the URN of the tasks to the fully qualified name of the task function.
    """
    pass
