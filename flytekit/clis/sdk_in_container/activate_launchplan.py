import click
from flytekit.models.launch_plan import LaunchPlanState

from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context

_activate_launchplan_help = """
The activate-launchplan command activates a specified or the latest version of the launchplan and disables a previous version.

- ``launchplan`` refers to the name of the Launchplan
- ``launchplan_version`` is optional and should be a valid version for a Launchplan version. If not specified the latest will be used.
"""


@click.command("activate-launchplan", help=_activate_launchplan_help)
@click.option(
    "-p",
    "--project",
    required=False,
    type=str,
    default="flytesnacks",
    help="Fecth launchplan from this project",
)
@click.option(
    "-d",
    "--domain",
    required=False,
    type=str,
    default="development",
    help="Fetch launchplan from this domain",
)
@click.argument(
    "launchplan",
    required=True,
    type=str,
)
@click.argument(
    "launchplan-version",
    required=False,
    type=str,
    default=None,
)
@click.pass_context
def activate_launchplan(
    ctx: click.Context,
    project: str,
    domain: str,
    launchplan: str,
    launchplan_version: str,
):
    remote = get_and_save_remote_with_click_context(ctx, project, domain)
    try:
        launchplan = remote.fetch_launch_plan(
            project=project,
            domain=domain,
            name=launchplan,
            version=launchplan_version,
        )
        remote.client.update_launch_plan(id=launchplan.id, state=LaunchPlanState.ACTIVE)
        click.secho(f"\n Launchplan was activated: {launchplan.name}:{launchplan.id.version}", fg="green")
    except StopIteration as e:
        click.secho(f"{e.value}", fg="red")
