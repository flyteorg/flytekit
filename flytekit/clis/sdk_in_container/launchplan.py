import rich_click as click

from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.models.launch_plan import LaunchPlanState

_launchplan_help = """
The launchplan command activates or deactivates a specified or the latest version of the launchplan.
If ``--activate`` is chosen then the previous version of the launchplan will be deactivated.

- ``launchplan`` refers to the name of the Launchplan
- ``launchplan_version`` is optional and should be a valid version for a Launchplan version. If not specified the latest will be used.
"""


@click.command("launchplan", help=_launchplan_help)
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
@click.option(
    "--activate/--deactivate",
    required=True,
    type=bool,
    is_flag=True,
    help="Activate or Deactivate the launchplan",
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
def launchplan(
    ctx: click.Context,
    project: str,
    domain: str,
    activate: bool,
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
        state = LaunchPlanState.ACTIVE if activate else LaunchPlanState.INACTIVE
        remote.client.update_launch_plan(id=launchplan.id, state=state)
        click.secho(
            f"\n Launchplan was set to {LaunchPlanState.enum_to_string(state)}: {launchplan.name}:{launchplan.id.version}",
            fg="green",
        )
    except StopIteration as e:
        click.secho(f"{e.value}", fg="red")
