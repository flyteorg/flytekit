import rich
import rich_click as click

from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.remote import FlyteRemote


@click.command("fetch")
@click.argument("flyte_data_uri", type=str, required=True, metavar="FLYTE-DATA-URI (of the form flyte://...)")
@click.pass_context
def fetch(ctx: click.Context, flyte_data_uri: str):
    """
    Retrieve Inputs/Outputs for a Flyte Execution or any of the inner node executions from the remote server.

    The URI can be retrieved from the Flyte Console, or by invoking the get_data API.
    """

    remote: FlyteRemote = get_and_save_remote_with_click_context(ctx, project="flytesnacks", domain="development")
    click.secho(f"Fetching data from {flyte_data_uri}...", dim=True)
    data = remote.get(flyte_data_uri)
    rich.print(data.literals)
