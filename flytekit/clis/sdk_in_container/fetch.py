import json
import pathlib
import typing

import rich_click as click
from google.protobuf.json_format import MessageToJson
from rich import print
from rich.panel import Panel
from rich.pretty import Pretty

from flytekit import FlyteContext, Literal
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.core.type_engine import LiteralsResolver
from flytekit.interaction.rich_utils import RichCallback
from flytekit.interaction.string_literals import literal_map_string_repr, literal_string_repr
from flytekit.remote import FlyteRemote


def download_literal(var: str, data: Literal, download_to: typing.Optional[str] = None):
    """
    Download a single literal to a file, if it is a blob or structured dataset.
    """
    if data is None:
        print(f"Skipping {var} as it is None.")
        return
    if data.scalar:
        if data.scalar and (data.scalar.blob or data.scalar.structured_dataset):
            uri = data.scalar.blob.uri if data.scalar.blob else data.scalar.structured_dataset.uri
            if uri is None:
                print("No data to download.")
                return
            FlyteContext.current_context().file_access.get_data(
                uri, download_to, is_multipart=False, callback=RichCallback()
            )
        elif data.scalar.union is not None:
            download_literal(var, data.scalar.union.value, download_to)
        elif data.scalar.generic is not None:
            with open(download_to, "w") as f:
                json.dump(MessageToJson(data.scalar.generic), f)
        else:
            print(
                f"[dim]Skipping {var} val {literal_string_repr(data)} as it is not a blob, structured dataset,"
                f" or generic type.[/dim]"
            )
            return
    elif data.collection:
        download_to = pathlib.Path(download_to)
        for i, v in enumerate(data.collection.literals):
            download_literal(f"{var}_{i}", v, str(download_to / f"{i}"))
    elif data.map:
        download_to = pathlib.Path(download_to)
        for k, v in data.map.literals.items():
            download_literal(f"{var}_{k}", v, str(download_to / f"{k}"))
    print(f"Downloaded f{var} to {download_to}")


@click.command("fetch")
@click.option(
    "--recursive",
    "-r",
    is_flag=True,
    help="Fetch recursively, all variables in the URI. This is not needed for directrories as they"
    " are automatically recursively downloaded.",
)
@click.argument("flyte-data-uri", type=str, required=True, metavar="FLYTE-DATA-URI (format flyte://...)")
@click.argument(
    "download-to", type=click.Path(), required=False, default=None, metavar="DOWNLOAD-TO Local path (optional)"
)
@click.pass_context
def fetch(ctx: click.Context, recursive: bool, flyte_data_uri: str, download_to: typing.Optional[str] = None):
    """
    Retrieve Inputs/Outputs for a Flyte Execution or any of the inner node executions from the remote server.

    The URI can be retrieved from the Flyte Console, or by invoking the get_data API.
    """

    remote: FlyteRemote = get_and_save_remote_with_click_context(ctx, project="flytesnacks", domain="development")
    click.secho(f"Fetching data from {flyte_data_uri}...", dim=True)
    data = remote.get(flyte_data_uri)
    if isinstance(data, Literal):
        p = literal_string_repr(data)
    elif isinstance(data, LiteralsResolver):
        p = literal_map_string_repr(data.literals)
    else:
        p = data
    pretty = Pretty(p)
    panel = Panel(pretty)
    print(panel)
    if download_to:
        if isinstance(data, Literal):
            download_literal("data", data, download_to)
        else:
            if not recursive:
                raise click.UsageError("Please specify --recursive to download a all variables in a literal map.")
            download_to = pathlib.Path(download_to)
            for var, literal in data.literals.items():
                download_literal(var, literal, str(download_to / var))
