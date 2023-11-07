import os
import pathlib
import typing

import rich_click as click
from google.protobuf.json_format import MessageToJson
from rich import print
from rich.panel import Panel
from rich.pretty import Pretty

from flytekit import BlobType, FlyteContext, Literal
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.core.type_engine import LiteralsResolver
from flytekit.interaction.rich_utils import RichCallback
from flytekit.interaction.string_literals import literal_map_string_repr, literal_string_repr
from flytekit.remote import FlyteRemote


def download_literal(var: str, data: Literal, download_to: typing.Optional[pathlib.Path] = None):
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
            is_multipart = False
            if data.scalar.blob:
                is_multipart = data.scalar.blob.metadata.type.dimensionality == BlobType.BlobDimensionality.MULTIPART
            elif data.scalar.structured_dataset:
                is_multipart = True
            FlyteContext.current_context().file_access.get_data(
                uri, str(download_to / var) + os.sep, is_multipart=is_multipart, callback=RichCallback()
            )
        elif data.scalar.union is not None:
            download_literal(var, data.scalar.union.value, download_to)
        elif data.scalar.generic is not None:
            with open(download_to / f"{var}.json", "w") as f:
                f.write(MessageToJson(data.scalar.generic))
        else:
            print(
                f"[dim]Skipping {var} val {literal_string_repr(data)} as it is not a blob, structured dataset,"
                f" or generic type.[/dim]"
            )
            return
    elif data.collection:
        for i, v in enumerate(data.collection.literals):
            download_literal(f"{i}", v, download_to / var)
    elif data.map:
        download_to = pathlib.Path(download_to)
        for k, v in data.map.literals.items():
            download_literal(f"{k}", v, download_to / var)
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
        download_to = pathlib.Path(download_to)
        if isinstance(data, Literal):
            download_literal("data", data, download_to)
        else:
            if not recursive:
                raise click.UsageError("Please specify --recursive to download all variables in a literal map.")
            for var, literal in data.literals.items():
                download_literal(var, literal, download_to)
