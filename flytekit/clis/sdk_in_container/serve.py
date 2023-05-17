from concurrent import futures

import click
import grpc
from flyteidl.service.external_plugin_service_pb2_grpc import add_ExternalPluginServiceServicer_to_server

from flytekit.extend.backend.external_plugin_service import BackendPluginServer

_serve_help = """Start a grpc server for the external plugin service."""


@click.command("serve", help=_serve_help)
@click.option(
    "--port",
    default="8000",
    is_flag=False,
    type=int,
    help="Grpc port for the external plugin service",
)
@click.option(
    "--worker",
    default="10",
    is_flag=False,
    type=int,
    help="Number of workers for the grpc server",
)
@click.option(
    "--timeout",
    default=None,
    is_flag=False,
    type=int,
    help="It will wait for the specified number of seconds before shutting down grpc server. It should only be used "
    "for testing.",
)
@click.pass_context
def serve(_: click.Context, port, worker, timeout):
    """
    Start a grpc server for the external plugin service.
    """
    click.secho("Starting the external plugin service...", fg="blue")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=worker))
    add_ExternalPluginServiceServicer_to_server(BackendPluginServer(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination(timeout=timeout)
