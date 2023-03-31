from concurrent import futures

import click
import grpc
from flyteidl.service.external_plugin_service_pb2_grpc import add_ExternalPluginServiceServicer_to_server

from flytekit.extend.backend.external_plugin_service import BackendPluginServer

_serve_help = """Start a grpc server for the external plugin service."""


@click.command("serve", help=_serve_help)
@click.option(
    "--port",
    default="80",
    is_flag=False,
    type=int,
    help="Grpc port for the flyteplugins service",
)
@click.pass_context
def serve(_: click.Context, port):
    click.secho(f"Starting the external plugin service...", fg="blue")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_ExternalPluginServiceServicer_to_server(BackendPluginServer(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()
