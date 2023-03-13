from concurrent import futures

import click
import grpc
from flyteidl.service.plugin_system_pb2_grpc import add_BackendPluginServiceServicer_to_server

from flytekit.extend.backend.grpc_server import BackendPluginServer
from flytekit.loggers import cli_logger

_serve_help = """Start a grpc server for the backend plugin system."""


@click.command("serve", help=_serve_help)
@click.option(
    "--port",
    default="9090",
    is_flag=False,
    type=int,
    help="Grpc port for the flyteplugins service",
)
@click.pass_context
def serve(_: click.Context, port):
    cli_logger.info("Starting a grpc server for the flyteplugins service.")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_BackendPluginServiceServicer_to_server(BackendPluginServer(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()
