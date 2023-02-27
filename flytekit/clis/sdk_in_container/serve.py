from concurrent import futures

import click
import grpc
from flyteidl.service.plugin_system_pb2_grpc import add_BackendPluginServiceServicer_to_server

from flytekit.extend.backend.grpc_server import BackendPluginServer
from flytekit.loggers import cli_logger

_serve_help = """Start a grpc server for the backend plugin system."""


@click.command("serve", help=_serve_help)
@click.pass_context
def serve(_: click.Context):
    print("Starting a grpc server for the backend plugin system.")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_BackendPluginServiceServicer_to_server(BackendPluginServer(), server)

    server.add_insecure_port("[::]:8000")
    server.start()
    server.wait_for_termination()
