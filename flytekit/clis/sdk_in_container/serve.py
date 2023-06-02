from concurrent import futures

import click
import grpc
from flyteidl.service.agent_pb2_grpc import add_AsyncAgentServiceServicer_to_server

from flytekit.extend.backend.agent_service import AgentService

_serve_help = """Start a grpc server for the agent service."""


@click.command("serve", help=_serve_help)
@click.option(
    "--port",
    default="8000",
    is_flag=False,
    type=int,
    help="Grpc port for the agent service",
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
    Start a grpc server for the agent service.
    """
    click.secho("Starting the agent service...", fg="blue")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=worker))
    add_AsyncAgentServiceServicer_to_server(AgentService(), server)

    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination(timeout=timeout)
