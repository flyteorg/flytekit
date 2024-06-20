from concurrent import futures

import grpc
import rich_click as click
from flyteidl.service import agent_pb2
from flyteidl.service.agent_pb2_grpc import (
    add_AgentMetadataServiceServicer_to_server,
    add_AsyncAgentServiceServicer_to_server,
    add_SyncAgentServiceServicer_to_server,
)
from rich.console import Console
from rich.table import Table


@click.group("serve")
@click.pass_context
def serve(ctx: click.Context):
    """
    Start the specific service.
    """
    pass


@serve.command()
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
def agent(_: click.Context, port, worker, timeout):
    """
    Start a grpc server for the agent service.
    """
    import asyncio

    asyncio.run(_start_grpc_server(port, worker, timeout))


async def _start_grpc_server(port: int, worker: int, timeout: int):
    from flytekit.extend.backend.agent_service import AgentMetadataService, AsyncAgentService, SyncAgentService

    click.secho("🚀 Starting the agent service...")
    _start_http_server()
    print_agents_metadata()

    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=worker))

    add_AsyncAgentServiceServicer_to_server(AsyncAgentService(), server)
    add_SyncAgentServiceServicer_to_server(SyncAgentService(), server)
    add_AgentMetadataServiceServicer_to_server(AgentMetadataService(), server)
    _start_health_check_server(server, worker)

    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    await server.wait_for_termination(timeout)


def _start_http_server():
    try:
        from prometheus_client import start_http_server

        click.secho("Starting up the server to expose the prometheus metrics...")
        start_http_server(9090)
    except ImportError as e:
        click.secho(f"Failed to start the prometheus server with error {e}", fg="red")


def _start_health_check_server(server: grpc.Server, worker: int):
    try:
        from grpc_health.v1 import health, health_pb2, health_pb2_grpc

        health_servicer = health.HealthServicer(
            experimental_non_blocking=True,
            experimental_thread_pool=futures.ThreadPoolExecutor(max_workers=worker),
        )

        for service in agent_pb2.DESCRIPTOR.services_by_name.values():
            health_servicer.set(service.full_name, health_pb2.HealthCheckResponse.SERVING)
        health_servicer.set(health.SERVICE_NAME, health_pb2.HealthCheckResponse.SERVING)

        health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    except ImportError as e:
        click.secho(f"Failed to start the health check servicer with error {e}", fg="red")


def print_agents_metadata():
    from flytekit.extend.backend.base_agent import AgentRegistry

    agents = AgentRegistry.list_agents()

    table = Table(title="Agent Metadata")
    table.add_column("Agent Name", style="cyan", no_wrap=True)
    table.add_column("Support Task Types", style="cyan")
    table.add_column("Is Sync", style="green")

    for a in agents:
        categories = ""
        for c in a.supported_task_categories:
            categories += f"{c.name} (v{c.version}) "
        table.add_row(a.name, categories, str(a.is_sync))

    console = Console()
    console.print(table)
