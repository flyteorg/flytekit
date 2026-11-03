import rich_click as click

from flytekit.clis.sdk_in_container.constants import CTX_DOMAIN, CTX_PROJECT
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.interfaces import cli_identifiers

CTX_DEPTH = "depth"

_dump_help = """
The dump command aggregates workflow execution metrics and displays them. This aggregation is meant to provide an easy
to understand breakdown of where time is spent in a hierarchical manner.

- execution_id refers to the id of the workflow execution
"""

_explain_help = """
The explain command prints each individual execution span and the associated timestamps and Flyte entity reference.
This breakdown provides precise information into exactly how and when Flyte processes a workflow execution.

- execution_id refers to the id of the workflow execution
"""


@click.group("metrics")
@click.option(
    "-d",
    "--depth",
    required=False,
    type=int,
    default=-1,
    help="The depth of Flyte entity hierarchy to traverse when computing metrics for this execution",
)
@click.option(
    "-p",
    "--project",
    required=False,
    type=str,
    default="flytesnacks",
    help="The project of the workflow execution",
)
@click.option(
    "-d",
    "--domain",
    required=False,
    type=str,
    default="development",
    help="The domain of the workflow execution",
)
@click.pass_context
def metrics(ctx: click.Context, depth, domain, project):
    ctx.obj[CTX_DEPTH] = depth
    ctx.obj[CTX_DOMAIN] = domain
    ctx.obj[CTX_PROJECT] = project


@click.command("dump", help=_dump_help)
@click.argument("execution_id", type=str)
@click.pass_context
def metrics_dump(
    ctx: click.Context,
    execution_id: str,
):
    depth = ctx.obj[CTX_DEPTH]
    domain = ctx.obj[CTX_DOMAIN]
    project = ctx.obj[CTX_PROJECT]

    # retrieve remote
    remote = get_and_save_remote_with_click_context(ctx, project, domain)
    # retrieve workflow execution metrics
    workflow_execution_id = cli_identifiers.WorkflowExecutionIdentifier(
        project=project, domain=domain, name=execution_id
    )
    execution_metrics = remote.get_execution_metrics(id=workflow_execution_id, depth=depth)
    execution_metrics.dump()


@click.command("explain", help=_explain_help)
@click.argument("execution_id", type=str)
@click.pass_context
def metrics_explain(
    ctx: click.Context,
    execution_id: str,
):
    depth = ctx.obj[CTX_DEPTH]
    domain = ctx.obj[CTX_DOMAIN]
    project = ctx.obj[CTX_PROJECT]

    # retrieve remote
    remote = get_and_save_remote_with_click_context(ctx, project, domain)
    # retrieve workflow execution metrics
    workflow_execution_id = cli_identifiers.WorkflowExecutionIdentifier(
        project=project, domain=domain, name=execution_id
    )
    execution_metrics = remote.get_execution_metrics(id=workflow_execution_id, depth=depth)
    execution_metrics.explain()


metrics.add_command(metrics_dump)
metrics.add_command(metrics_explain)
