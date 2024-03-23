import rich_click as click

from flytekit.clis.sdk_in_container.constants import CTX_DOMAIN, CTX_PROJECT
from flytekit.clis.sdk_in_container.helpers import FLYTE_REMOTE_INSTANCE_KEY, get_and_save_remote_with_click_context
from flytekit.clis.sdk_in_container.utils import (
    domain_option_dec,
    project_option_dec,
)
from flytekit.interfaces import cli_identifiers
from flytekit.remote import FlyteRemote

EXECUTION_ID = "execution_id"


@click.command("relaunch", help="Relaunch a failed execution")
@click.pass_context
def relaunch(ctx: click.Context):
    """
    Relaunches an execution.
    """
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    remote: FlyteRemote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]
    execution_identifier = cli_identifiers.WorkflowExecutionIdentifier(
        project=project, domain=domain, name=ctx.obj[EXECUTION_ID]
    )
    execution_identifier_resp = remote.client.relaunch_execution(id=execution_identifier)
    execution_identifier = cli_identifiers.WorkflowExecutionIdentifier.promote_from_model(execution_identifier_resp)
    click.secho("Launched execution: {}".format(execution_identifier), fg="blue")
    click.echo("")


@click.command("recover", help="Recover a failed execution")
@click.pass_context
def recover(ctx: click.Context):
    """
    Recovers an execution.
    """
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    remote: FlyteRemote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]
    execution_identifier = cli_identifiers.WorkflowExecutionIdentifier(
        project=project, domain=domain, name=ctx.obj[EXECUTION_ID]
    )
    execution_identifier_resp = remote.client.recover_execution(id=execution_identifier)
    execution_identifier = cli_identifiers.WorkflowExecutionIdentifier.promote_from_model(execution_identifier_resp)
    click.secho("Launched execution: {}".format(execution_identifier), fg="blue")
    click.echo("")


execution_help = """
The execution command allows you to interact with Flyte's execution system,
such as recovering/relaunching a failed execution.
"""


@click.group("execution", help=execution_help)
@project_option_dec
@domain_option_dec
@click.option(
    EXECUTION_ID,
    "--execution-id",
    required=True,
    type=str,
    help="The execution id",
)
@click.pass_context
def execute(
    ctx: click.Context,
    project: str,
    domain: str,
    execution_id: str,
):
    # save remote instance in ctx.obj
    get_and_save_remote_with_click_context(ctx, project, domain)
    if ctx.obj is None:
        ctx.obj = {}

    ctx.obj.update(ctx.params)


execute.add_command(recover)
execute.add_command(relaunch)
