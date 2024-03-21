import rich_click as click
from flytekit.clis.sdk_in_container.utils import (
    domain_option_dec,
    project_option_dec,
)
from flytekit.interfaces import cli_identifiers
from flytekit.clis.sdk_in_container.constants import CTX_DOMAIN, CTX_PROJECT
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context

EXECUTION_ID = "execution_id"


@click.command("recover", help="Recover a failed execution")
@click.pass_context
def recover(ctx: click.Context):
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    remote = get_and_save_remote_with_click_context(ctx, project, domain)
    execution_identifier = cli_identifiers.WorkflowExecutionIdentifier(
        project=project, domain=domain, name=ctx.obj[EXECUTION_ID]
    )
    execution_identifier_resp = remote.client.recover_execution(id=execution_identifier)
    execution_identifier = cli_identifiers.WorkflowExecutionIdentifier.promote_from_model(execution_identifier_resp)
    click.secho("Launched execution: {}".format(execution_identifier), fg="blue")
    click.echo("")


execution_help = """
The execution command allows you to interact with Flyte's execution system. You can
recover executions.
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
    if ctx.obj is None:
        ctx.obj = {}
    ctx.obj.update(ctx.params)
    
execute.add_command(recover)