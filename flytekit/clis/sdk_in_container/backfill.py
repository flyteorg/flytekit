from datetime import datetime, timedelta

import click

from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context
from flytekit.clis.sdk_in_container.run import DurationParamType, DateTimeType
from flytekit.remote import FlyteRemote

_backfill_help = """
Backfill command generates, registers a new workflow based on the input launchplan, that can be used to run an
automated backfill. The workflow can be managed using the Flyte UI and can be canceled, relaunched, recovered and
is implicitly cached. 
"""


@click.command("backfill", help=_backfill_help)
@click.option(
    "-p",
    "--project",
    required=False,
    type=str,
    default="flytesnacks",
    help="Project to register and run this workflow in",
)
@click.option(
    "-d",
    "--domain",
    required=False,
    type=str,
    default="development",
    help="Domain to register and run this workflow in",
)
@click.option(
    "--dry-run",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Just generate the workflow - do not register or execute",
)
@click.option(
    "--parallel/--serial",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="All backfill can be run in parallel - with max-parallelism",
)
@click.option(
    "--no-execute",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    show_default=True,
    help="Generate the workflow and register, do not execute",
)
@click.option(
    "--from-date",
    required=False,
    type=DateTimeType(),
    default=None,
    help="Date from which the backfill should begin. Start date is inclusive.",
)
@click.option(
    "--to-date",
    required=False,
    type=DateTimeType(),
    default=None,
    help="Date to which the backfill should run_until. End date is inclusive",
)
@click.option(
    "--duration",
    required=False,
    type=DurationParamType(),
    default=None,
    help="Timedelta for number of days, minutes hours given either a from-date or end-date to compute the"
         " backfills between",
)
@click.argument(
    "launchplan",
    required=True,
    type=str,
    # help="Name of launchplan to be backfilled.",
)
@click.argument(
    "launchplan-version",
    required=False,
    type=str,
    default=None,
    # help="Version of the launchplan to be backfilled, if not specified, the latest version "
    #      "(by registration time) will be used",
)
@click.pass_context
def backfill(ctx: click.Context, project: str, domain: str, from_date: datetime, to_date: datetime,
             duration: timedelta, launchplan: str, launchplan_version: str,
             dry_run: bool, no_execute: bool, parallel: bool):
    if from_date and to_date and duration:
        raise click.BadParameter("Cannot use from-date, to-date and duration. Use any two")
    if not (from_date or to_date):
        raise click.BadParameter(
            "One of following pairs are required -> (from-date, to-date) | (from-date, duration) | (to-date, duration)")
    if from_date and to_date:
        pass
    elif not duration:
        raise click.BadParameter("One of start-date and end-date are needed with duration")
    elif from_date:
        to_date = from_date + duration
    else:
        from_date = to_date - duration

    remote = get_and_save_remote_with_click_context(ctx, project, domain)
    lp = remote.fetch_launch_plan(project=project, domain=domain, name=launchplan, version=launchplan_version)
    wf = FlyteRemote.create_backfiller(start_date=from_date, end_date=to_date, for_lp=lp, parallel=parallel,
                                       output=click.secho)
