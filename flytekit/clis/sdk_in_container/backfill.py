import typing
from datetime import datetime, timedelta

import click
from croniter import croniter

from flytekit import Workflow, LaunchPlan, CronSchedule
from flytekit.clis.sdk_in_container.helpers import FLYTE_REMOTE_INSTANCE_KEY
from flytekit.clis.sdk_in_container.run import DurationParamType
from flytekit.remote import FlyteRemote, FlyteLaunchPlan

_backfill_help = """
Backfill command generates, registers a new workflow based on the input launchplan, that can be used to run an
automated backfill. The workflow can be managed using the Flyte UI and can be canceled, relaunched, recovered and
is implicitly cached. 
"""


def create_backfiller_for(start_date: datetime, end_date: datetime, for_lp: typing.Union[LaunchPlan, FlyteLaunchPlan],
                          serial: bool = False, per_node_timeout: timedelta = None,
                          per_node_retries: int = 0) -> Workflow:
    """
    Generates a new imperative workflow for the launchplan that can be used to backfill the given launchplan.
    This can only be used to generate  backfilling workflow only for schedulable launchplans
    """
    if not for_lp:
        raise RuntimeError("Launch plan is required!")

    if start_date >= end_date:
        raise ValueError(
            f"for a backfill start date should be earlier than end date. Received {start_date} -> {end_date}")

    schedule = for_lp.entity_metadata.schedule if isinstance(for_lp, FlyteLaunchPlan) else for_lp.schedule

    if schedule is None:
        raise ValueError("Backfill can only be created for scheduled launch plans")

    if schedule.cron_schedule is not None:
        cron_schedule = schedule.cron_schedule
    else:
        raise NotImplementedError("Currently backfilling only supports cron schedules.")

    click.secho(f"Generating backfill from {start_date} -> {end_date}", fg="yellow")
    wf = Workflow(name=f"backfill-{for_lp.name}")
    date_iter = croniter(cron_schedule.schedule, start_time=start_date, ret_type=datetime)
    prev_node = None
    while True:
        next_start_date = date_iter.get_next()
        if next_start_date >= end_date:
            break
        next_node = wf.add_launch_plan(for_lp, t=next_start_date)
        next_node = next_node.with_overrides(name=f"b-{next_start_date}", retries=per_node_retries,
                                             timeout=per_node_timeout)
        if serial and prev_node:
            prev_node.runs_before(next_node)
            click.secho(f"\r -> {next_node.name}")
        else:
            click.secho(f"  -> {next_node.name}")
        prev_node = next_node

    return wf


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
    type=click.DateTime(),
    default=None,
    help="Date from which the backfill should begin. Start date is inclusive.",
)
@click.option(
    "--to-date",
    required=False,
    type=click.DateTime(),
    default=None,
    help="Date to which the backfill should run_until. End date is inclusive",
)
@click.option(
    "--duration",
    required=False,
    type=DurationParamType(),
    callback=DurationParamType().convert,
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
        raise click.BadParameter("One of start-date and end-date are needed with duration")
    if from_date and to_date:
        pass
    elif not duration:
        raise click.BadParameter("One of start-date and end-date are needed with duration")
    elif from_date:
        to_date = from_date + duration
    else:
        from_date = to_date - duration

    remote: FlyteRemote = ctx.obj[FLYTE_REMOTE_INSTANCE_KEY]
    lp = remote.fetch_launch_plan(project=project, domain=domain, name=launchplan, version=launchplan_version)
    create_backfiller_for(start_date=from_date, end_date=to_date, for_lp=lp, serial=not parallel)
