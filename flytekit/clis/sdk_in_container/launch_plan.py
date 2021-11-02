import logging as _logging
import os as _os

import click
import six as _six

from flytekit.clis.helpers import construct_literal_map_from_parameter_map as _construct_literal_map_from_parameter_map
from flytekit.clis.sdk_in_container import constants as _constants
from flytekit.clis.sdk_in_container.constants import (
    CTX_DOMAIN,
    CTX_PROJECT,
    CTX_VERSION,
    domain_option,
    project_option,
    version_option,
)
from flytekit.common import utils as _utils
from flytekit.common.launch_plan import SdkLaunchPlan as _SdkLaunchPlan
from flytekit.configuration.internal import DOMAIN as _DOMAIN
from flytekit.configuration.internal import IMAGE as _IMAGE
from flytekit.configuration.internal import PROJECT as _PROJECT
from flytekit.configuration.internal import VERSION as _VERSION
from flytekit.configuration.internal import look_up_version_from_image_tag as _look_up_version_from_image_tag
from flytekit.models import launch_plan as _launch_plan_model
from flytekit.models.core import identifier as _identifier
from flytekit.tools.module_loader import iterate_registerable_entities_in_order


class LaunchPlanAbstractGroup(click.Group):
    """
    This class iterates over the workflow folders and loads all workflows that are implemented via the programming
    model.
    """

    def __init__(self, name, **attrs):
        super(LaunchPlanAbstractGroup, self).__init__(name, commands=None, **attrs)

    def list_commands(self, ctx):
        commands = []
        lps = {}
        pkgs = ctx.obj[_constants.CTX_PACKAGES]
        # Discover all launch plans by loading the modules
        for m, k, lp in iterate_registerable_entities_in_order(
            pkgs, include_entities={_SdkLaunchPlan}, detect_unreferenced_entities=False
        ):
            safe_name = _utils.fqdn(m.__name__, k, entity_type=lp.resource_type)
            commands.append(safe_name)
            lps[safe_name] = lp

        ctx.obj["lps"] = lps
        commands.sort()

        return commands

    def get_command(self, ctx, lp_argument):
        # Get the launch plan object in one of two ways. If get_command is being called by the list function
        # then it should have been cached in the context.
        # If we are actually running the command, then it won't have been cached and we'll have to load everything again
        launch_plan = None
        pkgs = ctx.obj[_constants.CTX_PACKAGES]

        if "lps" in ctx.obj:
            launch_plan = ctx.obj["lps"][lp_argument]
        else:
            for m, k, lp in iterate_registerable_entities_in_order(
                pkgs,
                include_entities={_SdkLaunchPlan},
                detect_unreferenced_entities=False,
            ):
                safe_name = _utils.fqdn(m.__name__, k, entity_type=lp.resource_type)
                if lp_argument == safe_name:
                    launch_plan = lp

        if launch_plan is None:
            raise Exception("Could not load launch plan {}".format(lp_argument))

        launch_plan._id = _identifier.Identifier(
            _identifier.ResourceType.LAUNCH_PLAN,
            ctx.obj[_constants.CTX_PROJECT],
            ctx.obj[_constants.CTX_DOMAIN],
            lp_argument,
            ctx.obj[_constants.CTX_VERSION],
        )
        return self._get_command(ctx, launch_plan, lp_argument)

    def _get_command(self, ctx, lp, cmd_name):
        """
        :param ctx:
        :param flytekit.common.launch_plan.SdkLaunchPlan lp:
        :rtype: click.Command
        """
        pass


class LaunchPlanExecuteGroup(LaunchPlanAbstractGroup):
    def _get_command(self, ctx, lp, cmd_name):
        """
        This function returns the function that click will actually use to execute a specific launch plan.  It also
        stores the launch plan python object and the command name in the closure.

        :param ctx:
        :param flytekit.common.launch_plan.SdkLaunchPlan lp:
        :param Text cmd_name: The name of the launch plan, as passed in from the abstract class
        """

        def _execute_lp(**kwargs):
            for input_name in _six.iterkeys(kwargs):
                if isinstance(kwargs[input_name], tuple):
                    kwargs[input_name] = list(kwargs[input_name])

            inputs = _construct_literal_map_from_parameter_map(lp.default_inputs, kwargs)
            execution = lp.execute_with_literals(
                ctx.obj[_constants.CTX_PROJECT],
                ctx.obj[_constants.CTX_DOMAIN],
                literal_inputs=inputs,
                notification_overrides=ctx.obj.get(_constants.CTX_NOTIFICATIONS, None),
            )
            click.echo(
                click.style(
                    "Workflow scheduled, execution_id={}".format(_six.text_type(execution.id)),
                    fg="blue",
                )
            )

        command = click.Command(name=cmd_name, callback=_execute_lp)

        # Iterate through the workflow's inputs
        for var_name in sorted(lp.default_inputs.parameters):
            param = lp.default_inputs.parameters[var_name]
            # TODO: Figure out how to better handle the fact that we want strings to parse,
            # but we probably shouldn't have click say that that's the type on the CLI.
            help_msg = "{} Type: {}".format(
                _six.text_type(param.var.description), _six.text_type(param.var.type)
            ).strip()

            if param.required:
                # If it's a required input, add the required flag
                wrapper = click.option(
                    "--{}".format(var_name),
                    required=True,
                    type=_six.text_type,
                    help=help_msg,
                )
            else:
                # If it's not a required input, it should have a default
                # Use to_python_std so that the text of the default ends up being parseable, if not, the click
                # arg would look something like 'Integer(10)'. If the user specified '11' on the cli, then
                # we'd get '11' and then we'd need annoying logic to differentiate between the default text
                # and user text.
                default = param.default.to_python_std()
                wrapper = click.option(
                    "--{}".format(var_name),
                    default="{}".format(_six.text_type(default)),
                    type=_six.text_type,
                    help="{}. Default: {}".format(help_msg, _six.text_type(default)),
                )

            command = wrapper(command)

        return command


@click.group("lp")
@project_option
@domain_option
@version_option
@click.pass_context
def launch_plans(ctx, project, domain, version):
    """
    Launch plan control group, including executions
    """

    version = version or _look_up_version_from_image_tag(_IMAGE.get())
    if not version:
        raise click.UsageError("Could not find image from config, please specify a value for ``--version``")

    ctx.obj[CTX_PROJECT] = project
    ctx.obj[CTX_DOMAIN] = domain
    ctx.obj[CTX_VERSION] = version
    _os.environ[_PROJECT.env_var] = project
    _os.environ[_DOMAIN.env_var] = domain
    _os.environ[_VERSION.env_var] = version


@click.group("execute", cls=LaunchPlanExecuteGroup)
@click.pass_context
def execute_launch_plan(ctx):
    """
    Execute launch plans found in this container
    """
    pass


def activate_all_impl(project, domain, version, pkgs, ignore_schedules=False):
    # TODO: This should be a transaction to ensure all or none are updated
    # TODO: We should optionally allow deactivation of missing launch plans

    # Discover all launch plans by loading the modules
    _logging.info(f"Setting this version's {version} launch plans active in {project} {domain}")
    for m, k, lp in iterate_registerable_entities_in_order(
        pkgs, include_entities={_SdkLaunchPlan}, detect_unreferenced_entities=False
    ):
        lp._id = _identifier.Identifier(
            _identifier.ResourceType.LAUNCH_PLAN,
            project,
            domain,
            _utils.fqdn(m.__name__, k, entity_type=lp.resource_type),
            version,
        )
        if not (lp.is_scheduled and ignore_schedules):
            _logging.info(f"Setting active {_utils.fqdn(m.__name__, k, entity_type=lp.resource_type)}")
            lp.update(_launch_plan_model.LaunchPlanState.ACTIVE)


@click.command("activate-all-schedules")
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to register tasks with. This is normally parsed from the" "image, but you can override here.",
)
@click.pass_context
def activate_all_schedules(ctx, version=None):
    """
    THIS COMMAND IS DEPRECATED. PLEASE USE activate-all

    The behavior of this command is identical to activate-all.
    """
    click.secho(
        "activate-all-schedules is deprecated, please use activate-all instead.",
        color="yellow",
    )
    project = ctx.obj[_constants.CTX_PROJECT]
    domain = ctx.obj[_constants.CTX_DOMAIN]
    pkgs = ctx.obj[_constants.CTX_PACKAGES]
    version = version or ctx.obj[_constants.CTX_VERSION] or _look_up_version_from_image_tag(_IMAGE.get())
    activate_all_impl(project, domain, version, pkgs)


@click.command("activate-all")
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to register tasks with. This is normally parsed from the" "image, but you can override here.",
)
@click.option(
    "--ignore-schedules",
    is_flag=True,
    help="Activate all except for launch plans with schedules.",
)
@click.pass_context
def activate_all(ctx, version=None, ignore_schedules=False):
    """
    This command will activate all found launch plans at the given version.  If there are existing
    active launch plans that collide on project, domain, and name, but differ on version, those will be
    deactivated in favor of the version specified in this command. If a launch plan is associated with a schedule,
    the schedule will also be deactivated or activated as appropriate.

    Note:
        1.  Currently, this is not a transaction.  Therefore, if the command fails, it is possible that some schedules
            have been updated.
        2.  If a launch plan is scheduled on an older version for a given project, domain, and name AND there is not a
            matching scheduled launch plan found when running this command, the existing schedule will remain active
            until it is manually disabled.
    """
    project = ctx.obj[_constants.CTX_PROJECT]
    domain = ctx.obj[_constants.CTX_DOMAIN]
    pkgs = ctx.obj[_constants.CTX_PACKAGES]
    version = version or ctx.obj[_constants.CTX_VERSION] or _look_up_version_from_image_tag(_IMAGE.get())
    activate_all_impl(project, domain, version, pkgs, ignore_schedules=ignore_schedules)


launch_plans.add_command(execute_launch_plan)
launch_plans.add_command(activate_all_schedules)
launch_plans.add_command(activate_all)
