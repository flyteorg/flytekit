import logging as _logging

import click

from flytekit.clis.sdk_in_container.constants import CTX_DOMAIN, CTX_PACKAGES, CTX_PROJECT, CTX_TEST, CTX_VERSION
from flytekit.common import utils as _utils
from flytekit.common.core import identifier as _identifier
from flytekit.common.tasks import task as _task
from flytekit.configuration.internal import IMAGE as _IMAGE
from flytekit.configuration.internal import look_up_version_from_image_tag as _look_up_version_from_image_tag
from flytekit.tools.module_loader import iterate_registerable_entities_in_order


def register_all(project, domain, pkgs, test, version):
    if test:
        click.echo("Test switch enabled, not doing anything...")
    click.echo(
        "Running task, workflow, and launch plan registration for {}, {}, {} with version {}".format(
            project, domain, pkgs, version
        )
    )

    # m = module (i.e. python file)
    # k = value of dir(m), type str
    # o = object (e.g. SdkWorkflow)
    for m, k, o in iterate_registerable_entities_in_order(pkgs):
        name = _utils.fqdn(m.__name__, k, entity_type=o.resource_type)
        _logging.debug("Found module {}\n   K: {} Instantiated in {}".format(m, k, o._instantiated_in))
        o._id = _identifier.Identifier(o.resource_type, project, domain, name, version)

        if test:
            click.echo("Would register {:20} {}".format("{}:".format(o.entity_type_text), o.id.name))
        else:
            click.echo("Registering {:20} {}".format("{}:".format(o.entity_type_text), o.id.name))
            o.register(project, domain, o.id.name, version)


def register_tasks_only(project, domain, pkgs, test, version):
    if test:
        click.echo("Test switch enabled, not doing anything...")

    click.echo("Running task only registration for {}, {}, {} with version {}".format(project, domain, pkgs, version))

    # Discover all tasks by loading the module
    for m, k, t in iterate_registerable_entities_in_order(pkgs, include_entities={_task.SdkTask}):
        name = _utils.fqdn(m.__name__, k, entity_type=t.resource_type)

        if test:
            click.echo("Would register task {:20} {}".format("{}:".format(t.entity_type_text), name))
        else:
            click.echo("Registering task {:20} {}".format("{}:".format(t.entity_type_text), name))
            t.register(project, domain, name, version)


@click.group("register")
# --pkgs on the register group is DEPRECATED, use same arg on pyflyte.main instead
@click.option(
    "--pkgs", multiple=True, help="DEPRECATED. This arg can only be used before the 'register' keyword",
)
@click.option("--test", is_flag=True, help="Dry run, do not actually register with Admin")
@click.pass_context
def register(ctx, pkgs=None, test=None):
    """
    Run registration steps for the workflows in this container.

    Run with the --test switch for a dry run to see what will be registered.  A default launch plan will also be
    created, if a role can be found in the environment variables.
    """
    if pkgs:
        raise click.UsageError("--pkgs must now be specified before the 'register' keyword on the command line")

    ctx.obj[CTX_TEST] = test


@click.command("tasks")
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to register tasks with. This is normally parsed from the" "image, but you can override here.",
)
@click.pass_context
def tasks(ctx, version=None):
    """
    Only register tasks.
    """
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    test = ctx.obj[CTX_TEST]
    pkgs = ctx.obj[CTX_PACKAGES]

    version = version or ctx.obj[CTX_VERSION] or _look_up_version_from_image_tag(_IMAGE.get())
    register_tasks_only(project, domain, pkgs, test, version)


@click.command("workflows")
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to register tasks with. This is normally parsed from the" "image, but you can override here.",
)
@click.pass_context
def workflows(ctx, version=None):
    """
    Register both tasks and workflows.  Also create and register a default launch plan for all workflows.
    """
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    test = ctx.obj[CTX_TEST]
    pkgs = ctx.obj[CTX_PACKAGES]

    version = version or ctx.obj[CTX_VERSION] or _look_up_version_from_image_tag(_IMAGE.get())
    register_all(project, domain, pkgs, test, version)


register.add_command(tasks)
register.add_command(workflows)
