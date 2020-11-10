import os as _os
from typing import List as _List

import click

from flytekit.clis.sdk_in_container.constants import CTX_DOMAIN, CTX_PACKAGES, CTX_PROJECT, CTX_TEST
from flytekit.common import utils as _utils
from flytekit.common.core import identifier as _identifier
from flytekit.common.tasks import task as _task
from flytekit.configuration import sdk as _sdk_config
from flytekit.models.core import identifier as _identifier_model
from flytekit.tools.fast_registration import compute_digest as _compute_digest
from flytekit.tools.fast_registration import get_additional_distribution_loc as _get_additional_distribution_loc
from flytekit.tools.fast_registration import upload_package as _upload_package
from flytekit.tools.module_loader import iterate_registerable_entities_in_order


def fast_register_all(project: str, domain: str, pkgs: _List[str], test: bool, version: str, source_dir):
    if test:
        click.echo("Test switch enabled, not doing anything...")

    if not version:
        digest = _compute_digest(source_dir)
    else:
        digest = version
    remote_package_path = _upload_package(source_dir, digest, _sdk_config.FAST_REGISTRATION_DIR.get())

    click.echo(
        "Running task, workflow, and launch plan fast registration for {}, {}, {} with version {} and code dir {}".format(
            project, domain, pkgs, digest, source_dir
        )
    )

    # m = module (i.e. python file)
    # k = value of dir(m), type str
    # o = object (e.g. SdkWorkflow)
    for m, k, o in iterate_registerable_entities_in_order(pkgs):
        name = _utils.fqdn(m.__name__, k, entity_type=o.resource_type)
        o._id = _identifier.Identifier(o.resource_type, project, domain, name, digest)

        if test:
            click.echo("Would fast register {:20} {}".format("{}:".format(o.entity_type_text), o.id.name))
        else:
            click.echo("Fast registering {:20} {}".format("{}:".format(o.entity_type_text), o.id.name))
            _get_additional_distribution_loc(_sdk_config.FAST_REGISTRATION_DIR.get(), digest)
            if o.resource_type == _identifier_model.ResourceType.TASK:
                o.fast_register(project, domain, o.id.name, digest, remote_package_path)
            else:
                o.register(project, domain, o.id.name, digest)


def fast_register_tasks_only(
    project: str, domain: str, pkgs: _List[str], test: bool, version: str, source_dir: _os.PathLike
):
    if test:
        click.echo("Test switch enabled, not doing anything...")

    if not version:
        digest = _compute_digest(source_dir)
    else:
        digest = version
    remote_package_path = _upload_package(source_dir, digest, _sdk_config.FAST_REGISTRATION_DIR.get())

    click.echo(
        "Running task only fast registration for {}, {}, {} with version {} and code dir {}".format(
            project, domain, pkgs, digest, source_dir
        )
    )

    # Discover all tasks by loading the module
    for m, k, t in iterate_registerable_entities_in_order(pkgs, include_entities={_task.SdkTask}):
        name = _utils.fqdn(m.__name__, k, entity_type=t.resource_type)

        if test:
            click.echo("Would fast register task {:20} {}".format("{}:".format(t.entity_type_text), name))
        else:
            click.echo("Fast registering task {:20} {}".format("{}:".format(t.entity_type_text), name))
            t.fast_register(project, domain, name, digest, remote_package_path)


@click.group("fast-register")
@click.option("--test", is_flag=True, help="Dry run, do not actually register with Admin")
@click.pass_context
def fast_register(ctx, test=None):
    """
    Run fast registration steps for the Flyte entities in this container. This is an optimization to avoid the
    conventional container build and upload cycle. This can be useful for fast iteration when making code changes.
    If you do need to change the container itself (e.g. by adding a new dependency/import) you must rebuild and
    upload a container.

    Caveats: Your flyte config must specify a fast registration dir like so:
    [sdk]
    fast_registration_dir=s3://my-s3-bucket/dir

    **and** ensure that the role specified in [auth] section of your config has read access to this remote location.
    Furthermore, the role you assume to call fast-register must have **write** permission to this remote location.

    Run with the --test switch for a dry run to see what will be registered.  A default launch plan will also be
    created, if a role can be found in the environment variables.
    """

    ctx.obj[CTX_TEST] = test


@click.command("tasks")
@click.option(
    "--source-dir",
    type=str,
    help="The root dir of the code that should be uploaded for fast registration.",
    required=True,
)
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to register tasks with. This is normally computed deterministically from your code, "
    "but you can override here.",
)
@click.pass_context
def tasks(ctx, source_dir, version=None):
    """
    Only fast register tasks.

    For example, consider a sample directory where tasks defined in workflows/ imports code from util/ like so:

    \b
    $ tree /root/code/
    /root/code/
    ├── Dockerfile
    ├── Makefile
    ├── README.md
    ├── conf.py
    ├── notebook.config
    ├── workflows
    │   ├── __init__.py
    │   ├── compose
    │   │   ├── README.md
    │   │   ├── __init__.py
    │   │   ├── a_workflow.py
    │   │   ├── b_workflow.py
    ├── util
    │   ├── __init__.py
    │   ├── shared_task_code.py
    ├── requirements.txt
    ├── flyte.config

    Your source dir will need to be /root/code/ rather than the workflow packages dir /root/code/workflows you might
    have specified in your flyte.config because all of the code your workflows depends on needs to be encapsulated in
    `source_dir`, like so:

    pyflyte -p myproject -d development fast-register tasks --source-dir /root/code/

    """
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    test = ctx.obj[CTX_TEST]
    pkgs = ctx.obj[CTX_PACKAGES]

    fast_register_tasks_only(project, domain, pkgs, test, version, source_dir)


@click.command("workflows")
@click.option(
    "--source-dir",
    type=str,
    help="The root dir of the code that should be uploaded for fast registration.",
    required=True,
)
@click.option(
    "-v",
    "--version",
    type=str,
    help="Version to register entities with. This is normally computed deterministically from your code, "
    "but you can override here.",
)
@click.pass_context
def workflows(ctx, source_dir, version=None):
    """
    Fast register both tasks and workflows.  Also create and register a default launch plan for all workflows.
    The `source_dir` param should point to the root directory of your project that contains all of your working code.

    For example, consider a sample directory structure where code in workflows/ imports code from util/ like so:

    \b
    $ tree /root/code/
    /root/code/
    ├── Dockerfile
    ├── Makefile
    ├── README.md
    ├── conf.py
    ├── notebook.config
    ├── workflows
    │   ├── __init__.py
    │   ├── compose
    │   │   ├── README.md
    │   │   ├── __init__.py
    │   │   ├── a_workflow.py
    │   │   ├── b_workflow.py
    ├── util
    │   ├── __init__.py
    │   ├── shared_workflow_code.py
    ├── requirements.txt
    ├── flyte.config

    Your source dir will need to be /root/code/ rather than the workflow packages dir /root/code/workflows you might
    have specified in your flyte.config because all of the code your workflows depends on needs to be encapsulated in
    `source_dir`, like so:

    pyflyte -p myproject -d development fast-register workflows --source-dir /root/code/
    """
    project = ctx.obj[CTX_PROJECT]
    domain = ctx.obj[CTX_DOMAIN]
    test = ctx.obj[CTX_TEST]
    pkgs = ctx.obj[CTX_PACKAGES]

    fast_register_all(project, domain, pkgs, test, version, source_dir)


fast_register.add_command(tasks)
fast_register.add_command(workflows)
