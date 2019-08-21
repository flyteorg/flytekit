from __future__ import absolute_import
from __future__ import print_function

import click

from flytekit.clis.sdk_in_container.constants import CTX_PACKAGES, CTX_PROJECT, CTX_DOMAIN, CTX_VERSION
from flytekit.common import workflow as _workflow, utils as _utils
from flytekit.common.exceptions.scopes import system_entry_point
from flytekit.common.tasks import task as _sdk_task
from flytekit.common.utils import write_proto_to_file as _write_proto_to_file
from flytekit.configuration import TemporaryConfiguration
from flytekit.configuration.internal import CONFIGURATION_PATH
from flytekit.configuration.internal import IMAGE as _IMAGE
from flytekit.models.workflow_closure import WorkflowClosure as _WorkflowClosure
from flytekit.tools.module_loader import iterate_registerable_entities_in_order


@system_entry_point
def serialize_tasks(pkgs):
    # Serialize all tasks
    for m, k, t in iterate_registerable_entities_in_order(pkgs, include_entities={_sdk_task.SdkTask}):
        fname = '{}.pb'.format(_utils.fqdn(m.__name__, k, entity_type=t.resource_type))
        click.echo('Writing task {} to {}'.format(t.id, fname))
        pb = t.to_flyte_idl()
        _write_proto_to_file(pb, fname)


@system_entry_point
def serialize_workflows(pkgs):
    # Create map to look up tasks by their unique identifier.  This is so we can compile them into the workflow closure.
    tmap = {}
    for _, _, t in iterate_registerable_entities_in_order(pkgs, include_entities={_sdk_task.SdkTask}):
        tmap[t.id] = t

    for m, k, w in iterate_registerable_entities_in_order(pkgs, include_entities={_workflow.SdkWorkflow}):
        click.echo('Serializing {}'.format(_utils.fqdn(m.__name__, k, entity_type=w.resource_type)))
        task_templates = []
        for n in w.nodes:
            if n.task_node is not None:
                task_templates.append(tmap[n.task_node.reference_id])

        wc = _WorkflowClosure(workflow=w, tasks=task_templates)
        wc_pb = wc.to_flyte_idl()

        fname = '{}.pb'.format(_utils.fqdn(m.__name__, k, entity_type=w.resource_type))
        click.echo('  Writing workflow closure {}'.format(fname))
        _write_proto_to_file(wc_pb, fname)


@click.group('serialize')
@click.pass_context
def serialize(ctx):
    """
    This command produces protobufs for tasks and templates.
    For tasks, one pb file is produced for each task, representing one TaskTemplate object.
    For workflows, one pb file is produced for each workflow, representing a WorkflowClosure object.  The closure
        object contains the WorkflowTemplate, along with the relevant tasks for that workflow.  In lieu of Admin,
        this serialization step will set the URN of the tasks to the fully qualified name of the task function.
    """
    click.echo('Serializing Flyte elements with image {}'.format(_IMAGE.get()))


@click.command('tasks')
@click.pass_context
def tasks(ctx):
    pkgs = ctx.obj[CTX_PACKAGES]
    internal_settings = {
        'project': ctx.obj[CTX_PROJECT],
        'domain': ctx.obj[CTX_DOMAIN],
        'version': ctx.obj[CTX_VERSION]
    }
    # Populate internal settings for project/domain/version from the environment so that the file names are resolved
    # with the correct strings.  The file itself doesn't need to change though.
    with TemporaryConfiguration(CONFIGURATION_PATH.get(), internal_settings):
        serialize_tasks(pkgs)


@click.command('workflows')
@click.pass_context
def workflows(ctx):
    pkgs = ctx.obj[CTX_PACKAGES]
    internal_settings = {
        'project': ctx.obj[CTX_PROJECT],
        'domain': ctx.obj[CTX_DOMAIN],
        'version': ctx.obj[CTX_VERSION]
    }
    # Populate internal settings for project/domain/version from the environment so that the file names are resolved
    # with the correct strings.  The file itself doesn't need to change though.
    with TemporaryConfiguration(CONFIGURATION_PATH.get(), internal_settings):
        serialize_workflows(pkgs)


serialize.add_command(tasks)
serialize.add_command(workflows)
