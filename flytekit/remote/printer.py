"""
This module provides the ``FlyteRemote`` object, which is the end-user's main starting point for interacting
with a Flyte backend in an interactive and programmatic way. This of this experience as kind of like the web UI
but in Python object form.
"""
from __future__ import annotations

import textwrap
import typing
from typing import TYPE_CHECKING

from flytekit.models import schedule, interface as interface_models
from flytekit.models.admin import common as admin_common_models
from flytekit.models.core import execution as execution_models
from flytekit.models.core import identifier as identifier_models


MAX_OFFSET = 20

from flytekit.models import literals as literal_models
from flytekit.models.execution import (
    NodeExecutionGetDataResponse,
    WorkflowExecutionGetDataResponse,
)
if TYPE_CHECKING:
    from flytekit.remote.executions import FlyteWorkflowExecution
    from flytekit.remote.nodes import FlyteNode
    from flytekit.remote.task import FlyteTask
    from flytekit.remote.workflow import FlyteWorkflow
    from flytekit.remote.interface import TypedInterface
    from flytekit.remote.launch_plan import FlyteLaunchPlan

ExecutionDataResponse = typing.Union[WorkflowExecutionGetDataResponse, NodeExecutionGetDataResponse]

MOST_RECENT_FIRST = admin_common_models.Sort("created_at", admin_common_models.Sort.Direction.DESCENDING)


def spaces(i: int) -> str:
    return " " * i


def render_identifier(fid: identifier_models.Identifier) -> str:
    part = f"([{fid.project}/{fid.domain}] {fid.name}:{fid.version})"
    if fid.resource_type == identifier_models.ResourceType.TASK:
        return "Task: " + part
    if fid.resource_type == identifier_models.ResourceType.WORKFLOW:
        return "Workflow: " + part
    if fid.resource_type == identifier_models.ResourceType.LAUNCH_PLAN:
        return "Launch Plan: " + part
    return "Unspecified: " + part


def render_parameter_map(pmap: interface_models.ParameterMap, indent: int = 0):
    if not pmap.parameters:
        return "{}"

    offset = min(MAX_OFFSET, max([len(x) for x in pmap.parameters.keys()]))
    res = []
    for (
        k,
        v,
    ) in pmap.parameters.items():
        padding = max(0, offset - len(k))
        r = f"{k}: " + " " * padding + str(v)
        res.append(r)
    combined = "\n".join(res)
    return textwrap.indent(combined, " " * indent)


def render_typed_interface(iface: TypedInterface) -> str:
    ins = []
    for k, v in iface.inputs.items():
        ins.append(f"{k}: {v.type}")
    instr = "\n".join(ins)
    instr = textwrap.indent(instr, " " * 2)
    outs = []
    for k, v in iface.outputs.items():
        outs.append(f"{k}: {v.type}")
    outstr = "\n".join(outs)
    outstr = textwrap.indent(outstr, " " * 2)
    return f"Inputs:\n{instr}\nOutputs:\n{outstr}\n"


def render_binding_data(bd: literal_models.BindingData) -> str:
    if bd.scalar:
        return f"Scalar: {str(bd.scalar)}"
    if bd.promise:
        return f"{bd.promise.var} of node {bd.promise.node_id}"
    if bd.collection:
        entries = [render_binding_data(x) for x in bd.collection.bindings]
        indented_entries = textwrap.indent("".join(entries), " " * 2)
        return f"[\n{indented_entries}]"
    if bd.map:
        entries = []
        for k, v in bd.map.bindings.items():
            entry = f"{k}:\n{textwrap.indent(render_binding_data(v), ' ' * 2)}"
            entries.append(entry)
        indented_entries = textwrap.indent("".join(entries), " " * 2)
        return "{\n" + indented_entries + "\n}"


def render_schedule_expr(sched: schedule.Schedule) -> str:
    sched_expr = "NONE"
    if sched and sched.cron_expression:
        sched_expr = f"cron({sched.cron_expression})"
    elif sched and sched.rate:
        sched_expr = "rate({unit}={value})".format(
            unit=schedule.Schedule.FixedRateUnit.enum_to_string(sched.rate.unit),
            value=str(sched.rate.value),
        )
    return "{:30}".format(sched_expr)


def render_id(fid: identifier_models.Identifier, indent: int = 0) -> str:
    if fid.resource_type == identifier_models.ResourceType.LAUNCH_PLAN:
        title = "Launch Plan:"
    elif fid.resource_type == identifier_models.ResourceType.TASK:
        title = "Task:"
    else:
        title = "Workflow:"

    x = f"""\
    {title}
      [{fid.project}/{fid.domain}]
      {fid.name}@{fid.version}
    """
    return textwrap.indent(textwrap.dedent(x), " " * indent)


def render_literal_map(lm: literal_models.LiteralMap, indent: int = 0) -> str:
    if not lm.literals:
        return "{}"
    offset = min(MAX_OFFSET, max([len(x) for x in lm.literals.keys()]))
    res = []
    for (
            k,
            v,
    ) in lm.literals.items():
        padding = max(0, offset - len(k))
        r = f"{k}: " + " " * padding + str(v)
        res.append(r)
    combined = "\n".join(res)
    return textwrap.indent(combined, " " * indent)


def render_flyte_node(node: FlyteNode) -> str:
    node_id = f"{node.id}:\n"
    binding_strs = []
    for b in node.inputs:
        binding_strs.append(b.var + ":\n" + textwrap.indent(str(b.binding), " " * 2))
    inputs_str = "Inputs:\n" + textwrap.indent("\n".join(binding_strs), " " * 2) + "\n"

    upstream_ids = (
        "Upstream nodes:\n  "
        + (", ".join([un.id for un in node.upstream_nodes]) if node.upstream_nodes else "None")
        + "\n"
    )
    executable = "Executable:\n" + textwrap.indent(str(node.flyte_entity), " " * 2)

    return node_id + textwrap.indent(inputs_str + upstream_ids + executable, " " * 2)


def render_flyte_task(task: FlyteTask) -> str:
    header = f"""\
    Task ID:
      [{task.id.project}/{task.id.domain}]
      {task.name}@{task.id.version}
    """
    header = textwrap.dedent(header)

    io = str(task.interface)
    io = f"Interface:\n{textwrap.indent(io, ' ' * 2) if io else '  None'}"

    container = task.container.image if task.container else ""

    return textwrap.dedent(header + io + container)


def render_flyte_launch_plan(flp: FlyteLaunchPlan) -> str:
    header = f"""\
    Launch Plan ID:
      [{flp.id.project}/{flp.id.domain}]
      {flp.name}@{flp.id.version}
      Workflow:
      [{flp.workflow_id.project}/{flp.workflow_id.domain}]
      {flp.workflow_id.name}@{flp.workflow_id.version}        
    """
    header = textwrap.dedent(header)

    schedule = f"Schedule: {flp.entity_metadata.schedule}" if flp.entity_metadata.schedule else ""
    notifies = f"Notifications: {flp.entity_metadata.notifications}" if flp.entity_metadata.notifications else ""
    labels = f"Labels: {str(flp.labels)}" if len(flp.labels.values) > 0 else ""
    annotate = f"Annotations: {str(flp.annotations)}" if len(flp.annotations.values) > 0 else ""

    data = f"Offloaded data location: {flp.raw_output_data_config.output_location_prefix or '(default)'}"
    iam = f"IAM Role: {flp.auth_role.assumable_iam_role}" if flp.auth_role.assumable_iam_role else ""
    svc = (
        f"Service Account: {flp.auth_role.kubernetes_service_account}"
        if flp.auth_role.kubernetes_service_account
        else ""
    )
    fixed = "Fixed inputs:" + (
        "\n" + render_literal_map(flp.fixed_inputs, indent=4) if len(flp.fixed_inputs.literals) > 0 else " None"
    )
    defaults = "Default inputs:" + (
        "\n" + render_parameter_map(flp.default_inputs, indent=4) if len(flp.default_inputs.parameters) > 0 else " None"
    )

    # Filter out empty strings
    entries = filter(
        lambda x: bool(x), [header, schedule, notifies, labels, annotate, data, iam, svc, fixed, defaults]
    )
    return textwrap.dedent("\n".join(entries))


def render_workflow_execution(wf_exec: FlyteWorkflowExecution) -> str:
    result = f"\nExecution {wf_exec.id.project}:{wf_exec.id.domain}:{wf_exec.id.name}\n"

    result += "\t{:12} ".format("State:") + \
              f"{execution_models.WorkflowExecutionPhase.enum_to_string(wf_exec.closure.phase):10} "

    result += render_id(wf_exec.spec.launch_plan, indent=6) + "\n"

    # Add rendering and display of uri
    # todo: fix raw inputs when removing guessing
    if wf_exec._raw_inputs is not None:
        result += "    Inputs:\n" + render_literal_map(wf_exec.raw_inputs.literals, 8)
    # Add output rendering and display of uri

    if wf_exec.closure.error is not None:
        result += render_error(wf_exec.closure.error)

    result += render_node_executions(wf_exec)

    return result


def render_flyte_workflow(fwf: FlyteWorkflow) -> str:
    header = f"""\
    Workflow ID:
      [{fwf.id.project}/{fwf.id.domain}]
      {fwf.name}@{fwf.id.version}
    """
    header = textwrap.dedent(header)

    io = render_typed_interface(fwf.interface)
    io = f"Interface:\n{textwrap.indent(io, ' ' * 2) if io else '  None'}"

    behavior = f"""\
    Failure Policy: {"Fail immediately" if not fwf.metadata.on_failure else "Wait for executable nodes"}
    Interruptible: {fwf.metadata_defaults.interruptible}
    """
    behavior = textwrap.dedent(behavior)

    binding_strs = []
    for b in fwf.outputs:
        binding_strs.append(b.var + ":\n" + textwrap.indent(str(b.binding), " " * 2))
    all_bindings = textwrap.indent("\n".join(binding_strs), " " * 2)
    bindings = f"Output Bindings:\n" + all_bindings

    # Nodes
    node_strs = [str(n) for n in fwf.nodes]
    all_nodes = textwrap.indent("\n".join(node_strs), " " * 2)
    nodes = f"Nodes:\n" + all_nodes

    return textwrap.dedent(header + io + behavior + bindings + nodes)


def render_error(error, indent: int = 0) -> str:
    out = "Error:\n"
    out += "\tCode: {}\n".format(error.code)
    out += "\tMessage:\n"
    for l in error.message.splitlines():
        out += f"\t\t{l:>8}"
    return textwrap.indent(out + "\n", spaces(indent))


def render_node_executions(wf_exec: FlyteWorkflowExecution, indent: int = 0) -> str:
    res = "Node Executions:\n"

    for node_id, ne in wf_exec.node_executions.items():
        if node_id == "start-node":
            continue

        node_res = textwrap.indent(f"Node: {ne.id.node_id}\n", spaces(6))
        node_res += textwrap.indent(f"{'Status:':10} {execution_models.NodeExecutionPhase.enum_to_string(ne.closure.phase)}\n", spaces(9))

        node_res += textwrap.indent("{:15} {:60} \n".format("Started:", str(ne.closure.started_at)), spaces(9))
        node_res += textwrap.indent("{:15} {:60} \n".format("Duration:", str(ne.closure.duration)), spaces(9))
        node_res += "\n"

        node_res += textwrap.indent(f"{'Input URI':15}: {ne.input_uri}\n", spaces(9))

        if ne.closure.output_uri:
            node_res += textwrap.indent(f"{'Output URI':15}: {ne.closure.output_uri}\n", spaces(9))

        if ne.closure.error is not None:
            node_res += textwrap.indent(f"{'Error':15}: {render_error(ne.closure.error)}\n", spaces(9))

        task_executions = ne.task_executions
        if len(task_executions) > 0:
            node_res += textwrap.indent("Task Executions:\n", spaces(9))
            for te in sorted(task_executions, key=lambda x: x.id.retry_attempt):
                node_res += textwrap.indent("Attempt {}:\n".format(te.id.retry_attempt), spaces(12))
                node_res += textwrap.indent("{:15} {:60}\n".format("Created:", str(te.closure.created_at)), spaces(15))
                node_res += textwrap.indent("{:15} {:60}\n".format("Started:", str(te.closure.started_at)), spaces(15))
                node_res += textwrap.indent("{:15} {:60}\n".format("Updated:", str(te.closure.updated_at)), spaces(15))
                node_res += textwrap.indent("{:15} {:60}\n".format("Duration:", str(te.closure.duration)), spaces(15))
                node_res += textwrap.indent("{:15} {:10}\n".format("Status:", execution_models.TaskExecutionPhase.enum_to_string(te.closure.phase)), spaces(15))
                if len(te.closure.logs) == 0:
                    node_res += spaces(15) + "{:15} {:60}\n".format("Logs:", "(None Found Yet)")
                else:
                    node_res += spaces(15) + "\t\t\t\t\tLogs:\n"
                    for log in sorted(te.closure.logs, key=lambda x: x.name):
                        node_res += spaces(18) + "{:8} {}\n".format("Name:", log.name)
                        node_res += spaces(18) + "{:8} {}\n".format("URI:", log.uri)

                if te.closure.error is not None:
                    render_error(te.closure.error, indent=15)

                # if te.is_parent:
                #     _click.echo(
                #         "\t\t\t\t\t{:15} {:60} ".format(
                #             "Subtasks:",
                #             "flyte-cli get-child-executions -h {host}{insecure} -u {urn}".format(
                #                 host=host,
                #                 urn=_tt(cli_identifiers.TaskExecutionIdentifier.promote_from_model(te.id)),
                #                 insecure=" --insecure" if insecure else "",
                #             ),
                #         )
                #     )
            node_res += "\n"
        node_res += "\n"
        res += node_res
    return textwrap.indent(res, spaces(indent))
