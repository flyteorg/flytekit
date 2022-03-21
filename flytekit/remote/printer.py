"""
This module provides the ``FlyteRemote`` object, which is the end-user's main starting point for interacting
with a Flyte backend in an interactive and programmatic way. This of this experience as kind of like the web UI
but in Python object form.
"""
from __future__ import annotations

import textwrap
import typing
from typing import TYPE_CHECKING

from flytekit.models import schedule
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

ExecutionDataResponse = typing.Union[WorkflowExecutionGetDataResponse, NodeExecutionGetDataResponse]

MOST_RECENT_FIRST = admin_common_models.Sort("created_at", admin_common_models.Sort.Direction.DESCENDING)


def spaces(i: int) -> str:
    return " " * i


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
