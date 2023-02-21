import os

import click
import yaml

from datetime import datetime

from flyteidl.admin.execution_pb2 import WorkflowExecutionGetMetricsRequest
from flyteidl.admin.common_pb2 import CategoricalSpanInfo
from flyteidl.admin.node_execution_pb2 import NodeExecutionGetMetricsRequest
from flyteidl.core.identifier_pb2 import NodeExecutionIdentifier, WorkflowExecutionIdentifier
from flytekit.clis.sdk_in_container.helpers import get_and_save_remote_with_click_context

_metrics_help = """
TODO @hamersaw
"""

@click.group("metrics")
@click.pass_context
def metrics(ctx: click.Context):
    pass

@click.command("dump", help="")
@click.option(
    "-d",
    "--depth",
    required=False,
    type=int,
    default=-1,
    help="TODO"
)
@click.argument("execution_id", type=str)
@click.argument("node_id", type=str, required=False)
@click.pass_context
def metrics_dump(
    ctx: click.Context,
    depth: int,
    execution_id: str,
    node_id: str,
):
    # retrieve remote
    remote = get_and_save_remote_with_click_context(ctx, None, None)

    sync_client = remote.client

    workflow_execution_id=WorkflowExecutionIdentifier(
        project="flytesnacks",
        domain="development",
        name=execution_id
    )

    if node_id != None:
        request = NodeExecutionGetMetricsRequest(
            id=NodeExecutionIdentifier(
                node_id=node_id,
                execution_id=workflow_execution_id,
            ),
            depth=depth,
        )
        response = sync_client.get_node_execution_metrics(request)
    else:
        request = WorkflowExecutionGetMetricsRequest(id=workflow_execution_id, depth=depth)
        response = sync_client.get_execution_metrics(request)

    id, info = aggregate_reference_span(response.span)
    yaml.emitter.Emitter.process_tag = lambda self, *args, **kw: None
    print(yaml.dump({id: info}, indent=2))

def aggregate_reference_span(span):
    id = ""
    id_type = span.reference.WhichOneof('id')
    if id_type == "workflow_id":
        id = span.reference.workflow_id.name
    elif id_type == "node_id":
        id = span.reference.node_id.node_id
    elif id_type == "task_id":
        id = span.reference.task_id.retry_attempt

    spans = aggregate_spans(span.reference.spans)
    return id, spans

def aggregate_spans(spans):
    breakdown = {}

    tasks = {}
    nodes = {}
    workflows = {}
 
    for span in spans:
        span_type = span.WhichOneof("info")
        if span_type == "category":
            category = CategoricalSpanInfo.Category.Name(span.category.category)

            start_time = datetime.fromtimestamp(span.start_time.seconds + span.start_time.nanos/1e9)
            end_time = datetime.fromtimestamp(span.end_time.seconds + span.end_time.nanos/1e9)
            total_time = (end_time - start_time).total_seconds()

            if category in breakdown:
                breakdown[category] += total_time
            else:
                breakdown[category] = total_time

        elif span_type == "reference":
            id, underlying_span = aggregate_reference_span(span)

            id_type = span.reference.WhichOneof('id')
            if id_type == "workflow_id":
                workflows[id] = underlying_span
            elif id_type == "node_id":
                nodes[id] = underlying_span
            elif id_type == "task_id":
                tasks[id] = underlying_span

            for category, total_time in underlying_span["breakdown"].items():
                if category in breakdown:
                    breakdown[category] += total_time
                else:
                    breakdown[category] = total_time

    span = {
        "breakdown": breakdown
    }

    if len(tasks) > 0:
        span["task_attempts"] = tasks
    if len(nodes) > 0:
        span["nodes"] = nodes
    if len(workflows) > 0:
        span["workflows"] = workflows

    return span

@click.command("explain", help="")
@click.option(
    "-d",
    "--depth",
    required=False,
    type=int,
    default=-1,
    help="TODO"
)
@click.argument("execution_id", type=str)
@click.argument("node_id", type=str, required=False)
@click.pass_context
def metrics_explain(
    ctx: click.Context,
    depth: int,
    execution_id: str,
    node_id: str,
):
    # retrieve remote
    remote = get_and_save_remote_with_click_context(ctx, None, None)

    #remote.set_signal(signal_id="say_hello_approval", execution_name="f7c193bad61a04710a5a", project="flytesnacks", domain="development", value=True)
    #return

    sync_client = remote.client

    workflow_execution_id=WorkflowExecutionIdentifier(
        project="flytesnacks",
        domain="development",
        name=execution_id
    )

    if node_id != None:
        request = NodeExecutionGetMetricsRequest(
            id=NodeExecutionIdentifier(
                node_id=node_id,
                execution_id=workflow_execution_id,
            ),
            depth=depth,
        )
        response = sync_client.get_node_execution_metrics(request)
    else:
        request = WorkflowExecutionGetMetricsRequest(id=workflow_execution_id, depth=depth)
        response = sync_client.get_execution_metrics(request)

    print('{:25s}{:25s}{:25s} {:>8s}    {:s}'.format('category', 'start_timestamp', 'end_timestamp', 'duration', 'entity'))
    print('-'*140)

    print_span(response.span, -1, "")

def print_span(span, indent, identifier):
    start_time = datetime.fromtimestamp(span.start_time.seconds + span.start_time.nanos/1e9)
    end_time = datetime.fromtimestamp(span.end_time.seconds + span.end_time.nanos/1e9)

    span_type = span.WhichOneof("info")
    if span_type == "category":
        category = CategoricalSpanInfo.Category.Name(span.category.category)
        indent_str = ""
        for i in range(indent):
            indent_str += "  "

        print("{:25s}{:25s}{:25s} {:7.2f}s    {:s}{:s}".format(
            category,
            start_time.strftime("%m-%d %H:%M:%S.%f"),
            end_time.strftime("%m-%d %H:%M:%S.%f"),
            (end_time - start_time).total_seconds(),
            indent_str,
            identifier,
        ))

    elif span_type == "reference":
        id_type = span.reference.WhichOneof('id')
        span_identifier = ""

        if id_type == "workflow_id":
            reference_identifier = "workflow/" + span.reference.workflow_id.name
        elif id_type == "node_id":
            reference_identifier = "node/" + span.reference.node_id.node_id
        elif id_type == "task_id":
            reference_identifier = "task/" + str(span.reference.task_id.retry_attempt)

        for under_span in span.reference.spans:
            print_span(under_span, indent+1, reference_identifier)

metrics.add_command(metrics_dump)
metrics.add_command(metrics_explain)
