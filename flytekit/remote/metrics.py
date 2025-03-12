from datetime import datetime

import yaml
from flyteidl.core import metrics_pb2 as _metrics_pb2

from flytekit.models import common as _common_models


def aggregate_spans(spans):
    breakdown = {}

    tasks = {}
    nodes = {}
    workflows = {}

    for span in spans:
        id_type = span.WhichOneof("id")
        if id_type == "operation_id":
            operation_id = span.operation_id

            start_time = datetime.fromtimestamp(span.start_time.seconds + span.start_time.nanos / 1e9)
            end_time = datetime.fromtimestamp(span.end_time.seconds + span.end_time.nanos / 1e9)
            total_time = (end_time - start_time).total_seconds()

            if operation_id in breakdown:
                breakdown[operation_id] += total_time
            else:
                breakdown[operation_id] = total_time
        else:
            id, underlying_span = aggregate_reference_span(span)

            if id_type == "workflow_id":
                workflows[id] = underlying_span
            elif id_type == "node_id":
                nodes[id] = underlying_span
            elif id_type == "task_id":
                tasks[id] = underlying_span

            for operation_id, total_time in underlying_span["breakdown"].items():
                if operation_id in breakdown:
                    breakdown[operation_id] += total_time
                else:
                    breakdown[operation_id] = total_time

    span = {"breakdown": breakdown}

    if len(tasks) > 0:
        span["task_attempts"] = tasks
    if len(nodes) > 0:
        span["nodes"] = nodes
    if len(workflows) > 0:
        span["workflows"] = workflows

    return span


def aggregate_reference_span(span):
    id = ""
    id_type = span.WhichOneof("id")
    if id_type == "workflow_id":
        id = span.workflow_id.name
    elif id_type == "node_id":
        id = span.node_id.node_id
    elif id_type == "task_id":
        id = span.task_id.retry_attempt

    spans = aggregate_spans(span.spans)
    return id, spans


def print_span(span, indent, identifier):
    start_time = datetime.fromtimestamp(span.start_time.seconds + span.start_time.nanos / 1e9)
    end_time = datetime.fromtimestamp(span.end_time.seconds + span.end_time.nanos / 1e9)

    id_type = span.WhichOneof("id")
    span_identifier = ""

    if id_type == "operation_id":
        indent_str = ""
        for i in range(indent):
            indent_str += "  "

        print(
            "{:25s}{:25s}{:25s} {:7.2f}s    {:s}{:s}".format(
                span.operation_id,
                start_time.strftime("%m-%d %H:%M:%S.%f"),
                end_time.strftime("%m-%d %H:%M:%S.%f"),
                (end_time - start_time).total_seconds(),
                indent_str,
                identifier,
            )
        )

        span_identifier = identifier + "/" + span.operation_id
    else:
        if id_type == "workflow_id":
            span_identifier = "workflow/" + span.workflow_id.name
        elif id_type == "node_id":
            span_identifier = "node/" + span.node_id.node_id
        elif id_type == "task_id":
            span_identifier = "task/" + str(span.task_id.retry_attempt)

    for under_span in span.spans:
        print_span(under_span, indent + 1, span_identifier)


class FlyteExecutionSpan(_common_models.FlyteIdlEntity):
    def __init__(self, span: _metrics_pb2.Span):
        self._span = span

    def explain(self):
        print(
            "{:25s}{:25s}{:25s} {:>8s}    {:s}".format(
                "operation", "start_timestamp", "end_timestamp", "duration", "entity"
            )
        )
        print("-" * 140)

        print_span(self._span, -1, "")

    def dump(self):
        id, info = aggregate_reference_span(self._span)
        yaml.emitter.Emitter.process_tag = lambda self, *args, **kw: None
        print(yaml.dump({id: info}, indent=2))

    def to_flyte_idl(self):
        return self._span

    @classmethod
    def from_flyte_idl(cls, pb):
        return cls(span=pb)
