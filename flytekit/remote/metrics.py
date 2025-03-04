from flyteidl.core import metrics_pb2 as _metrics_pb2

from flytekit.models import common as _common_models
from flytekit.utils import metrics as metrics_utils


class FlyteExecutionSpan(_common_models.FlyteIdlEntity):
    def __init__(self, span: _metrics_pb2.Span):
        self._span = span

    def explain(self):
        metrics_utils.explain_execution_span(self._span)

    def dump(self):
        metrics_utils.dump_execution_span(self._span)

    def to_flyte_idl(self):
        return self._span

    @classmethod
    def from_flyte_idl(cls, pb):
        return cls(span=pb)
