from datetime import datetime
from typing import Dict, List, Optional, Tuple

from flyteidl.core import artifact_id_pb2 as art_id
from google.protobuf.timestamp_pb2 import Timestamp

from flytekit.models.interface import Variable


def filter_outputs_for_dynamic_partitions(output_vars: Dict[str, Variable]) -> List[Tuple[str, Variable]]:
    """
    Helper function to filter outputs where an artifact's partition (including time) values are not statically bound
    (statically bound means either bound to an input, or a constant value). We need a list of these Variables in the
    correct order to be able to match up entries in the Output Metadata Tracker object, which basically is a list of
    the user's dynamically created partition values, in the order in which python evaluated an Artifact's annotate
    function. That is, users can do something like

        Pricing = Artifacts(name="pricing", partition_keys=["region"])
        EstError = Artifacts(name="estimation_error", partition_keys=["dataset"], time_partitioned=True)

        @task
        def t1() -> Annotated[pd.DataFrame, Pricing], Annotated[float, EstError]:
            df = get_pricing_results()
            dt = get_time()
            return Pricing.create_from(df, region="dubai"), \
            EstError.create_from(msq_error, dataset="train", time_partition=dt)

    We rely on Python's evaluation order to match up the correct partition values with the correct artifact.
    https://docs.python.org/3/reference/expressions.html#evaluation-order
    """

    with_dynamic = []
    for k, v in output_vars.items():
        if v.artifact_partial_id is not None:
            if v.artifact_partial_id.HasField("time_partition"):
                if v.artifact_partial_id.time_partition.value.HasField("runtime_binding"):
                    with_dynamic.append((k, v))
                    continue

            if v.artifact_partial_id.HasField("partitions"):
                for p in v.artifact_partial_id.partitions.value.values():
                    if p.HasField("runtime_binding"):
                        with_dynamic.append((k, v))
                        break

    return with_dynamic


def idl_partitions_from_dict(p: Optional[Dict[str, str]] = None) -> Optional[art_id]:
    if p:
        return art_id.Partitions(value={k: art_id.LabelValue(static_value=v) for k, v in p.items()})

    return None


def idl_time_partition_from_datetime(tp: Optional[datetime] = None) -> Optional[art_id.TimePartition]:
    if tp:
        t = Timestamp()
        t.FromDatetime(tp)
        lv = art_id.LabelValue(time_value=t)
        return art_id.TimePartition(value=lv)

    return None
