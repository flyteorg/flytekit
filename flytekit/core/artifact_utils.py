from datetime import datetime
from typing import Dict, Optional

from flyteidl.core.artifact_id_pb2 import LabelValue, Partitions, TimePartition
from google.protobuf.timestamp_pb2 import Timestamp


def idl_partitions_from_dict(p: Optional[Dict[str, str]] = None) -> Optional[Partitions]:
    if p:
        return Partitions(value={k: LabelValue(static_value=v) for k, v in p.items()})

    return None


def idl_time_partition_from_datetime(tp: Optional[datetime] = None) -> Optional[TimePartition]:
    if tp:
        t = Timestamp()
        t.FromDatetime(tp)
        lv = LabelValue(time_value=t)
        return TimePartition(value=lv)

    return None
