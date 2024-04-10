from __future__ import annotations

# for why we have the above
# https://github.com/protocolbuffers/protobuf/issues/9765#issuecomment-1119247779
from datetime import datetime
from typing import Dict, Optional

from flyteidl.core.artifact_id_pb2 import Granularity, LabelValue, Partitions, TimePartition
from google.protobuf.timestamp_pb2 import Timestamp


def idl_partitions_from_dict(p: Optional[Dict[str, str]] = None) -> Optional[Partitions]:
    if p:
        return Partitions(value={k: LabelValue(static_value=v) for k, v in p.items()})

    return None


def idl_time_partition_from_datetime(
    tp: Optional[datetime] = None, time_partition_granularity: Optional[Granularity] = None
) -> Optional[TimePartition]:
    if tp:
        t = Timestamp()
        t.FromDatetime(tp)
        lv = LabelValue(time_value=t)
        return TimePartition(value=lv, granularity=time_partition_granularity)

    return None
