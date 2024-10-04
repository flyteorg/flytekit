from __future__ import annotations

# for why we have the above
# https://github.com/protocolbuffers/protobuf/issues/9765#issuecomment-1119247779
from datetime import datetime
from typing import Dict, Optional

import flyteidl_rust as flyteidl

from flytekit.models import utils


def idl_partitions_from_dict(p: Optional[Dict[str, str]] = None) -> Optional[flyteidl.core.Partitions]:
    if p:
        return flyteidl.core.Partitions(
            value={k: flyteidl.core.LabelValue(value=flyteidl.label_value.Value.StaticValue(v)) for k, v in p.items()}
        )

    return None


def idl_time_partition_from_datetime(
    tp: Optional[datetime] = None, time_partition_granularity: Optional[flyteidl.core.Granularity] = None
) -> Optional[flyteidl.core.TimePartition]:
    if tp:
        t = utils.convert_from_datetime_to_timestamp(tp)
        lv = flyteidl.core.LabelValue(value=flyteidl.label_value.Value.TimeValue(t))
        return flyteidl.core.TimePartition(value=lv, granularity=time_partition_granularity)

    return None
