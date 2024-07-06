from __future__ import annotations

from flyteidl.core import artifact_id_pb2 as art_id

# This is a sentinel input binding that is set by flytekit to indicate that a partition (or time-partition) value will
# be dynamic. It will be filled in automatically if the user doesn't bind all of an artifact's partition keys.
# It lives in this file to reduce circular dependencies.
DYNAMIC_INPUT_BINDING = art_id.LabelValue(runtime_binding=art_id.RuntimeBinding())
