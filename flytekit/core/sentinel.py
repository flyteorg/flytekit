from __future__ import annotations

import flyteidl_rust as flyteidl

# This is a sentinel input binding that is set by flytekit to indicate that a partition (or time-partition) value will
# be dynamic. It will be filled in automatically if the user doesn't bind all of an artifact's partition keys.
# It lives in this file to reduce circular dependencies.
DYNAMIC_INPUT_BINDING = flyteidl.core.LabelValue(runtime_binding=flyteidl.core.RuntimeBinding())
