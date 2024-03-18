"""Experimental features of flytekit."""

# TODO(eapolinario): Remove this once a new flytekit release is out and
# references are updated in the monodocs build.
from flytekit.core.array_node_map_task import map_task  # noqa: F401
from flytekit.experimental.eager_function import EagerException, eager
