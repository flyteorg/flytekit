"""Pytest plugin that auto-registers flytekit testing fixtures.

When ``flytekit`` is installed, these fixtures are automatically available in any
pytest session without needing to import them explicitly. This works via the
``pytest11`` entry point registered in ``pyproject.toml``.
"""

from flytekit.testing.fixtures import flyte_cache, flyte_context, flyte_tmp_dir

__all__ = ["flyte_cache", "flyte_context", "flyte_tmp_dir"]
