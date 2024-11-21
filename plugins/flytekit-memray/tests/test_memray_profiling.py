from unittest.mock import Mock, patch
import pytest
from flytekit import task, current_context
from flytekitplugins.memray import memray_profiling


@task(enable_deck=True)
@memray_profiling
def heavy_compute(i: int) -> int:
    return i + 1


def test_local_exec():
    heavy_compute(i=7)
    assert (
        len(current_context().decks) == 6
    )  # memray flamegraph, timeline, input, and output, source code, dependencies


def test_errors():
    reporter = "summary"
    with pytest.raises(
        ValueError, match=f"{reporter} is not a supported html reporter."
    ):
        memray_profiling(memray_html_reporter=reporter)

    reporter = "flamegraph"
    with pytest.raises(
        ValueError,
        match=f"unrecognized arguments for {reporter} reporter. Please check https://bloomberg.github.io/memray/{reporter}.html",
    ):
        memray_profiling(memray_reporter_args=["--leaks", "trash"])

    reporter = "flamegraph"
    with pytest.raises(
        ValueError,
        match=f"unrecognized arguments for {reporter} reporter. Please check https://bloomberg.github.io/memray/{reporter}.html",
    ):
        memray_profiling(memray_reporter_args=[0, 1, 2])
